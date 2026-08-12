use crate::actor::EventSourcedActor;
use crate::behaviour::{Actor, Flow};
use crate::error::TellError;
use crate::journal::Journal;
use crate::path::{ActorPath, is_valid_name};
use crate::reply::ReplyTo;
use crate::system::{ActorOfError, SystemInner};
use parking_lot::Mutex;

use std::sync::{Arc, Weak};
use tokio::sync::mpsc;

/// Mailbox capacity for every spawned actor.
pub(crate) const MAILBOX_CAPACITY: usize = 64;

/// Where a path currently resolves to.
///
/// A *cache*, not an identity. The identity is the path; this is how to reach
/// whatever is at it right now, and it is replaced rather than repaired when the
/// instance behind it goes away.
pub(crate) enum Link<C> {
    /// The actor's mailbox, in this process.
    Local(mpsc::Sender<C>),
    /// A closure that encodes the command and ships it to whichever node hosts
    /// the actor. Built where `C: Serialize` is known, which is what keeps that
    /// bound off `ActorRef` itself — and off every caller that merely holds one.
    Remote(RemoteSend<C>),
}

pub(crate) type RemoteSend<C> =
    Arc<dyn Fn(C) -> futures_util::future::BoxFuture<'static, Result<(), TellError>> + Send + Sync>;

impl<C> Clone for Link<C> {
    fn clone(&self) -> Self {
        match self {
            Link::Local(tx) => Link::Local(tx.clone()),
            Link::Remote(f) => Link::Remote(f.clone()),
        }
    }
}

impl<C> Link<C> {
    /// Whether this link still reaches something.
    ///
    /// A remote link has nothing to inspect and reports `true`; whether the host
    /// is alive is a question for the send.
    pub(crate) fn is_alive(&self) -> bool {
        match self {
            Link::Local(tx) => !tx.is_closed(),
            Link::Remote(_) => true,
        }
    }

    /// Send, handing the command back when it can still be tried elsewhere.
    ///
    /// A local send that fails returns the command, which is what makes a
    /// re-resolve-and-retry possible at all. A remote one has already consumed
    /// it — encoding takes ownership — so it returns `None` and the failure is
    /// final.
    pub(crate) async fn send(&self, cmd: C) -> Result<(), (TellError, Option<C>)>
    where
        C: Send + 'static,
    {
        match self {
            Link::Local(tx) => tx
                .send(cmd)
                .await
                .map_err(|e| (TellError::MailboxClosed, Some(e.0))),
            Link::Remote(send) => send(cmd).await.map_err(|e| (e, None)),
        }
    }
}

/// A cheap, cloneable handle for sending commands to an actor.
///
/// **A name, not a handle.** It is a path plus a cached link: the path is what
/// the reference *is*, and the link is where that path happened to resolve last
/// time. A send that fails drops the cache, re-resolves once and retries — so a
/// reference held across a restart, an idle offload, or (later) a move to
/// another node keeps working, with the holder doing nothing and knowing
/// nothing.
///
/// Says nothing about *where* the actor is either. The same type, `tell` and
/// `ask` work whether it is in this process or on another node, which is what
/// lets business logic be written once and hosted either way.
pub struct ActorRef<C> {
    path: ActorPath,
    /// Shared across clones, so one re-resolution serves all of them. `None`
    /// means "not resolved yet, or the last link failed" — both of which are
    /// answered the same way, by resolving.
    link: Arc<Mutex<Option<Link<C>>>>,
    /// The registry to re-resolve against. Weak because the registry holds
    /// references too; a strong one here would make every actor keep its own
    /// system alive forever.
    system: Weak<SystemInner>,
}

// Manual `Clone` — the derive would demand `C: Clone`, and a handle is
// cloneable whatever it carries.
impl<C> Clone for ActorRef<C> {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            link: self.link.clone(),
            system: self.system.clone(),
        }
    }
}

// Manual too, and for the same reason. The path is the useful part: the mailbox
// contents belong to the actor, and the link is an implementation detail that
// changes under the holder.
impl<C> std::fmt::Debug for ActorRef<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorRef")
            .field("path", &self.path)
            .finish()
    }
}

impl<C: Send + 'static> ActorRef<C> {
    /// A reference to `path`, with `link` as its starting cache.
    pub(crate) fn at(path: ActorPath, link: Option<Link<C>>, system: Weak<SystemInner>) -> Self {
        Self {
            path,
            link: Arc::new(Mutex::new(link)),
            system,
        }
    }

    /// The name this reference addresses.
    #[must_use]
    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    /// Deliver `cmd` to the actor's mailbox, waiting if the mailbox is full.
    ///
    /// Fails if the path reaches nothing: no actor was ever created there, or the
    /// one that was has stopped and nobody has recreated it. A path whose actor
    /// *has* been recreated is not a failure — the link is refreshed and the
    /// command delivered.
    pub async fn tell(&self, cmd: C) -> Result<(), TellError> {
        let Some(link) = self.cached() else {
            let link = self.resolve().ok_or(TellError::MailboxClosed)?;
            return link.send(cmd).await.map_err(|(e, _)| e);
        };

        let cmd = match link.send(cmd).await {
            Ok(()) => return Ok(()),
            Err((e, None)) => return Err(e),
            Err((_, Some(cmd))) => cmd,
        };

        // Exactly once. Re-resolving in a loop would turn a genuinely dead
        // actor into a spin against a registry that is not going to change; the
        // second failure is the honest answer.
        self.forget(&link);
        let link = self.resolve().ok_or(TellError::MailboxClosed)?;
        link.send(cmd).await.map_err(|(e, _)| e)
    }

    /// Send a request and await the actor's reply — the request/response pattern.
    ///
    /// `make` builds the command from a fresh [`ReplyTo`]; the actor answers
    /// through it. Paired with [`CommandEffect::and_ack`], the reply lands only
    /// after the durable write, so the caller gets genuine backpressure rather
    /// than an acknowledgement of an intention.
    ///
    /// Works the same whether the actor is here or on another node: encoding
    /// the command registers the reply handle against this node, and the answer
    /// is routed back to it. Note there is no bound on `R` here — the
    /// requirement that a reply must round-trip lives on `ReplyTo`'s own
    /// `Serialize`, so it applies exactly to handles that actually cross a
    /// host and not to every local-only reply type in every consumer.
    ///
    /// [`CommandEffect::and_ack`]: crate::CommandEffect::and_ack
    pub async fn ask<F, R>(&self, make: F) -> Result<R, TellError>
    where
        F: FnOnce(ReplyTo<R>) -> C,
        R: Send + 'static,
    {
        let (reply, rx) = ReplyTo::channel();
        // Dropped when this future is, however it ends. Cancelling an `ask`
        // whose handle has already crossed a host would otherwise leave a row in
        // this node's waiting table holding a channel nobody will ever read.
        let _waiting = Cancelled(reply.deregister());
        self.tell(make(reply)).await?;
        // A remote actor that stops mid-request, a host that goes away, or an
        // answer that will not decode all end here: the sender is dropped and
        // the caller is told, rather than waiting on something that is not
        // coming.
        rx.await.map_err(|_| TellError::MailboxClosed)
    }

    /// [`ask`](Self::ask), giving up after `within`.
    ///
    /// Opt-in rather than the default, and deliberately so: asks range from
    /// reading a field to waiting on a model, and one number cannot serve both.
    /// A default high enough for the slow ones is no backstop for the fast ones.
    ///
    /// Reach for it when the answer stops being useful after a while — an
    /// interactive read, a health probe — and not otherwise. The cases where
    /// nobody *can* answer already fail on their own: a mailbox that closes, a
    /// handle dropped without an answer, a node that stands down. What is left
    /// is a host that vanished mid-request, which only a deadline ends.
    ///
    /// # Errors
    /// [`TellError::NoAnswer`] if the deadline passes first, plus anything
    /// [`ask`](Self::ask) can fail with.
    pub async fn ask_within<F, R>(
        &self,
        within: std::time::Duration,
        make: F,
    ) -> Result<R, TellError>
    where
        F: FnOnce(ReplyTo<R>) -> C,
        R: Send + 'static,
    {
        // Dropping the inner future is what deregisters, so this needs nothing
        // of its own.
        tokio::time::timeout(within, self.ask(make))
            .await
            .unwrap_or(Err(TellError::NoAnswer))
    }

    /// Stop the actor at this path, and everything under it.
    ///
    /// Returns once the subtree is quiet, deepest first, so a caller that
    /// stopped a supervisor knows its sessions are gone rather than going. `false`
    /// if nothing was there — stopping a path twice is not an error.
    ///
    /// The path outlives this, like every other way an actor ends: create one at
    /// the same path again and every reference held across the gap reaches the
    /// new instance.
    ///
    /// An actor stops *itself* by returning [`Flow::Stop`]. Calling this on its
    /// own reference from inside a handler would wait for a task that is waiting
    /// for the handler to return.
    ///
    /// [`Flow::Stop`]: crate::Flow::Stop
    pub async fn stop(&self) -> bool {
        let Some(system) = self.system.upgrade() else {
            return false;
        };
        system.stop_at(&self.path).await
    }

    pub(crate) fn cached(&self) -> Option<Link<C>> {
        self.link.lock().clone()
    }

    /// Drop `stale` from the cache, unless somebody has already replaced it.
    fn forget(&self, stale: &Link<C>) {
        let mut slot = self.link.lock();
        if slot.as_ref().is_some_and(|link| link.is_same(stale)) {
            *slot = None;
        }
    }

    /// Look the path up in the registry and cache what comes back.
    ///
    /// `None` when the path reaches nothing — including when it holds a stopped
    /// actor. Resolution never *creates*: a ref that woke an actor up would break
    /// the rule that reading a session must not load it.
    fn resolve(&self) -> Option<Link<C>> {
        let system = self.system.upgrade()?;
        let found = system.resolve::<C>(&self.path)?;
        *self.link.lock() = Some(found.clone());
        Some(found)
    }
}

/// Deregisters a waiting caller when the `ask` awaiting it goes away.
///
/// Held inside the future, so it fires on a timeout, a `select!` that lost, a
/// dropped request — every way a caller can stop caring. It also fires on
/// success, where forgetting a correlation the answer already removed costs a
/// lookup and keeps the rule to one line.
struct Cancelled(crate::reply::Deregister);

impl Drop for Cancelled {
    fn drop(&mut self) {
        if let Some(deregister) = self.0.lock().take() {
            deregister();
        }
    }
}

impl<C> Link<C> {
    /// Whether two links reach the same place, so that dropping a stale one
    /// cannot discard a fresh one a concurrent send just resolved.
    pub(crate) fn is_same(&self, other: &Self) -> bool {
        match (self, other) {
            (Link::Local(a), Link::Local(b)) => a.same_channel(b),
            (Link::Remote(a), Link::Remote(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

/// Handle to the runtime from inside an actor: its own path, its children, and
/// the journal for actors that manage persistence themselves.
///
/// Parameterized by the **command** type rather than the actor type, so an actor
/// and an adapter wrapping it (see [`Persistent`]) hand out the same context.
/// Parameterized by actor type they would be two incompatible types for one
/// mailbox.
///
/// There is deliberately no `parent()`. A reference to a parent is *given* to a
/// child — by the parent at `actor_of`, or by a shard recipe that closed over
/// one — so it is typed at the point it is made and never asserted. A typed
/// `parent()` would have to carry the parent's command type through every
/// signature that touches a context, to serve the few actors that reach upwards,
/// and the check it bought would not survive the parent moving to another host:
/// a remote command is typed by its registered decoder at delivery, not by the
/// caller. Akka Typed removed the same method for the same reason.
///
/// [`Persistent`]: crate::Persistent
pub struct ActorContext<C> {
    pub(crate) inner: Arc<SystemInner>,
    pub(crate) self_tx: mpsc::Sender<C>,
    pub(crate) path: ActorPath,
}

impl<C: Send + 'static> ActorContext<C> {
    /// Where this actor is, which is what it is.
    #[must_use]
    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    /// A reference to this actor's own mailbox.
    #[must_use]
    pub fn self_ref(&self) -> ActorRef<C> {
        ActorRef::at(
            self.path.clone(),
            Some(Link::Local(self.self_tx.clone())),
            Arc::downgrade(&self.inner),
        )
    }

    /// The child named `name`, creating it from `actor` if it is not there.
    ///
    /// Get-or-create: two callers naming one path get one actor, and the loser's
    /// `actor` is dropped without ever being started. That matters most for
    /// event-sourced children, where two instances at one name means two actors
    /// writing one journal.
    ///
    /// A child that needs to reach back is *given* the reference — `ctx.self_ref()`
    /// at this call, or a shard reference closed over by a recipe. There is no
    /// `parent()` to read one from, on purpose: see the type-level note on
    /// [`ActorSystem`](crate::ActorSystem).
    pub fn actor_of<B: Actor>(
        &self,
        name: &str,
        actor: B,
    ) -> Result<ActorRef<B::Command>, ActorOfError> {
        self.system().get_or_create(&self.path, name, actor)
    }

    /// Adapt an event-sourced actor into an ordinary one, over this actor's
    /// journal — the same adapter [`ActorSystem::persistent`] applies, reached
    /// from inside an actor.
    ///
    /// ```ignore
    /// ctx.actor_of("agent-main", ctx.persistent(AgentActor::new(..)))?;
    /// ```
    ///
    /// [`ActorSystem::persistent`]: crate::ActorSystem::persistent
    #[must_use]
    pub fn persistent<B: EventSourcedActor>(&self, actor: B) -> crate::Persistent<B> {
        crate::Persistent::new(actor, self.journal().clone())
    }

    /// A reference to the actors of a shard type, from inside an actor.
    ///
    /// The same reference [`ActorSystem::shard_actor_of`] hands out, reached
    /// from where most sends happen: an actor that needs a peer knows what to
    /// say to it, not where it is.
    ///
    /// [`ActorSystem::shard_actor_of`]: crate::ActorSystem::shard_actor_of
    #[must_use]
    pub fn shard_actor_of<S: crate::Shard>(&self) -> ActorRef<S::Command> {
        self.system().shard_actor_of::<S>()
    }

    /// Direct journal access for actors that manage persistence themselves
    /// (e.g. copying a snapshot to seed a forked instance).
    #[must_use]
    pub fn journal(&self) -> &Arc<dyn Journal> {
        &self.inner.journal
    }

    fn system(&self) -> crate::ActorSystem {
        crate::ActorSystem::from_inner(self.inner.clone())
    }
}

/// A running actor, as the registry holds it: how to reach it, how to ask it to
/// stop, and how to tell when it has.
pub(crate) struct Spawned<C> {
    pub(crate) link: Link<C>,
    /// Raised to stop this one actor. Held by the registry entry, so an actor
    /// started outside the registry simply has nobody who can raise it.
    pub(crate) stop: tokio::sync::watch::Sender<bool>,
    /// Closed by the actor's own task when it has ended — after its children
    /// have, which is what makes "stop everything under here" a thing that can
    /// be waited on.
    pub(crate) terminated: tokio::sync::watch::Receiver<()>,
}

/// Start an actor at `path`.
///
/// Registration is deliberately not here: the registry is the one thing that
/// decides what lives at a path, and a spawn that also registered would give it
/// a second owner.
pub(crate) fn spawn_at<A: Actor>(
    actor: A,
    inner: Arc<SystemInner>,
    path: ActorPath,
) -> Spawned<A::Command> {
    let (tx, rx) = tokio::sync::mpsc::channel(MAILBOX_CAPACITY);
    let (stop, stop_rx) = tokio::sync::watch::channel(false);
    let (ended, terminated) = tokio::sync::watch::channel(());
    let ctx = ActorContext {
        inner,
        self_tx: tx.clone(),
        path,
    };
    tokio::spawn(run_actor(actor, rx, ctx, stop_rx, ended));
    Spawned {
        link: Link::Local(tx),
        stop,
        terminated,
    }
}

/// Reject a name that could not be one path segment.
pub(crate) fn check_name(name: &str) -> Result<(), ActorOfError> {
    if is_valid_name(name) {
        Ok(())
    } else {
        Err(ActorOfError::InvalidName(name.to_owned()))
    }
}

/// The lifecycle of a single actor: start, process commands, then take its
/// children with it.
///
/// Everything about persistence used to live here. It now lives in
/// [`Persistent`], so this loop is the same for every kind of actor.
///
/// [`Persistent`]: crate::Persistent
pub(crate) async fn run_actor<A: Actor>(
    mut actor: A,
    mut rx: mpsc::Receiver<A::Command>,
    mut ctx: ActorContext<A::Command>,
    mut stop: tokio::sync::watch::Receiver<bool>,
    ended: tokio::sync::watch::Sender<()>,
) {
    let mut stand_down = ctx.inner.stand_down.clone();
    stand_down.mark_unchanged();

    if let Err(e) = actor.on_start(&mut ctx).await {
        tracing::error!(path = %ctx.path, error = %e, "actor failed to start; shutting down");
    } else {
        serve(&mut actor, &mut rx, &mut ctx, &mut stand_down, &mut stop).await;
    }

    // The guardian half, and the whole of it: an actor takes its children with
    // it. Its own entry goes first, so nothing new resolves to an actor that is
    // on its way out, and the actor value is dropped last of all — after the
    // subtree below it is quiet, which is what lets a parent's final act assume
    // its children are gone.
    ctx.inner
        .retire(&ctx.path, &Link::Local(ctx.self_tx.clone()));
    ctx.inner.stop_descendants(&ctx.path).await;
    drop(actor);
    drop(ended);
}

/// Handle commands until something says to stop.
async fn serve<A: Actor>(
    actor: &mut A,
    rx: &mut mpsc::Receiver<A::Command>,
    ctx: &mut ActorContext<A::Command>,
    stand_down: &mut tokio::sync::watch::Receiver<bool>,
    stop: &mut tokio::sync::watch::Receiver<bool>,
) {
    loop {
        let cmd = tokio::select! {
            cmd = rx.recv() => match cmd {
                Some(cmd) => cmd,
                None => return,
            },
            () = stood_down(stand_down) => return,
            () = asked_to_stop(stop) => return,
        };

        // The handler is raced against the same signals, so an instance loses
        // its in-flight work rather than finishing it. That is the point: a
        // node without quorum cannot know whether this instance now belongs to
        // somebody else, and a half-finished turn is a smaller loss than one
        // completed against a history that has moved on. A stop is treated the
        // same way rather than given a rule of its own.
        let flow = tokio::select! {
            flow = actor.handle(cmd, ctx) => flow,
            () = stood_down(stand_down) => return,
            () = asked_to_stop(stop) => return,
        };
        match flow {
            Flow::Continue => {}
            Flow::Stop => return,
        }
    }
}

/// Resolve once this node has stopped serving, and never otherwise.
///
/// A system with no cluster holds a signal that is never sent, so this pends
/// forever and the `select!` around it costs nothing.
async fn stood_down(watch: &mut tokio::sync::watch::Receiver<bool>) {
    loop {
        if watch.changed().await.is_err() {
            // The sender is gone, which means the system is being torn down.
            // Standing down is the right reading of that.
            return;
        }
        if *watch.borrow_and_update() {
            return;
        }
    }
}

/// Resolve once this actor in particular has been asked to stop.
///
/// The sender lives in the registry entry, so it going away means only that
/// nobody is in a position to ask — an actor started outside the registry, which
/// runs until it stops itself. That is the opposite reading to the one above,
/// and deliberately: one signal is the system ending, the other is a request
/// that can no longer be made.
async fn asked_to_stop(watch: &mut tokio::sync::watch::Receiver<bool>) {
    loop {
        if watch.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
        if *watch.borrow_and_update() {
            return;
        }
    }
}
