use crate::actor::EventSourcedActor;
use crate::behaviour::{Actor, Flow, Root};
use crate::error::TellError;
use crate::journal::Journal;
use crate::path::{ActorPath, is_valid_name};
use crate::reply::ReplyTo;
use crate::system::{ActorOfError, SystemInner};
use parking_lot::Mutex;
use std::marker::PhantomData;
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
        self.tell(make(reply)).await?;
        // A remote actor that stops mid-request, a host that goes away, or an
        // answer that will not decode all end here: the sender is dropped and
        // the caller is told, rather than waiting on something that is not
        // coming.
        rx.await.map_err(|_| TellError::MailboxClosed)
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

impl<C> Link<C> {
    /// Whether two links reach the same place, so that dropping a stale one
    /// cannot discard a fresh one a concurrent send just resolved.
    fn is_same(&self, other: &Self) -> bool {
        match (self, other) {
            (Link::Local(a), Link::Local(b)) => a.same_channel(b),
            (Link::Remote(a), Link::Remote(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

/// Handle to the runtime from inside an actor: its own path, its parent, its
/// children, and the journal for actors that manage persistence themselves.
///
/// Parameterized by the **command** types rather than the actor types, so an
/// actor and an adapter wrapping it (see [`Persistent`]) hand out the same
/// context. Parameterized by actor type they would be two incompatible types for
/// one mailbox.
///
/// `PC` is the parent's command type, which is what makes [`parent`](Self::parent)
/// a typed reference rather than a lookup by string. It defaults to [`Root`], so
/// an actor at the top of the tree writes `ActorContext<MyCommand>` and never
/// mentions it.
///
/// [`Persistent`]: crate::Persistent
/// [`Root`]: crate::Root
pub struct ActorContext<C, PC = Root> {
    pub(crate) inner: Arc<SystemInner>,
    pub(crate) self_tx: mpsc::Sender<C>,
    pub(crate) path: ActorPath,
    /// `fn() -> PC` rather than `PC`, so the context is `Send`/`Sync` on its own
    /// merits and does not inherit the parent command type's.
    pub(crate) parent: PhantomData<fn() -> PC>,
}

impl<C: Send + 'static, PC: Send + 'static> ActorContext<C, PC> {
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

    /// A reference to this actor's parent.
    ///
    /// An ordinary reference to an ordinary path — nothing was handed down at
    /// construction, which is what an actor built on a host that never saw its
    /// parent needs. An actor at the top of the tree gets a reference it can
    /// hold and never send through, because [`Root`] has no values: the type
    /// system says root takes no messages.
    ///
    /// [`Root`]: crate::Root
    #[must_use]
    pub fn parent(&self) -> ActorRef<PC> {
        ActorRef::at(
            self.path.parent().unwrap_or_else(ActorPath::root),
            None,
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
    /// The bound is what makes the tree honest — a child may only be created
    /// under a parent whose commands it declared as its
    /// [`ParentCommand`](Actor::ParentCommand).
    pub fn actor_of<B>(&self, name: &str, actor: B) -> Result<ActorRef<B::Command>, ActorOfError>
    where
        B: Actor<ParentCommand = C>,
    {
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

/// Start an actor at `path` and return the link to its mailbox.
///
/// Registration is deliberately not here: the registry is the one thing that
/// decides what lives at a path, and a spawn that also registered would give it
/// a second owner.
pub(crate) fn spawn_at<A: Actor>(
    actor: A,
    inner: Arc<SystemInner>,
    path: ActorPath,
) -> Link<A::Command> {
    let (tx, rx) = tokio::sync::mpsc::channel(MAILBOX_CAPACITY);
    let ctx = ActorContext {
        inner,
        self_tx: tx.clone(),
        path,
        parent: PhantomData,
    };
    tokio::spawn(run_actor(actor, rx, ctx));
    Link::Local(tx)
}

/// Reject a name that could not be one path segment.
pub(crate) fn check_name(name: &str) -> Result<(), ActorOfError> {
    if is_valid_name(name) {
        Ok(())
    } else {
        Err(ActorOfError::InvalidName(name.to_owned()))
    }
}

/// The lifecycle of a single actor: start, then process commands until the
/// mailbox closes or the actor asks to stop.
///
/// Everything about persistence used to live here. It now lives in
/// [`Persistent`], so this loop is the same for every kind of actor.
///
/// [`Persistent`]: crate::Persistent
pub(crate) async fn run_actor<A: Actor>(
    mut actor: A,
    mut rx: mpsc::Receiver<A::Command>,
    mut ctx: ActorContext<A::Command, A::ParentCommand>,
) {
    let mut stand_down = ctx.inner.stand_down.clone();
    stand_down.mark_unchanged();

    if let Err(e) = actor.on_start(&mut ctx).await {
        tracing::error!(path = %ctx.path, error = %e, "actor failed to start; shutting down");
        return;
    }

    loop {
        let cmd = tokio::select! {
            cmd = rx.recv() => match cmd {
                Some(cmd) => cmd,
                None => break,
            },
            () = stood_down(&mut stand_down) => break,
        };

        // The handler is raced against the same signal, so an instance loses
        // its in-flight work rather than finishing it. That is the point: a
        // node without quorum cannot know whether this instance now belongs to
        // somebody else, and a half-finished turn is a smaller loss than one
        // completed against a history that has moved on.
        let flow = tokio::select! {
            flow = actor.handle(cmd, &mut ctx) => flow,
            () = stood_down(&mut stand_down) => break,
        };
        match flow {
            Flow::Continue => {}
            Flow::Stop => break,
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
