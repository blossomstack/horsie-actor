use crate::behaviour::{Actor, Flow};
use crate::error::TellError;
use crate::journal::Journal;
use crate::reply::ReplyTo;
use crate::system::SystemInner;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Mailbox capacity for every spawned actor.
pub(crate) const MAILBOX_CAPACITY: usize = 64;

/// How a reference actually reaches its actor.
enum Reach<C> {
    /// The actor's mailbox, in this process.
    Local(mpsc::Sender<C>),
    /// A closure that encodes the command and ships it to whichever node hosts
    /// the actor. Built where `C: Serialize` is known, which is what keeps that
    /// bound off `ActorRef` itself — and off every caller that merely holds one.
    Remote(RemoteSend<C>),
}

type RemoteSend<C> =
    Arc<dyn Fn(C) -> futures_util::future::BoxFuture<'static, Result<(), TellError>> + Send + Sync>;

impl<C> Clone for Reach<C> {
    fn clone(&self) -> Self {
        match self {
            Reach::Local(tx) => Reach::Local(tx.clone()),
            Reach::Remote(f) => Reach::Remote(f.clone()),
        }
    }
}

/// A cheap, cloneable handle for sending commands to an actor.
///
/// Says nothing about where the actor is. The same type, `tell` and `ask` work
/// whether it is in this process or on another node, which is what lets business
/// logic be written once and hosted either way.
pub struct ActorRef<C> {
    reach: Reach<C>,
}

// Manual `Clone` — a `Sender<C>` clones regardless of whether `C: Clone`.
impl<C> Clone for ActorRef<C> {
    fn clone(&self) -> Self {
        Self {
            reach: self.reach.clone(),
        }
    }
}

// Manual too, and for the same reason: a handle is printable whether or not its
// command type is. There is nothing useful to show beyond liveness — the
// mailbox contents belong to the actor.
impl<C> std::fmt::Debug for ActorRef<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.reach {
            Reach::Local(tx) => f
                .debug_struct("ActorRef")
                .field("reach", &"local")
                .field("alive", &!tx.is_closed())
                .finish(),
            Reach::Remote(_) => f
                .debug_struct("ActorRef")
                .field("reach", &"remote")
                .finish(),
        }
    }
}

impl<C: Send + 'static> ActorRef<C> {
    pub(crate) fn new(tx: mpsc::Sender<C>) -> Self {
        Self {
            reach: Reach::Local(tx),
        }
    }

    pub(crate) fn remote(send: RemoteSend<C>) -> Self {
        Self {
            reach: Reach::Remote(send),
        }
    }

    /// Whether the actor is still running.
    ///
    /// A local actor stops for reasons its callers never see — it asked to, or
    /// its log moved past it and it stood down rather than serve stale. The
    /// registry uses this to notice a dead instance and start a fresh one
    /// instead of handing out a reference nothing is listening to. A remote
    /// reference has nothing to inspect and reports `true`; whether the host is
    /// alive is a question for the send.
    #[must_use]
    pub fn is_alive(&self) -> bool {
        match &self.reach {
            Reach::Local(tx) => !tx.is_closed(),
            Reach::Remote(_) => true,
        }
    }

    /// Deliver `cmd` to the actor's mailbox, waiting if the mailbox is full.
    /// Fails only if the actor has stopped.
    pub async fn tell(&self, cmd: C) -> Result<(), TellError> {
        match &self.reach {
            Reach::Local(tx) => tx.send(cmd).await.map_err(|_| TellError::MailboxClosed),
            Reach::Remote(send) => send(cmd).await,
        }
    }

    /// Send a request and await the actor's reply — the request/response pattern.
    ///
    /// `make` builds the command from a fresh [`ReplyTo`]; the actor answers
    /// through it. Paired with [`CommandEffect::and_ack`], the reply lands only
    /// after the durable write, so the caller gets genuine backpressure rather
    /// than an acknowledgement of an intention.
    ///
    /// [`CommandEffect::and_ack`]: crate::CommandEffect::and_ack
    pub async fn ask<F, R>(&self, make: F) -> Result<R, TellError>
    where
        F: FnOnce(ReplyTo<R>) -> C,
        R: Send + 'static,
    {
        // Refuse rather than hang. The reply handle is a channel into this
        // process; shipping the command to another host leaves it behind, so
        // nobody would ever answer and the caller would wait forever. Failing
        // here is the difference between a bug you can see and one you cannot.
        if matches!(self.reach, Reach::Remote(_)) {
            return Err(TellError::AskNotRoutable);
        }
        let (reply, rx) = ReplyTo::channel();
        self.tell(make(reply)).await?;
        rx.await.map_err(|_| TellError::MailboxClosed)
    }
}

/// Handle to the runtime from inside an actor: spawn children, reference self,
/// and reach the journal directly when an actor manages persistence itself.
///
/// Parameterized by the **command** type rather than the actor type, so an actor
/// and an adapter wrapping it (see [`Persistent`]) hand out the same context.
/// Parameterized by actor type they would be two incompatible types for one
/// mailbox.
///
/// [`Persistent`]: crate::Persistent
pub struct ActorContext<C> {
    pub(crate) inner: Arc<SystemInner>,
    pub(crate) self_tx: mpsc::Sender<C>,
}

impl<C: Send + 'static> ActorContext<C> {
    /// A reference to this actor's own mailbox.
    pub fn self_ref(&self) -> ActorRef<C> {
        ActorRef::new(self.self_tx.clone())
    }

    /// Spawn a child actor in the same system.
    pub fn spawn<B: Actor>(&self, actor: B) -> ActorRef<B::Command> {
        crate::system::spawn_in(actor, self.inner.clone())
    }

    /// Spawn an event-sourced child. It recovers from its own `persistence_id`
    /// before accepting commands.
    pub fn spawn_persistent<B: crate::actor::EventSourcedActor>(
        &self,
        actor: B,
    ) -> ActorRef<B::Command> {
        crate::system::spawn_persistent_in(actor, self.inner.clone())
    }

    /// Direct journal access for actors that manage persistence themselves
    /// (e.g. copying a snapshot to seed a forked instance).
    pub fn journal(&self) -> &Arc<dyn Journal> {
        &self.inner.journal
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
    mut ctx: ActorContext<A::Command>,
) {
    let mut stand_down = ctx.inner.stand_down.clone();
    if *stand_down.borrow_and_update() {
        return;
    }

    if let Err(e) = actor.on_start(&mut ctx).await {
        tracing::error!(error = %e, "actor failed to start; shutting down");
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
