use crate::behaviour::{Actor, Flow};
use crate::error::TellError;
use crate::journal::Journal;
use crate::reply::ReplyTo;
use crate::system::SystemInner;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Mailbox capacity for every spawned actor.
pub(crate) const MAILBOX_CAPACITY: usize = 64;

/// A cheap, cloneable handle for sending commands to an actor.
pub struct ActorRef<C> {
    tx: mpsc::Sender<C>,
}

// Manual `Clone` — a `Sender<C>` clones regardless of whether `C: Clone`.
impl<C> Clone for ActorRef<C> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

// Manual too, and for the same reason: a handle is printable whether or not its
// command type is. There is nothing useful to show beyond liveness — the
// mailbox contents belong to the actor.
impl<C> std::fmt::Debug for ActorRef<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorRef")
            .field("alive", &!self.tx.is_closed())
            .finish()
    }
}

impl<C: Send + 'static> ActorRef<C> {
    pub(crate) fn new(tx: mpsc::Sender<C>) -> Self {
        Self { tx }
    }

    /// Deliver `cmd` to the actor's mailbox, waiting if the mailbox is full.
    /// Fails only if the actor has stopped.
    pub async fn tell(&self, cmd: C) -> Result<(), TellError> {
        self.tx
            .send(cmd)
            .await
            .map_err(|_| TellError::MailboxClosed)
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
    if let Err(e) = actor.on_start(&mut ctx).await {
        tracing::error!(error = %e, "actor failed to start; shutting down");
        return;
    }

    while let Some(cmd) = rx.recv().await {
        match actor.handle(cmd, &mut ctx).await {
            Flow::Continue => {}
            Flow::Stop => break,
        }
    }
}
