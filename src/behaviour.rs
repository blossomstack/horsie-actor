use crate::error::JournalError;
use crate::runtime::ActorContext;
use async_trait::async_trait;
use thiserror::Error;

/// What the runtime should do after a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    /// Keep processing the mailbox.
    Continue,
    /// Stop the actor, and everything under it. Its mailbox is dropped, so an
    /// [`ActorRef`] to it fails with [`TellError::MailboxClosed`] — until
    /// somebody creates an actor at the same path again, at which point the same
    /// reference reaches the new one. A reference names a path, not an instance.
    ///
    /// Identical to being stopped from outside with [`ActorRef::stop`], and
    /// deliberately: a holder cannot tell which happened, so the two must not
    /// differ.
    ///
    /// [`ActorRef`]: crate::ActorRef
    /// [`ActorRef::stop`]: crate::ActorRef::stop
    /// [`TellError::MailboxClosed`]: crate::TellError::MailboxClosed
    Stop,
}

/// Why an actor refused to start.
#[derive(Debug, Error)]
pub enum StartError {
    /// Replaying the actor's history failed, so it has no trustworthy state to
    /// run from. Starting anyway would silently resume from a partial history.
    #[error("recovery failed: {0}")]
    Recovery(#[from] JournalError),
}

/// The bare mailbox contract: a command type, and one command handled at a time.
///
/// Deliberately says nothing about persistence. Event sourcing is *one*
/// implementation of this trait — see [`Persistent`], which adapts any
/// [`EventSourcedActor`] into an `Actor` — rather than a property of being an
/// actor at all. That split is what lets a stateless actor and an event-sourced
/// one be spawned, addressed and hosted through exactly the same machinery.
///
/// [`Persistent`]: crate::Persistent
/// [`EventSourcedActor`]: crate::EventSourcedActor
#[async_trait]
pub trait Actor: Send + Sized + 'static {
    /// Messages the actor accepts.
    type Command: Send + 'static;

    /// What this actor's parent accepts — and so what
    /// [`ActorContext::parent`] hands back a reference to. [`Root`] for a
    /// top-level actor, whose parent takes no messages at all.
    ///
    /// An associated type rather than a lookup by name, so a child reaching
    /// upwards is checked by the compiler. The parent's *commands* rather than
    /// the parent's type, because that is all a child depends on — and because
    /// it is what lets a test put a double at the parent's path without the
    /// child knowing or the parent's implementation being nameable.
    ///
    /// [`ActorContext::parent`]: crate::ActorContext::parent
    type ParentCommand: Send + 'static;

    /// Handle one command, then say whether to keep going.
    async fn handle(
        &mut self,
        cmd: Self::Command,
        ctx: &mut ActorContext<Self::Command, Self::ParentCommand>,
    ) -> Flow;

    /// Runs once before the first command is handled.
    ///
    /// Returning `Err` aborts startup: the actor processes nothing and its
    /// mailbox closes. That is the honest outcome for a failed recovery — an
    /// actor that cannot rebuild its state has nothing to serve.
    async fn on_start(
        &mut self,
        _ctx: &mut ActorContext<Self::Command, Self::ParentCommand>,
    ) -> Result<(), StartError> {
        Ok(())
    }
}

/// What the root of the tree accepts: nothing.
///
/// `/` exists so that every actor has a parent and a path has somewhere to
/// start. No actor is there and it holds no behaviour — so an actor at the top
/// declares `type ParentCommand = Root` and gets a reference from
/// [`ActorContext::parent`] it can hold and never send through. This type has no
/// values; the type system is what says root takes no messages, rather than a
/// runtime error on the first attempt.
///
/// Note what this does *not* mean: every actor below root does own its children
/// and takes them with it when it stops. Root is the one place the chain ends,
/// because there is nothing there to end it.
///
/// [`ActorContext::parent`]: crate::ActorContext::parent
pub enum Root {}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::wildcard_enum_match_arm
)]
mod tests {
    use super::*;
    use crate::system::ActorSystem;
    use tokio::sync::oneshot;

    struct Adder {
        total: i64,
    }

    enum Cmd {
        Add(i64),
        Get(oneshot::Sender<i64>),
    }

    #[async_trait]
    impl Actor for Adder {
        type Command = Cmd;
        type ParentCommand = Root;

        async fn handle(&mut self, cmd: Cmd, _ctx: &mut ActorContext<Cmd>) -> Flow {
            match cmd {
                Cmd::Add(n) => {
                    self.total += n;
                    Flow::Continue
                }
                Cmd::Get(reply) => {
                    let _ = reply.send(self.total);
                    Flow::Continue
                }
            }
        }
    }

    /// A plain `Actor` has no `persistence_id` and never reads or writes a
    /// journal. That is what makes a non-event-sourced actor possible at all —
    /// and later, what lets a cluster singleton be either kind.
    #[tokio::test]
    async fn plain_actor_needs_no_journal() {
        let system = ActorSystem::in_memory();
        let actor = system.actor_of("adder", Adder { total: 0 }).unwrap();
        actor.tell(Cmd::Add(2)).await.unwrap();
        actor.tell(Cmd::Add(3)).await.unwrap();
        let (tx, rx) = oneshot::channel();
        actor.tell(Cmd::Get(tx)).await.unwrap();
        assert_eq!(rx.await.unwrap(), 5);
    }

    /// `Flow::Stop` closes the mailbox, so the next send fails rather than
    /// hanging or being silently dropped.
    #[tokio::test]
    async fn stop_closes_the_mailbox() {
        struct Stopper;
        #[async_trait]
        impl Actor for Stopper {
            type Command = ();
            type ParentCommand = Root;
            async fn handle(&mut self, _cmd: (), _ctx: &mut ActorContext<()>) -> Flow {
                Flow::Stop
            }
        }

        let system = ActorSystem::in_memory();
        let actor = system.actor_of("stopper", Stopper).unwrap();
        actor.tell(()).await.unwrap();
        // Loop rather than sleep: the stop is processed on the actor's own task,
        // so the close is observable soon but not synchronously.
        for _ in 0..100 {
            if actor.tell(()).await.is_err() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("mailbox never closed after Flow::Stop");
    }
}
