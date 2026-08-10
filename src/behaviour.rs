use crate::error::JournalError;
use crate::runtime::ActorContext;
use async_trait::async_trait;
use thiserror::Error;

/// What the runtime should do after a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    /// Keep processing the mailbox.
    Continue,
    /// Stop the actor. Its mailbox is dropped, and every [`ActorRef`] to it
    /// starts failing with [`TellError::MailboxClosed`].
    ///
    /// [`ActorRef`]: crate::ActorRef
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

    /// Handle one command, then say whether to keep going.
    async fn handle(&mut self, cmd: Self::Command, ctx: &mut ActorContext<Self::Command>) -> Flow;

    /// Runs once before the first command is handled.
    ///
    /// Returning `Err` aborts startup: the actor processes nothing and its
    /// mailbox closes. That is the honest outcome for a failed recovery — an
    /// actor that cannot rebuild its state has nothing to serve.
    async fn on_start(&mut self, _ctx: &mut ActorContext<Self::Command>) -> Result<(), StartError> {
        Ok(())
    }
}

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
        let actor = system.spawn(Adder { total: 0 });
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
            async fn handle(&mut self, _cmd: (), _ctx: &mut ActorContext<()>) -> Flow {
                Flow::Stop
            }
        }

        let system = ActorSystem::in_memory();
        let actor = system.spawn(Stopper);
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
