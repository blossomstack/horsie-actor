use thiserror::Error;
use tokio::sync::oneshot;

/// The caller stopped waiting before the actor answered.
///
/// Not an error worth propagating in most handlers — it is the normal outcome
/// of a cancelled request, a dropped connection, or a timed-out caller.
#[derive(Debug, Error)]
#[error("the caller is no longer waiting for this reply")]
pub struct ReplyDropped;

/// Where an actor sends the answer to a request.
///
/// A thin wrapper over a one-shot channel today. It is its own type — rather
/// than `oneshot::Sender<R>` spelled out at every call site — because a reply
/// handle has to survive being carried to another host once actors can be
/// hosted remotely, and a `oneshot::Sender` cannot. Introducing that later would
/// mean touching every command variant in every consumer; introducing it now
/// costs a rename.
///
/// # It does not cross a host boundary yet
///
/// This is a channel into the asking process. Ship the command elsewhere and
/// the handle stays behind, so nobody answers. [`ActorRef::ask`] therefore
/// refuses a remote target outright rather than letting the caller wait
/// forever.
///
/// Note the shape of the trap if you work around that: giving a command a
/// `#[serde(skip)] Option<ReplyTo<_>>` field makes it encodable, and it then
/// arrives as `None`, the handler answers nobody, and the caller hangs. The
/// crate cannot detect that — only the refusal in `ask` stands between you and
/// it.
///
/// [`ActorRef::ask`]: crate::ActorRef::ask
pub struct ReplyTo<R> {
    tx: oneshot::Sender<R>,
}

impl<R> ReplyTo<R> {
    /// A reply handle and the receiver that awaits it.
    pub(crate) fn channel() -> (Self, oneshot::Receiver<R>) {
        let (tx, rx) = oneshot::channel();
        (Self { tx }, rx)
    }

    /// Build one over an existing one-shot sender.
    ///
    /// For callers that already own the receiving half — a test asserting on a
    /// reply, or a handler forwarding an answer it was handed.
    pub fn from_sender(tx: oneshot::Sender<R>) -> Self {
        Self { tx }
    }

    /// Answer the request.
    pub fn send(self, value: R) -> Result<(), ReplyDropped> {
        self.tx.send(value).map_err(|_| ReplyDropped)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_delivers_to_the_receiver() {
        let (reply, rx) = ReplyTo::channel();
        reply.send(7).unwrap();
        assert_eq!(rx.await.unwrap(), 7);
    }

    /// A dropped caller is reported, not panicked over: an actor answering a
    /// request whose caller has gone away is routine, and treating it as a
    /// failure would take the actor down with every cancelled request.
    #[tokio::test]
    async fn send_reports_a_dropped_caller() {
        let (reply, rx) = ReplyTo::<i32>::channel();
        drop(rx);
        assert!(reply.send(7).is_err());
    }
}
