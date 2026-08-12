use crate::envelope::NodeId;
use parking_lot::Mutex;
use serde::de::{DeserializeOwned, Error as _};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::oneshot;

/// The caller stopped waiting before the actor answered.
///
/// Not an error worth propagating in most handlers — it is the normal outcome
/// of a cancelled request, a dropped connection, or a timed-out caller.
#[derive(Debug, Error)]
#[error("the caller is no longer waiting for this reply")]
pub struct ReplyDropped;

/// Hands a caller the encoded answer it was waiting for.
pub type Deliver = Box<dyn FnOnce(Vec<u8>) + Send>;

/// What a node needs to be able to do for reply handles to cross it.
///
/// A trait rather than the concrete cluster node because `reply.rs` sits below
/// the cluster layer and must not depend on it — and because the whole surface
/// is these two calls.
pub trait ReplyRouter: Send + Sync + 'static {
    /// This node's identity, which is where a reply must be sent back to.
    fn local(&self) -> NodeId;

    /// Remember a caller waiting for an answer, and return its correlation id.
    ///
    /// `deliver` decodes the answer and hands it over. It is built where the
    /// reply type is known, which is what keeps that type out of this trait.
    fn register(&self, deliver: Deliver) -> u128;

    /// Send an encoded answer back to the node waiting for it.
    fn answer(&self, origin: NodeId, correlation: u128, payload: Vec<u8>);

    /// Tell `origin` that no answer is coming, so it can stop waiting.
    ///
    /// The remote half of dropping a reply handle. Locally that needs no
    /// message — the caller holds the other end of the channel and the drop
    /// wakes it — so this exists to make the two behave the same.
    fn abandon(&self, origin: NodeId, correlation: u128);

    /// Forget a caller on *this* node that has stopped waiting.
    ///
    /// Called when an `ask` is cancelled or times out. Without it the entry sits
    /// in the waiting table holding a channel nobody will read, until capacity
    /// evicts it.
    fn forget(&self, correlation: u128);
}

thread_local! {
    /// The node a command is being encoded for, set only around encoding.
    ///
    /// This is the one piece of ambient state in the crate, and it is here
    /// because serde has no way to pass context into an `impl Serialize`. A
    /// reply handle being encoded has to be registered with *some* node's
    /// waiting-caller table before it goes out, and only the code driving the
    /// encode knows which. Akka does exactly this, for exactly this reason.
    static ROUTER: std::cell::RefCell<Option<Arc<dyn ReplyRouter>>> =
        const { std::cell::RefCell::new(None) };
}

/// Run `f` with `router` available to any [`ReplyTo`] encoded or decoded inside it.
///
/// Nesting is not expected and not supported: the previous value is restored on
/// the way out, so an inner call cannot leak into an outer one.
pub(crate) fn with_router<T>(router: Arc<dyn ReplyRouter>, f: impl FnOnce() -> T) -> T {
    let previous = ROUTER.with(|slot| slot.borrow_mut().replace(router));
    let out = f();
    ROUTER.with(|slot| *slot.borrow_mut() = previous);
    out
}

fn router() -> Option<Arc<dyn ReplyRouter>> {
    ROUTER.with(|slot| slot.borrow().clone())
}

/// Where an actor sends the answer to a request.
///
/// A channel into the asking process when the actor is local, and an address —
/// origin node plus correlation id — once the command carrying it has crossed a
/// host boundary. [`ActorRef::ask`] works the same either way, which is the
/// point: business logic is written once and hosted anywhere.
///
/// [`ActorRef::ask`]: crate::ActorRef::ask
pub struct ReplyTo<R> {
    inner: Inner<R>,
    /// How to tell this node's waiting table that the caller has given up.
    ///
    /// Filled in when a local handle is encoded, because that is the moment a
    /// caller stops being a channel in this process and becomes a row in a
    /// table. Shared with whoever is awaiting the answer, which is what lets a
    /// cancelled `ask` clean up after itself.
    deregister: Deregister,
}

/// Set when a local handle is encoded — see [`ReplyTo::deregister`].
pub(crate) type Deregister = Arc<Mutex<Option<Box<dyn FnOnce() + Send>>>>;

enum Inner<R> {
    /// The caller is in this process.
    ///
    /// The sender is behind a lock and an `Option` because encoding a reply
    /// handle has to *move* it into the waiting-caller table, and `Serialize`
    /// only gets `&self`.
    Local(Mutex<Option<oneshot::Sender<R>>>),
    /// The caller is on another node.
    Remote {
        origin: NodeId,
        correlation: u128,
        /// What this handle can still do, or `None` once it has done it.
        ///
        /// Answering, being forwarded on, and being dropped are the three ways a
        /// handle ends, and exactly one of them may happen. Taking the pair is
        /// how that is enforced — and how the drop knows it is a drop rather
        /// than the tail of an answer.
        armed: Mutex<Option<Armed<R>>>,
    },
}

/// The two things a handle to a caller on another node can do.
struct Armed<R> {
    /// Encode the answer and route it to the origin. Captured when the handle
    /// was decoded, where the reply type was still known.
    answer: Box<dyn FnOnce(R) + Send>,
    /// Tell the origin that no answer is coming.
    abandon: Box<dyn FnOnce() + Send>,
}

/// Dropping a handle that has neither answered nor been passed on fails the
/// caller, wherever the caller is.
///
/// In one process that is free: the caller holds the other end of the channel.
/// Across a host it takes a message, and without one the caller waits forever —
/// the same actor failing in two different ways depending on where it happens to
/// be hosted, which is the distinction this crate exists to remove.
impl<R> Drop for ReplyTo<R> {
    fn drop(&mut self) {
        if let Inner::Remote { armed, .. } = &self.inner
            && let Some(armed) = armed.lock().take()
        {
            (armed.abandon)();
        }
    }
}

impl<R> ReplyTo<R> {
    /// A reply handle and the receiver that awaits it.
    pub(crate) fn channel() -> (Self, oneshot::Receiver<R>) {
        let (tx, rx) = oneshot::channel();
        (Self::from_sender(tx), rx)
    }

    /// Build one over an existing one-shot sender.
    ///
    /// For callers that already own the receiving half — a test asserting on a
    /// reply, or a handler forwarding an answer it was handed.
    pub fn from_sender(tx: oneshot::Sender<R>) -> Self {
        Self {
            inner: Inner::Local(Mutex::new(Some(tx))),
            deregister: Arc::new(Mutex::new(None)),
        }
    }

    /// The shared slot that says how to stop waiting — see [`Self::deregister`].
    pub(crate) fn deregister(&self) -> Deregister {
        self.deregister.clone()
    }

    /// Answer the request.
    ///
    /// # Errors
    /// If the caller has gone away, or if this handle has already been answered
    /// or passed on.
    pub fn send(self, value: R) -> Result<(), ReplyDropped> {
        // Everything is taken through a lock rather than moved out of `self`,
        // because `Drop` above means `self` cannot be destructured. It ends up
        // reading better: answering is "take the right to answer", which is the
        // same operation forwarding and dropping perform.
        match &self.inner {
            Inner::Local(tx) => tx
                .lock()
                .take()
                .ok_or(ReplyDropped)?
                .send(value)
                .map_err(|_| ReplyDropped),
            Inner::Remote { armed, .. } => {
                let armed = armed.lock().take().ok_or(ReplyDropped)?;
                (armed.answer)(value);
                // The answer has been handed to the transport. Whether it
                // arrives is not knowable from here, and a caller that has gone
                // away looks identical to one that has not — so this reports
                // what actually happened, which is that it was sent.
                Ok(())
            }
        }
    }
}

/// Encoding a reply handle registers the caller waiting behind it.
///
/// The `R: DeserializeOwned` bound is where the routable-reply requirement
/// lives, and it is deliberately *here* rather than on `ask`. On `ask` it would
/// spread to every local-only reply type in every consumer — including error
/// types that have no business being serialisable. Here it applies exactly when
/// a reply handle is actually about to cross a host, which the compiler already
/// checks at registration: `ClusterActor` requires its command to round-trip,
/// and a command containing a `ReplyTo<R>` only does if `R` does.
impl<R: DeserializeOwned + Send + 'static> Serialize for ReplyTo<R> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Some(router) = router() else {
            // Loud, never silent. Encoding without a router would produce a
            // handle addressed to nobody, and the symptom would be a caller
            // that waits forever with nothing in any log to explain it.
            return Err(serde::ser::Error::custom(
                "a reply handle was encoded outside a transport context, so nobody would answer it",
            ));
        };

        let wire = match &self.inner {
            Inner::Local(tx) => {
                let Some(tx) = tx.lock().take() else {
                    return Err(serde::ser::Error::custom(
                        "this reply handle has already been encoded or answered",
                    ));
                };
                let correlation = router.register(Box::new(move |payload: Vec<u8>| {
                    match serde_json::from_slice::<R>(&payload) {
                        Ok(value) => {
                            let _ = tx.send(value);
                        }
                        // Dropping the sender fails the caller's `ask` rather
                        // than leaving it waiting on an answer that cannot be
                        // decoded.
                        Err(e) => tracing::warn!(error = %e, "could not decode a reply"),
                    }
                }));
                let waiting = router.clone();
                *self.deregister.lock() = Some(Box::new(move || waiting.forget(correlation)));
                Wire {
                    origin: router.local(),
                    correlation,
                }
            }
            // Already addressed: a handle being forwarded on keeps pointing at
            // whoever originally asked, however many hosts it passes through.
            Inner::Remote {
                origin,
                correlation,
                ..
            } => Wire {
                origin: *origin,
                correlation: *correlation,
            },
        };
        let encoded = wire.serialize(serializer)?;

        // Only now, once the handle is genuinely on its way, does this one stop
        // being responsible for the caller. Disarming before the encode
        // succeeded would strand the caller on a command that never left.
        if let Inner::Remote { armed, .. } = &self.inner {
            drop(armed.lock().take());
        }
        Ok(encoded)
    }
}

/// Decoding a reply handle captures how to answer it.
///
/// The mirror of the bound above, and for the mirror reason: encoding a handle
/// means being able to decode the answer, decoding one means being able to
/// encode it.
impl<'de, R: Serialize + Send + 'static> Deserialize<'de> for ReplyTo<R> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = Wire::deserialize(deserializer)?;
        let Some(router) = router() else {
            return Err(D::Error::custom(
                "a reply handle was decoded outside a transport context, so it could not be answered",
            ));
        };
        let origin = wire.origin;
        let correlation = wire.correlation;
        let dropped = router.clone();
        let undeliverable = router.clone();
        Ok(Self {
            inner: Inner::Remote {
                origin,
                correlation,
                armed: Mutex::new(Some(Armed {
                    answer: Box::new(move |value: R| match serde_json::to_vec(&value) {
                        Ok(payload) => router.answer(origin, correlation, payload),
                        // An answer that will not encode is an answer that is
                        // never arriving, so the caller is told rather than left
                        // waiting on it.
                        Err(e) => {
                            tracing::warn!(error = %e, "could not encode a reply");
                            undeliverable.abandon(origin, correlation);
                        }
                    }),
                    abandon: Box::new(move || dropped.abandon(origin, correlation)),
                })),
            },
            deregister: Arc::new(Mutex::new(None)),
        })
    }
}

/// A reply handle on the wire: who is waiting, and which request they are
/// waiting for.
#[derive(serde::Serialize, serde::Deserialize)]
struct Wire {
    origin: NodeId,
    correlation: u128,
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

    struct FakeRouter {
        local: NodeId,
        waiting: Mutex<Vec<(u128, Deliver)>>,
        answered: Mutex<Vec<(NodeId, u128, Vec<u8>)>>,
        abandoned: Mutex<Vec<(NodeId, u128)>>,
        forgotten: Mutex<Vec<u128>>,
    }

    impl FakeRouter {
        fn new(local: u64) -> Arc<Self> {
            Arc::new(Self {
                local: NodeId(local),
                waiting: Mutex::new(Vec::new()),
                answered: Mutex::new(Vec::new()),
                abandoned: Mutex::new(Vec::new()),
                forgotten: Mutex::new(Vec::new()),
            })
        }
    }

    impl ReplyRouter for FakeRouter {
        fn local(&self) -> NodeId {
            self.local
        }
        fn register(&self, deliver: Deliver) -> u128 {
            let mut waiting = self.waiting.lock();
            let correlation = waiting.len() as u128 + 1;
            waiting.push((correlation, deliver));
            correlation
        }
        fn answer(&self, origin: NodeId, correlation: u128, payload: Vec<u8>) {
            self.answered.lock().push((origin, correlation, payload));
        }
        fn abandon(&self, origin: NodeId, correlation: u128) {
            self.abandoned.lock().push((origin, correlation));
        }
        fn forget(&self, correlation: u128) {
            self.forgotten.lock().push(correlation);
        }
    }

    /// The round trip: encode a handle on one node, decode it on another,
    /// answer it, and have the answer reach the original caller.
    #[tokio::test]
    async fn a_reply_handle_crosses_a_host_and_the_answer_comes_back() {
        let asking = FakeRouter::new(1);
        let hosting = FakeRouter::new(2);

        let (reply, rx) = ReplyTo::<i32>::channel();
        let bytes = with_router(asking.clone(), || serde_json::to_vec(&reply)).unwrap();

        let decoded: ReplyTo<i32> =
            with_router(hosting.clone(), || serde_json::from_slice(&bytes)).unwrap();
        decoded.send(42).unwrap();

        // The hosting node sent the answer back to node 1, quoting the
        // correlation the asking node minted.
        let sent = hosting.answered.lock().pop().unwrap();
        assert_eq!(sent.0, NodeId(1));

        // Which the asking node delivers to the caller still waiting.
        let (correlation, deliver) = asking.waiting.lock().pop().unwrap();
        assert_eq!(correlation, sent.1);
        deliver(sent.2);
        assert_eq!(rx.await.unwrap(), 42);
    }

    /// Encoding without a transport context is an error, never a handle
    /// addressed to nobody. Silence here would surface as a caller that waits
    /// forever with nothing in any log to explain it.
    #[tokio::test]
    async fn encoding_outside_a_transport_context_is_a_loud_error() {
        let (reply, _rx) = ReplyTo::<i32>::channel();
        let outcome = serde_json::to_vec(&reply);
        assert!(outcome.is_err(), "a reply handle encoded with no router");
    }

    #[tokio::test]
    async fn decoding_outside_a_transport_context_is_a_loud_error() {
        let asking = FakeRouter::new(1);
        let (reply, _rx) = ReplyTo::<i32>::channel();
        let bytes = with_router(asking, || serde_json::to_vec(&reply)).unwrap();

        let outcome = serde_json::from_slice::<ReplyTo<i32>>(&bytes);
        assert!(outcome.is_err(), "a reply handle decoded with no router");
    }

    /// A handle passed on to a third node still points at whoever asked, not at
    /// the node doing the forwarding.
    #[tokio::test]
    async fn a_forwarded_handle_still_points_at_the_original_caller() {
        let asking = FakeRouter::new(1);
        let middle = FakeRouter::new(2);

        let (reply, _rx) = ReplyTo::<i32>::channel();
        let first = with_router(asking.clone(), || serde_json::to_vec(&reply)).unwrap();
        let decoded: ReplyTo<i32> =
            with_router(middle.clone(), || serde_json::from_slice(&first)).unwrap();

        let forwarded = with_router(middle, || serde_json::to_vec(&decoded)).unwrap();
        let wire: Wire = serde_json::from_slice(&forwarded).unwrap();
        assert_eq!(
            wire.origin,
            NodeId(1),
            "the forwarding node claimed the reply"
        );
        assert!(
            middle_registered_nothing(&asking),
            "forwarding registered a second waiting caller"
        );
        assert_eq!(wire.correlation, 1);
    }

    fn middle_registered_nothing(asking: &Arc<FakeRouter>) -> bool {
        asking.waiting.lock().len() == 1
    }

    /// The headline: an actor on another node that takes a request and then
    /// drops the handle fails its caller, exactly as it would have in-process.
    /// Without this the caller waits forever, and which of the two happens
    /// depends only on where the actor was hosted.
    #[tokio::test]
    async fn dropping_a_handle_that_crossed_a_host_fails_the_caller() {
        let asking = FakeRouter::new(1);
        let hosting = FakeRouter::new(2);

        let (reply, rx) = ReplyTo::<i32>::channel();
        let bytes = with_router(asking.clone(), || serde_json::to_vec(&reply)).unwrap();
        let decoded: ReplyTo<i32> =
            with_router(hosting.clone(), || serde_json::from_slice(&bytes)).unwrap();

        drop(decoded);

        assert_eq!(
            hosting.abandoned.lock().as_slice(),
            [(NodeId(1), 1)],
            "the hosting node did not tell the origin that no answer was coming"
        );
        // And the origin, told, drops the entry — which is what wakes the
        // caller.
        let (_, deliver) = asking.waiting.lock().pop().unwrap();
        drop(deliver);
        assert!(rx.await.is_err());
    }

    /// Answering is not abandoning. A handle that did its job must not also
    /// report itself dropped, or the answer would race a "never coming" for the
    /// same caller.
    #[tokio::test]
    async fn answering_from_another_host_does_not_also_abandon() {
        let asking = FakeRouter::new(1);
        let hosting = FakeRouter::new(2);

        let (reply, _rx) = ReplyTo::<i32>::channel();
        let bytes = with_router(asking, || serde_json::to_vec(&reply)).unwrap();
        let decoded: ReplyTo<i32> =
            with_router(hosting.clone(), || serde_json::from_slice(&bytes)).unwrap();
        decoded.send(42).unwrap();

        assert!(hosting.abandoned.lock().is_empty());
    }

    /// Nor is forwarding. Passing a handle on hands over the duty to answer, so
    /// the node that let go of it must not report the caller abandoned — the
    /// third node is about to answer them.
    #[tokio::test]
    async fn forwarding_a_handle_does_not_abandon_the_caller() {
        let asking = FakeRouter::new(1);
        let middle = FakeRouter::new(2);

        let (reply, _rx) = ReplyTo::<i32>::channel();
        let first = with_router(asking, || serde_json::to_vec(&reply)).unwrap();
        let decoded: ReplyTo<i32> =
            with_router(middle.clone(), || serde_json::from_slice(&first)).unwrap();

        let _forwarded = with_router(middle.clone(), || serde_json::to_vec(&decoded)).unwrap();
        drop(decoded);

        assert!(
            middle.abandoned.lock().is_empty(),
            "the forwarding node reported a caller it had handed on"
        );
    }

    /// Encoding a handle is the moment a caller becomes a row in a table, so it
    /// is also the moment there is something to undo — which is what a
    /// cancelled `ask` calls.
    #[tokio::test]
    async fn encoding_a_handle_records_how_to_stop_waiting() {
        let asking = FakeRouter::new(1);
        let (reply, _rx) = ReplyTo::<i32>::channel();
        let deregister = reply.deregister();
        assert!(
            deregister.lock().is_none(),
            "nothing to undo before it left"
        );

        with_router(asking.clone(), || serde_json::to_vec(&reply)).unwrap();
        (deregister.lock().take().unwrap())();

        assert_eq!(asking.forgotten.lock().as_slice(), [1]);
    }

    /// Encoding the same handle twice would leave two callers waiting on one
    /// answer, and only one of them could ever get it.
    #[tokio::test]
    async fn a_handle_cannot_be_encoded_twice() {
        let asking = FakeRouter::new(1);
        let (reply, _rx) = ReplyTo::<i32>::channel();

        assert!(with_router(asking.clone(), || serde_json::to_vec(&reply)).is_ok());
        assert!(with_router(asking, || serde_json::to_vec(&reply)).is_err());
    }
}
