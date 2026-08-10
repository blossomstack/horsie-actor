use crate::envelope::{Envelope, NodeId};
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;

/// Inbound queue depth per node. Bounded so a node that stops draining exerts
/// backpressure on its senders rather than growing without limit.
const INBOX_CAPACITY: usize = 256;

/// Why an envelope could not be handed over.
#[derive(Debug, Error)]
pub enum TransportError {
    /// The target is not a member, or is no longer reachable.
    #[error("{0} is unreachable")]
    Unreachable(NodeId),

    /// The connection failed. Distinct from `Unreachable` because a caller may
    /// reasonably retry this one against the same node.
    #[error("transport failure: {0}")]
    Io(String),
}

/// Moves envelopes between nodes.
///
/// The whole seam between "which node owns this actor" and "how bytes get
/// there". Two implementations ship: [`InProcessTransport`] for tests, and a
/// TCP one for real deployments. Keeping it a trait is what lets the cluster
/// tests run several genuine nodes inside one process, with real placement and
/// real fencing, rather than asserting against a stand-in.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Hand `env` to `to`.
    ///
    /// Returns once the envelope is accepted for delivery, not once it is
    /// processed — an `Ok` here is not an acknowledgement that anything acted
    /// on it.
    async fn send(&self, to: NodeId, env: Envelope) -> Result<(), TransportError>;

    /// Take the stream of envelopes arriving at this node.
    ///
    /// Returns `None` if it has already been taken; there is exactly one
    /// consumer, because two would silently split the inbound stream between
    /// them.
    fn incoming(&self) -> Option<mpsc::Receiver<Envelope>>;

    /// This node's own identity.
    fn local_id(&self) -> NodeId;
}

/// The shared switchboard several [`InProcessTransport`]s route through.
#[derive(Clone, Default)]
pub struct InProcessNetwork {
    nodes: Arc<Mutex<HashMap<NodeId, mpsc::Sender<Envelope>>>>,
}

impl InProcessNetwork {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a node and get its transport.
    #[must_use]
    pub fn node(&self, id: NodeId) -> InProcessTransport {
        let (tx, rx) = mpsc::channel(INBOX_CAPACITY);
        self.nodes.lock().insert(id, tx);
        InProcessTransport {
            id,
            network: self.clone(),
            inbox: Mutex::new(Some(rx)),
        }
    }

    /// Detach a node: sends to it now fail as unreachable.
    ///
    /// This is how a test kills a host. Note it leaves the node's own transport
    /// alive and still believing it is a member, which is exactly the state a
    /// partitioned host is in — and the state the journal fence exists to make
    /// survivable.
    pub fn remove(&self, id: NodeId) {
        self.nodes.lock().remove(&id);
    }

    /// Whether `id` is currently reachable.
    #[must_use]
    pub fn contains(&self, id: NodeId) -> bool {
        self.nodes.lock().contains_key(&id)
    }
}

/// A [`Transport`] over an [`InProcessNetwork`] — real queues and real
/// backpressure, no sockets.
pub struct InProcessTransport {
    id: NodeId,
    network: InProcessNetwork,
    inbox: Mutex<Option<mpsc::Receiver<Envelope>>>,
}

#[async_trait]
impl Transport for InProcessTransport {
    async fn send(&self, to: NodeId, env: Envelope) -> Result<(), TransportError> {
        // Clone the sender out from under the lock: `send` awaits when the
        // target's inbox is full, and holding a synchronous lock across that
        // await would deadlock every other sender in the process.
        let tx = {
            let nodes = self.network.nodes.lock();
            nodes.get(&to).cloned()
        };
        let tx = tx.ok_or(TransportError::Unreachable(to))?;
        tx.send(env)
            .await
            .map_err(|_| TransportError::Unreachable(to))
    }

    fn incoming(&self) -> Option<mpsc::Receiver<Envelope>> {
        self.inbox.lock().take()
    }

    fn local_id(&self) -> NodeId {
        self.id
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
    use crate::envelope::Epoch;

    fn env(kind: &str, id: &str, payload: &[u8]) -> Envelope {
        Envelope {
            kind: kind.into(),
            id: id.into(),
            correlation: None,
            message_id: 1,
            epoch: Epoch(1),
            payload: payload.to_vec(),
        }
    }

    #[tokio::test]
    async fn delivers_to_the_addressed_node() {
        let net = InProcessNetwork::new();
        let a = net.node(NodeId(1));
        let b = net.node(NodeId(2));
        let mut inbox = b.incoming().unwrap();

        a.send(NodeId(2), env("counter", "c1", b"hello"))
            .await
            .unwrap();

        let got = inbox.recv().await.unwrap();
        assert_eq!(got.kind, "counter");
        assert_eq!(got.payload, b"hello");
    }

    /// Envelopes go to the addressed node only. Obvious, and worth pinning:
    /// a switchboard that broadcast would make every placement test pass for
    /// the wrong reason.
    #[tokio::test]
    async fn does_not_deliver_to_other_nodes() {
        let net = InProcessNetwork::new();
        let a = net.node(NodeId(1));
        let b = net.node(NodeId(2));
        let c = net.node(NodeId(3));
        let mut b_inbox = b.incoming().unwrap();
        let mut c_inbox = c.incoming().unwrap();

        a.send(NodeId(2), env("counter", "c1", b"x")).await.unwrap();

        assert!(b_inbox.recv().await.is_some());
        assert!(
            c_inbox.try_recv().is_err(),
            "an envelope addressed to node 2 reached node 3"
        );
    }

    /// A removed node is unreachable rather than silently swallowing sends.
    /// This is how the failover tests kill a host.
    #[tokio::test]
    async fn sending_to_a_removed_node_fails() {
        let net = InProcessNetwork::new();
        let a = net.node(NodeId(1));
        let _b = net.node(NodeId(2));

        net.remove(NodeId(2));

        let err = a
            .send(NodeId(2), env("counter", "c1", b"x"))
            .await
            .unwrap_err();
        assert!(matches!(err, TransportError::Unreachable(NodeId(2))));
    }

    /// There is exactly one consumer of a node's inbound stream. Two would
    /// split it between them, and each would see an arbitrary half of the
    /// traffic — a failure that looks like random message loss.
    #[tokio::test]
    async fn the_inbox_can_only_be_taken_once() {
        let net = InProcessNetwork::new();
        let a = net.node(NodeId(1));
        assert!(a.incoming().is_some());
        assert!(a.incoming().is_none());
    }

    #[tokio::test]
    async fn an_envelope_round_trips_through_serde() {
        let original = env("counter", "c1", b"payload");
        let bytes = serde_json::to_vec(&original).unwrap();
        let back: Envelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(original, back);
    }
}
