use serde::{Deserialize, Serialize};

/// Ownership generation for one actor instance.
///
/// Monotonic, assigned by whatever decides placement, and only ever compared —
/// never interpreted. A journal backend records the highest epoch it has seen
/// for an instance and rejects any write carrying a lower one.
///
/// This is the primitive that makes a disputed election survivable. Deciding who
/// owns an actor can be briefly wrong — a partitioned host does not know it lost
/// the argument — but a write carrying a stale epoch fails, so the two hosts
/// cannot merge into one history.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct Epoch(pub u64);

impl std::fmt::Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A cluster member's identity.
///
/// Stable across restarts: placement decisions are recorded against it, so a
/// node that comes back with a different id is a different node as far as
/// ownership is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

/// One addressed message between nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    /// Registered actor type — `ClusterActor::KIND`.
    pub kind: String,
    /// Instance id within that type.
    pub id: String,
    /// Set when the sender wants an answer; echoed on the reply so the sender
    /// can match it to the caller still waiting.
    pub correlation: Option<u128>,
    /// Deduplication key. The receiver remembers these, so a redelivery after a
    /// retry is dropped rather than applied a second time — which is what turns
    /// at-least-once delivery into effectively-once processing.
    pub message_id: u128,
    /// The sender's belief about the owner's generation. A receiver that has
    /// moved past it rejects rather than processes.
    pub epoch: Epoch,
    /// The encoded command.
    pub payload: Vec<u8>,
}
