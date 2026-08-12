use serde::{Deserialize, Serialize};

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

/// An answer travelling back to whoever asked.
///
/// Not addressed to an actor, which is why it is not an [`Envelope`]: a reply
/// belongs to a *caller*, and the correlation id is how the origin node finds
/// the one still waiting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reply {
    /// Minted by the origin node when the reply handle was encoded.
    pub correlation: u128,
    /// The encoded answer, or `None` for "no answer is coming".
    ///
    /// A handle dropped without being answered has to say so. In one process
    /// dropping it wakes the caller by itself, because the caller is holding the
    /// other end of the channel; once the handle has crossed a host, nothing is
    /// left to notice — so the drop is sent back explicitly, and the caller
    /// fails exactly as it would have locally.
    pub payload: Option<Vec<u8>>,
}

/// Anything one node sends another.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    /// A command for an actor.
    Command(Envelope),
    /// An answer for a caller.
    Reply(Reply),
}

/// One addressed message between nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    /// Who it is for, as an [`ActorPath`] in its display form. Text because an
    /// envelope is a wire type, and every reader parses it straight back.
    ///
    /// [`ActorPath`]: crate::ActorPath
    pub path: String,
    /// Deduplication key. The receiver remembers these, so a redelivery after a
    /// retry is dropped rather than applied a second time — which is what turns
    /// at-least-once delivery into effectively-once processing.
    pub message_id: u128,
    /// The encoded command.
    pub payload: Vec<u8>,
}
