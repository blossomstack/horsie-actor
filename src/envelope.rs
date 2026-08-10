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
