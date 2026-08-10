//! The Raft type configuration for membership consensus.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Raft's own node identifier: a plain integer, and the inside of [`NodeId`].
///
/// [`NodeId`]: crate::NodeId
pub type NodeIdx = u64;

/// Who the leader can currently reach, replicated so every node agrees.
///
/// This is the one thing proposed to the log, and it exists because Raft's own
/// membership is the wrong quantity for placement. Membership is *configuration*
/// — the three machines an operator deployed — and it does not change when one
/// of them dies. Placement over configuration would keep sending work to a
/// corpse; placement over "whoever I could reach last" is what let every node
/// invent its own answer, which is the failure this whole layer exists to end.
///
/// So liveness is *observed by one node and agreed by all*: only the leader
/// proposes, every node applies, and rendezvous hashing over the result gives
/// the same host on every node without any of them guessing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveSet {
    /// Members reachable from the leader, including the leader itself.
    pub nodes: BTreeSet<NodeIdx>,
}

impl std::fmt::Display for LiveSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "live[")?;
        for (i, node) in self.nodes.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{node}")?;
        }
        f.write_str("]")
    }
}

openraft::declare_raft_types!(
    /// Consensus over who is in the cluster and which of them are up.
    ///
    /// Deliberately the smallest state machine that does the job: a member set
    /// (Raft's own configuration) and a live set (one [`LiveSet`] entry whenever
    /// the leader's view changes). Actor journals never come near it, so the log
    /// takes an entry per failure or recovery rather than per write — a few a
    /// day in a healthy cluster, not a few thousand a second.
    pub Membership:
        D = LiveSet,
        R = (),
        Node = openraft::impls::BasicNode,
);
