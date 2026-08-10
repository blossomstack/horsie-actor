//! Hosting actors across several nodes.
//!
//! Three layers, and it is worth keeping them apart. **Membership** is who is in
//! the cluster, held by Raft so no node can invent its own answer. **Placement**
//! is which member hosts an instance, computed by hashing over that member set
//! so every node agrees without asking anyone. **The write fence** is the
//! conditional append in the journal, which is what makes a wrong answer to
//! either of the first two survivable rather than corrupting.
//!
//! Only the third is on the safety path. Agreement stops two nodes wasting
//! effort on the same instance; it is not what stops them merging one history.

mod delivery;
mod network;
mod node;
mod placement;
mod store;
mod types;

pub use delivery::Dedup;
pub use network::serve_consensus;
pub use node::{ClusterConfig, ClusterNode};
pub use placement::{Assignment, InstanceKey, PlacementCommand, PlacementEffect, PlacementTable};
pub use store::RaftStore;
pub use types::{LiveSet, Membership, NodeIdx};
