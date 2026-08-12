//! Generic, domain-free actor runtime with optional event sourcing.
//!
//! [`Actor`] is the bare contract: a command type, and one command handled at a
//! time. [`EventSourcedActor`] layers durability on top — its state is rebuilt
//! by replaying persisted events, so a fresh instance with the same
//! `persistence_id` recovers exactly where the previous one left off. The
//! [`Persistent`] adapter turns the second into the first, which is what keeps
//! persistence out of the mailbox loop and lets both kinds be spawned,
//! addressed and hosted identically.
//!
//! Every actor has an [`ActorPath`] — `/acct-7/session-3/agent-main` — and an
//! [`ActorRef`] is that path plus a cached link to whatever it resolves to right
//! now. A send that fails drops the cache, resolves once more and retries, so a
//! reference held across a restart or a reactivation keeps working without its
//! holder knowing anything happened. A path is created by its parent and never
//! by resolution: a reference cannot wake an actor nobody asked for.
//!
//! An actor owns the actors below it: stopping one stops its whole subtree,
//! children first, whether it was stopped from outside, stopped itself, or lost
//! its node. So a branch is unloaded in one call, and no actor outlives the one
//! that created it.
//!
//! An actor tree is node-local: a parent and its children are always on the same
//! machine, and clustering happens only at the roots. A [`Shard`] type is
//! registered once per node with that node's own wiring, and everything below a
//! shard root is an ordinary local child created by its parent.
//!
//! [`ActorSystem`] owns the journal, every actor currently running (keyed by
//! path), and the recipe for each registered shard type. Shard types can be
//! hosted across several nodes: membership is agreed by Raft, placement hashes
//! over the members the leader reports as live, replies route back to whoever
//! asked, and a node that cannot see a quorum stops what it hosts.
//!
//! None of that is what keeps a log consistent. [`Journal::persist`] is
//! conditional on the sequence number the writer believes the log ends at, and
//! that is the only part which holds for a process frozen through a failover.
//!
//! Neither agent nor workflow concepts appear here.

mod actor;
mod behaviour;
mod cluster;
mod envelope;
mod error;
mod journal;
mod path;
mod persistence_id;
mod persistent;
mod reply;
mod runtime;
mod shard;
mod system;
#[cfg(any(test, feature = "test-util"))]
pub mod testkit;
mod transport;
mod transport_tcp;

pub use actor::{CommandEffect, EventSourcedActor};
pub use behaviour::{Actor, Flow, StartError};
pub use cluster::{
    ClusterConfig, ClusterNode, Dedup, LiveSet, Membership, NodeIdx, PlacementTable, RaftStore,
    serve_consensus,
};
pub use envelope::{Envelope, Message, NodeId, Reply};
pub use error::{JournalError, TellError};
pub use journal::{InMemoryJournal, Journal, JournalResult};
// Re-exported so a caller can read `ClusterNode::raft().metrics()` without
// taking its own dependency on openraft.
pub use openraft::type_config::async_runtime::watch::WatchReceiver;
pub use path::{ActorPath, InvalidPath, is_valid_name};
pub use persistence_id::PersistenceId;
pub use persistent::Persistent;
pub use reply::{ReplyDropped, ReplyRouter, ReplyTo};
pub use runtime::{ActorContext, ActorRef};
pub use shard::{AddressPart, EntityContext, Shard, UnreadableAddress};
pub use system::{ActorOfError, ActorSystem, DispatchError, ShardOf};
pub use transport::{InProcessNetwork, InProcessTransport, RpcRequest, Transport, TransportError};
pub use transport_tcp::{TcpConfig, TcpTransport};
