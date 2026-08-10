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
//! [`ActorSystem`] owns the journal, the registry of actor types that can be
//! reached by id, and the instances currently running. Registered types can be
//! hosted across several nodes: membership is agreed by Raft, placement hashes
//! over the members the leader reports as live, replies route back to whoever
//! asked, and a node that cannot see a quorum stops what it hosts. None of that is what keeps a log consistent —
//! [`Journal::persist`] is conditional on the sequence number the writer
//! believes the log ends at, and that is the only part which holds for a
//! process frozen through a failover.
//!
//! Neither agent nor workflow concepts appear here.

mod actor;
mod behaviour;
mod cluster;
mod envelope;
mod error;
mod journal;
mod persistence_id;
mod persistent;
mod reply;
mod runtime;
mod system;
#[cfg(any(test, feature = "test-util"))]
pub mod testkit;
mod transport;
mod transport_tcp;

pub use actor::{CommandEffect, EventSourcedActor};
pub use behaviour::{Actor, Flow, StartError};
pub use cluster::{
    Assignment, ClusterConfig, ClusterNode, Dedup, InstanceKey, LiveSet, Membership, NodeIdx,
    PlacementCommand, PlacementEffect, PlacementTable, RaftStore, serve_consensus,
};
pub use envelope::{Envelope, Message, NodeId, Reply};
pub use error::{JournalError, TellError};
pub use journal::{InMemoryJournal, Journal, JournalResult};
// Re-exported so a caller can read `ClusterNode::raft().metrics()` without
// taking its own dependency on openraft.
pub use openraft::type_config::async_runtime::watch::WatchReceiver;
pub use persistence_id::PersistenceId;
pub use persistent::Persistent;
pub use reply::{ReplyDropped, ReplyRouter, ReplyTo};
pub use runtime::{ActorContext, ActorRef};
pub use system::{ActorOfError, ActorSystem, ClusterActor, DispatchError};
pub use transport::{InProcessNetwork, InProcessTransport, RpcRequest, Transport, TransportError};
pub use transport_tcp::{TcpConfig, TcpTransport};
