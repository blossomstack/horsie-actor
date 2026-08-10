//! Consensus messages over the cluster transport.
//!
//! Raft needs request/response; actor delivery does not. Rather than give
//! consensus its own listener, port and handshake, both ride the one connection
//! the transport already authenticates — [`Transport::rpc`] carries the request,
//! and [`serve_consensus`] answers it on the far side.

use crate::cluster::store::RaftStore;
use crate::cluster::types::{Membership, NodeIdx};
use crate::envelope::NodeId;
use crate::transport::Transport;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotMetaOf, SnapshotOf, VoteOf};
use openraft::{Raft, impls::BasicNode};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::sync::Arc;

/// One consensus message on the wire.
#[derive(Serialize, Deserialize)]
enum Request {
    Append(AppendEntriesRequest<Membership>),
    Vote(VoteRequest<Membership>),
    /// The snapshot is small enough — a log id and a member set — to send whole
    /// rather than stream. Chunking exists for state machines that hold real
    /// data; this one holds who is in the cluster.
    Snapshot {
        vote: VoteOf<Membership>,
        meta: SnapshotMetaOf<Membership>,
        data: Vec<u8>,
    },
}

/// The answer to one consensus message.
///
/// `Failed` carries the far side's error as text. Raft's own error types are
/// rich, and reconstructing them across the wire would mean serialising a type
/// hierarchy that changes between versions; a node that reports a failure is
/// treated as unreachable, which is the same conclusion by a shorter route.
#[derive(Serialize, Deserialize)]
enum Reply {
    Append(AppendEntriesResponse<Membership>),
    Vote(VoteResponse<Membership>),
    Snapshot(SnapshotResponse<Membership>),
    Failed(String),
}

/// Answer consensus messages arriving at this node, until the transport closes.
///
/// Spawn one of these per node. Without it a cluster elects nobody: every vote
/// goes out and none comes back, so each node campaigns forever and no member
/// set is ever agreed.
pub async fn serve_consensus(transport: Arc<dyn Transport>, raft: Raft<Membership, RaftStore>) {
    let Some(mut inbox) = transport.incoming_rpc() else {
        tracing::error!("the consensus inbox was already taken; this node cannot answer votes");
        return;
    };

    while let Some(request) = inbox.recv().await {
        let raft = raft.clone();
        // Per request, because answering must not block the next one: a
        // replication round trip would otherwise sit behind whatever came first.
        tokio::spawn(async move {
            let reply = match serde_json::from_slice::<Request>(&request.payload) {
                Ok(Request::Append(rpc)) => match raft.append_entries(rpc).await {
                    Ok(r) => Reply::Append(r),
                    Err(e) => Reply::Failed(e.to_string()),
                },
                Ok(Request::Vote(rpc)) => match raft.vote(rpc).await {
                    Ok(r) => Reply::Vote(r),
                    Err(e) => Reply::Failed(e.to_string()),
                },
                Ok(Request::Snapshot { vote, meta, data }) => {
                    let snapshot = openraft::storage::Snapshot {
                        meta,
                        snapshot: data,
                    };
                    match raft.install_full_snapshot(vote, snapshot).await {
                        Ok(r) => Reply::Snapshot(r),
                        Err(e) => Reply::Failed(e.to_string()),
                    }
                }
                Err(e) => Reply::Failed(format!("undecodable consensus message: {e}")),
            };
            match serde_json::to_vec(&reply) {
                Ok(bytes) => {
                    let _ = request.reply.send(bytes);
                }
                // Dropping the responder fails the caller's request, which is
                // the honest outcome: we cannot answer, and silence would make
                // it wait out a timeout to learn the same thing.
                Err(e) => tracing::warn!(error = %e, "could not encode a consensus reply"),
            }
        });
    }
}

/// Builds a client per peer. Holds no connection itself — the transport caches
/// those.
pub struct ConsensusNetwork {
    transport: Arc<dyn Transport>,
}

impl ConsensusNetwork {
    pub fn new(transport: Arc<dyn Transport>) -> Self {
        Self { transport }
    }
}

impl RaftNetworkFactory<Membership> for ConsensusNetwork {
    type Network = PeerLink;

    async fn new_client(&mut self, target: NodeIdx, _node: &BasicNode) -> Self::Network {
        PeerLink {
            transport: self.transport.clone(),
            target: NodeId(target),
        }
    }
}

/// The link to one peer.
pub struct PeerLink {
    transport: Arc<dyn Transport>,
    target: NodeId,
}

impl PeerLink {
    /// Send one message and decode the answer.
    ///
    /// Every failure — unreachable, undecodable, or an error reported by the
    /// peer — comes back as [`Unreachable`]. That is not laziness: Raft's
    /// response to all three is the same, which is to back off and try again,
    /// and a network layer that invented finer distinctions would be asserting
    /// something it cannot know from here.
    async fn call(&self, request: &Request) -> Result<Reply, RPCError<Membership>> {
        let payload =
            serde_json::to_vec(request).map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let bytes = self
            .transport
            .rpc(self.target, payload)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        serde_json::from_slice(&bytes).map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }
}

/// A reply of the wrong shape means the peer is running a protocol we are not.
fn mismatched(what: &str) -> RPCError<Membership> {
    RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("peer answered a {what} with something else"),
    )))
}

impl RaftNetworkV2<Membership> for PeerLink {
    type SnapshotData = Vec<u8>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<Membership>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<Membership>, RPCError<Membership>> {
        match self.call(&Request::Append(rpc)).await? {
            Reply::Append(r) => Ok(r),
            Reply::Failed(e) => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other(e),
            ))),
            Reply::Vote(_) | Reply::Snapshot(_) => Err(mismatched("append")),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<Membership>,
        _option: RPCOption,
    ) -> Result<VoteResponse<Membership>, RPCError<Membership>> {
        match self.call(&Request::Vote(rpc)).await? {
            Reply::Vote(r) => Ok(r),
            Reply::Failed(e) => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::other(e),
            ))),
            Reply::Append(_) | Reply::Snapshot(_) => Err(mismatched("vote")),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<Membership>,
        snapshot: SnapshotOf<Membership, Vec<u8>>,
        _cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<Membership>, StreamingError<Membership>> {
        let request = Request::Snapshot {
            vote,
            meta: snapshot.meta,
            data: snapshot.snapshot,
        };
        match self.call(&request).await? {
            Reply::Snapshot(r) => Ok(r),
            Reply::Failed(e) => Err(StreamingError::Unreachable(Unreachable::new(
                &std::io::Error::other(e),
            ))),
            Reply::Append(_) | Reply::Vote(_) => Err(StreamingError::Unreachable(
                Unreachable::new(&std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "peer answered a snapshot with something else",
                )),
            )),
        }
    }
}
