use crate::cluster::placement::{PlacementCommand, PlacementTable};
use crate::envelope::{Envelope, Epoch, NodeId};
use crate::transport::{Transport, TransportError};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// How many hosts to try before giving up. Each attempt re-resolves, so this is
/// a bound on "how many dead hosts will I walk past", not a bound on patience
/// with one host.
const SEND_ATTEMPTS: u32 = 3;

/// Pause between attempts that failed for a reason that might pass.
const RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(50);

/// How this node participates in a cluster.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This node's identity. Stable across restarts.
    pub local: NodeId,
    /// The nodes this one expects to see, including itself. Membership starts
    /// from this list and shrinks as nodes are marked down.
    pub members: Vec<NodeId>,
}

/// A node's view of the cluster: who is alive, who hosts what, and how to
/// reach them.
///
/// Placement here is *rendezvous hashing over the live member set*, not a
/// replicated log. Every node computes the same host for an instance without
/// coordinating, and disagreement during a membership change is resolved where
/// it actually matters — the journal, whose `claim_ownership` mints a strictly
/// higher epoch and fences the loser out.
///
/// That is the consequence of moving epoch minting into the journal: agreement
/// stops being what makes hosting safe and becomes what stops two nodes wasting
/// effort claiming the same log. A stronger agreement mechanism is a churn
/// optimisation on top of this, not a correctness prerequisite for it.
pub struct ClusterNode {
    local: NodeId,
    transport: Arc<dyn Transport>,
    table: Mutex<PlacementTable>,
    /// Local half of the message id. Paired with the node id it is unique
    /// cluster-wide without coordination, which is what lets the receiver dedup
    /// retries without a shared counter.
    counter: AtomicU64,
}

impl ClusterNode {
    /// Build a node over `transport`, seeded with `config`'s member list.
    #[must_use]
    pub fn new(config: ClusterConfig, transport: Arc<dyn Transport>) -> Self {
        let mut table = PlacementTable::new();
        for node in &config.members {
            table.apply(PlacementCommand::NodeUp { node: *node });
        }
        table.apply(PlacementCommand::NodeUp { node: config.local });
        Self {
            local: config.local,
            transport,
            table: Mutex::new(table),
            counter: AtomicU64::new(0),
        }
    }

    /// A message id unique across the cluster.
    ///
    /// The node id occupies the high bits and a local counter the low ones, so
    /// two nodes cannot mint the same id and neither has to ask anyone.
    pub fn next_message_id(&self) -> u128 {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        (u128::from(self.local.0) << 64) | u128::from(n)
    }

    /// This node's identity.
    #[must_use]
    pub fn local(&self) -> NodeId {
        self.local
    }

    /// Which node should host `(kind, id)`.
    ///
    /// Prefers a standing assignment, and falls back to the rendezvous
    /// candidate. `None` only when no member is alive, which includes this one
    /// having been marked down.
    #[must_use]
    pub fn owner_of(&self, kind: &str, id: &str) -> Option<NodeId> {
        let table = self.table.lock();
        table
            .owner(kind, id)
            .filter(|n| table.members().contains(n))
            .or_else(|| table.candidate(kind, id))
    }

    /// Whether this node should host `(kind, id)`.
    #[must_use]
    pub fn owns(&self, kind: &str, id: &str) -> bool {
        self.owner_of(kind, id) == Some(self.local)
    }

    /// Record that this node is hosting `(kind, id)`.
    pub fn record_local_assignment(&self, kind: &str, id: &str) {
        self.table.lock().apply(PlacementCommand::Assign {
            kind: kind.to_owned(),
            id: id.to_owned(),
            node: self.local,
        });
    }

    /// Mark a node gone, releasing everything it held so the next message to any
    /// of those instances lands somewhere alive.
    pub fn mark_down(&self, node: NodeId) {
        self.table.lock().apply(PlacementCommand::NodeDown { node });
    }

    /// Mark a node present again.
    pub fn mark_up(&self, node: NodeId) {
        self.table.lock().apply(PlacementCommand::NodeUp { node });
    }

    /// Send an already-encoded command to whichever node hosts `(kind, id)`.
    ///
    /// Retries, because a caller cannot usefully do it: a failure here usually
    /// means the host went away, and the right response is to re-resolve and
    /// aim somewhere alive — which needs the placement table, not the caller.
    /// Each attempt resolves the owner afresh, and an unreachable host is marked
    /// down first, so the next attempt goes elsewhere rather than at the same
    /// corpse.
    ///
    /// This is what makes delivery at-least-once. The `message_id` is what stops
    /// that becoming at-least-twice: the receiving node remembers ids it has
    /// already handled and drops repeats.
    pub async fn send(
        &self,
        kind: &str,
        id: &str,
        epoch: Epoch,
        payload: Vec<u8>,
        message_id: u128,
    ) -> Result<(), TransportError> {
        let mut last = None;
        for attempt in 0..SEND_ATTEMPTS {
            let Some(owner) = self.owner_of(kind, id) else {
                return Err(TransportError::Io(format!(
                    "no live member can host {kind}/{id}"
                )));
            };
            let env = Envelope {
                kind: kind.to_owned(),
                id: id.to_owned(),
                correlation: None,
                message_id,
                epoch,
                payload: payload.clone(),
            };
            match self.transport.send(owner, env).await {
                Ok(()) => return Ok(()),
                Err(TransportError::Unreachable(node)) => {
                    // Take it out of the table before retrying, or every attempt
                    // resolves to the same dead host.
                    self.mark_down(node);
                    last = Some(TransportError::Unreachable(node));
                }
                Err(other) => {
                    // A connection-level failure may be transient, so back off
                    // rather than giving up on a host that is merely busy.
                    last = Some(other);
                    tokio::time::sleep(RETRY_BACKOFF * (attempt + 1)).await;
                }
            }
        }
        Err(last
            .unwrap_or_else(|| TransportError::Io(format!("gave up delivering to {kind}/{id}"))))
    }

    /// The stream of envelopes arriving here, taken once.
    pub fn incoming(&self) -> Option<tokio::sync::mpsc::Receiver<Envelope>> {
        self.transport.incoming()
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
    use crate::transport::InProcessNetwork;

    fn cluster(net: &InProcessNetwork, local: u64, members: &[u64]) -> ClusterNode {
        ClusterNode::new(
            ClusterConfig {
                local: NodeId(local),
                members: members.iter().map(|n| NodeId(*n)).collect(),
            },
            Arc::new(net.node(NodeId(local))),
        )
    }

    /// Every node resolves an instance to the same host without coordinating.
    /// If they disagreed, two of them would claim the same log and one would be
    /// fenced mid-turn for nothing.
    #[tokio::test]
    async fn all_nodes_resolve_an_instance_to_the_same_host() {
        let net = InProcessNetwork::new();
        let a = cluster(&net, 1, &[1, 2, 3]);
        let b = cluster(&net, 2, &[1, 2, 3]);
        let c = cluster(&net, 3, &[1, 2, 3]);
        for id in ["c1", "c2", "c3", "c4", "c5"] {
            let from_a = a.owner_of("counter", id);
            assert_eq!(from_a, b.owner_of("counter", id));
            assert_eq!(from_a, c.owner_of("counter", id));
        }
    }

    /// Exactly one node claims ownership of any instance.
    #[tokio::test]
    async fn exactly_one_node_owns_each_instance() {
        let net = InProcessNetwork::new();
        let nodes: Vec<_> = (1..=3).map(|n| cluster(&net, n, &[1, 2, 3])).collect();
        for id in ["c1", "c2", "c3", "c4", "c5", "c6"] {
            let owners = nodes.iter().filter(|n| n.owns("counter", id)).count();
            assert_eq!(owners, 1, "instance {id} had {owners} owners");
        }
    }

    /// A message addressed to an instance reaches the node that hosts it.
    #[tokio::test]
    async fn a_message_reaches_the_hosting_node() {
        let net = InProcessNetwork::new();
        let a = cluster(&net, 1, &[1, 2]);
        let b = cluster(&net, 2, &[1, 2]);

        // Pick an id node 2 hosts, so the send genuinely crosses a boundary.
        let id = ["c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"]
            .into_iter()
            .find(|id| b.owns("counter", id))
            .expect("some id must land on node 2");

        let mut inbox = b.incoming().unwrap();
        a.send("counter", id, Epoch(1), b"hello".to_vec(), 1)
            .await
            .unwrap();

        let got = inbox.recv().await.unwrap();
        assert_eq!(got.id, id);
        assert_eq!(got.payload, b"hello");
    }

    /// Losing a node moves its instances somewhere alive, and does not move
    /// anybody else's.
    #[tokio::test]
    async fn losing_a_node_reassigns_only_its_own_instances() {
        let net = InProcessNetwork::new();
        let a = cluster(&net, 1, &[1, 2, 3]);
        let ids: Vec<String> = (0..100).map(|i| format!("c{i}")).collect();
        let before: Vec<_> = ids
            .iter()
            .map(|id| (id.clone(), a.owner_of("counter", id).unwrap()))
            .collect();

        a.mark_down(NodeId(3));

        for (id, was) in &before {
            let now = a.owner_of("counter", id).unwrap();
            assert_ne!(now, NodeId(3), "{id} is still assigned to a dead node");
            if *was != NodeId(3) {
                assert_eq!(now, *was, "{id} moved but its host was fine");
            }
        }
    }

    /// A send to a departed node re-resolves onto a live one rather than
    /// failing. The dead host is taken out of the table first, or every attempt
    /// would aim at the same corpse.
    #[tokio::test]
    async fn a_send_re_resolves_past_a_departed_node() {
        let net = InProcessNetwork::new();
        let a = cluster(&net, 1, &[1, 2]);
        let b = cluster(&net, 2, &[1, 2]);

        let id = ["c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"]
            .into_iter()
            .find(|id| b.owns("counter", id))
            .expect("some id must land on node 2");

        net.remove(NodeId(2));
        let mut own_inbox = a.incoming().unwrap();

        a.send("counter", id, Epoch(1), b"x".to_vec(), 1)
            .await
            .expect("the send should have re-resolved onto a live host");

        // Node 1 is the only member left, so it now hosts the instance itself —
        // and the envelope arrived at its own inbox.
        assert!(
            a.owns("counter", id),
            "the instance did not move off the dead node"
        );
        assert_eq!(own_inbox.recv().await.unwrap().payload, b"x");
    }

    /// With nowhere alive to send, the failure is reported rather than retried
    /// forever.
    #[tokio::test]
    async fn a_send_with_no_live_host_fails() {
        let net = InProcessNetwork::new();
        let a = cluster(&net, 1, &[1, 2]);
        net.remove(NodeId(1));
        net.remove(NodeId(2));
        a.mark_down(NodeId(1));

        let err = a
            .send("counter", "c1", Epoch(1), b"x".to_vec(), 1)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            TransportError::Unreachable(_) | TransportError::Io(_)
        ));
    }

    /// A standing assignment wins over the rendezvous candidate, so an instance
    /// already running somewhere is not migrated just because the member set
    /// grew.
    #[tokio::test]
    async fn a_recorded_assignment_survives_a_new_member() {
        let net = InProcessNetwork::new();
        let a = cluster(&net, 1, &[1, 2]);
        let id = ["c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"]
            .into_iter()
            .find(|id| a.owns("counter", id))
            .expect("some id must land on node 1");
        a.record_local_assignment("counter", id);

        a.mark_up(NodeId(9));
        assert!(
            a.owns("counter", id),
            "a running instance was migrated by a membership change"
        );
    }
}
