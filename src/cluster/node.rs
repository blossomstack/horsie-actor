use crate::cluster::network::ConsensusNetwork;
use crate::cluster::placement::PlacementTable;
use crate::cluster::store::RaftStore;
use crate::cluster::types::{LiveSet, Membership, NodeIdx};
use crate::envelope::{Envelope, Message, NodeId, Reply};
use crate::reply::ReplyRouter;
use crate::transport::{Transport, TransportError};
use openraft::type_config::async_runtime::watch::WatchReceiver;
use openraft::{Instant, Raft, ServerState};
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// How many hosts to try before giving up. Each attempt re-resolves, so this is
/// a bound on "how many dead hosts will I walk past", not a bound on patience
/// with one host.
const SEND_ATTEMPTS: u32 = 3;

/// Pause between attempts that failed for a reason that might pass.
const RETRY_BACKOFF: Duration = Duration::from_millis(50);

/// How often the leader re-examines who it can reach.
const LIVENESS_TICK: Duration = Duration::from_millis(200);

/// How many callers may be waiting on an answer at once.
///
/// A bound rather than a timeout, because there is no right timeout: an actor
/// may legitimately take a long time to answer. Entries normally leave on their
/// own — the far end answers, drops its handle, or the caller is cancelled and
/// deregisters — so this is the backstop for the case none of those reach: a
/// host that went away mid-request. Overflowing evicts the oldest, whose caller
/// has waited longest; it fails rather than hangs, which is the failure worth
/// having.
const WAITING_CAPACITY: usize = 8192;

/// How this node participates in a cluster.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This node's identity. Stable across restarts — a node that comes back
    /// under a different id is a different node as far as consensus is
    /// concerned.
    pub local: NodeId,
    /// The members to form a brand-new cluster from, including this node.
    ///
    /// Used **once**, when the Raft store is empty. After that membership lives
    /// in the log, so this list stops being consulted and cannot drift away
    /// from reality — which is what a peer list maintained by hand does.
    pub bootstrap: Vec<NodeId>,
    /// How long a peer may go unacknowledged before the leader stops counting
    /// it as live.
    ///
    /// The dial that matters. Short means fast failover and instances moved by
    /// a network blip; long means fewer false alarms and slower recovery.
    /// Nothing here makes that choice free.
    pub liveness_window: Duration,
}

impl ClusterConfig {
    /// A config for `local` in a cluster bootstrapped from `members`.
    #[must_use]
    pub fn new(local: NodeId, members: Vec<NodeId>) -> Self {
        Self {
            local,
            bootstrap: members,
            liveness_window: Duration::from_secs(3),
        }
    }
}

/// A node's view of the cluster: who is in it, who is up, who hosts what, and
/// how to reach them.
///
/// Two questions, kept apart. **Membership** is agreed by Raft and changes when
/// an operator scales the cluster. **Liveness** is observed by the leader and
/// replicated, so it changes when a machine dies. Placement is rendezvous
/// hashing over their intersection, which every node computes identically
/// because both inputs came out of the same log.
///
/// What this replaces is a node deciding for itself: the previous version
/// dropped a peer from its own member list the moment one send to it failed, so
/// a brief fault left two nodes hashing over different sets, both hosting the
/// same instances, and each rejecting the other's writes in turn. Neither side
/// made progress. A node can no longer form that opinion.
pub struct ClusterNode {
    local: NodeId,
    transport: Arc<dyn Transport>,
    store: RaftStore,
    raft: Raft<Membership, RaftStore>,
    table: Mutex<PlacementTable>,
    /// Whether this node may host or serve anything. See [`ClusterNode::serving`].
    serving: AtomicBool,
    /// The same flag, watchable, so the actor system can stop what it hosts the
    /// moment this node stands down.
    serving_tx: tokio::sync::watch::Sender<bool>,
    /// Local half of the message id. Paired with the node id it is unique
    /// cluster-wide without coordination, which is what lets the receiver dedup
    /// retries without a shared counter.
    counter: AtomicU64,
    /// Callers on this node waiting for an answer from somewhere else.
    ///
    /// Deliberately not durable, and deliberately not recovered. A reply is a
    /// caller sitting on an `await`, and a process that restarts has no caller
    /// left to answer — persisting this would only produce answers nobody is
    /// listening for.
    ///
    /// Ordered, so overflow evicts the oldest: correlation ids are minted
    /// monotonically per node, which makes the lowest key the longest wait.
    waiting: Mutex<BTreeMap<u128, crate::reply::Deliver>>,
    /// Local half of the correlation id, minted the same way as message ids.
    correlations: AtomicU64,
}

impl ClusterNode {
    /// Start consensus and return the node.
    ///
    /// Spawns three things: the loop that answers peers' consensus messages, the
    /// loop that keeps placement in step with the agreed live set, and — on
    /// whichever node is leader — the loop that observes liveness and proposes
    /// it.
    ///
    /// # Errors
    /// If Raft cannot be started over `store`.
    pub async fn start(
        config: ClusterConfig,
        transport: Arc<dyn Transport>,
        store: RaftStore,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error + Send + Sync>> {
        let raft_config = Arc::new(
            openraft::Config {
                // Election timing is bounded by the liveness window: a cluster
                // that takes longer to elect than it takes to declare a peer
                // dead would spend the gap with nothing agreed and nobody
                // serving.
                heartbeat_interval: 100,
                election_timeout_min: 300,
                election_timeout_max: 600,
                ..Default::default()
            }
            .validate()?,
        );

        let raft = Raft::new(
            config.local.0,
            raft_config,
            ConsensusNetwork::new(transport.clone()),
            store.clone(),
            store.clone(),
        )
        .await?;

        tokio::spawn(crate::cluster::network::serve_consensus(
            transport.clone(),
            raft.clone(),
        ));

        // Only on a genuinely fresh store. Initialising an existing cluster
        // would propose a membership that contradicts its own log.
        if raft.is_initialized().await? {
            tracing::debug!("joining an existing cluster from the local raft store");
        } else {
            let members: BTreeMap<NodeIdx, openraft::impls::BasicNode> = config
                .bootstrap
                .iter()
                .map(|n| (n.0, openraft::impls::BasicNode::default()))
                .collect();
            // A losing race is normal, not an error: every node bootstraps the
            // same member set, and the first one through wins.
            if let Err(e) = raft.initialize(members).await {
                tracing::debug!(error = %e, "another node had already formed the cluster");
            }
        }

        let (serving_tx, _) = tokio::sync::watch::channel(false);
        let node = Arc::new(Self {
            local: config.local,
            transport,
            store,
            raft,
            table: Mutex::new(PlacementTable::new()),
            serving: AtomicBool::new(false),
            serving_tx,
            counter: AtomicU64::new(0),
            waiting: Mutex::new(BTreeMap::new()),
            correlations: AtomicU64::new(0),
        });

        tokio::spawn(watch_cluster(node.clone(), config.liveness_window));
        Ok(node)
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

    /// Whether this node may host actors and answer requests.
    ///
    /// False while it cannot see a leader — which is what being in a minority
    /// looks like from inside one. A node that has lost touch with a quorum
    /// cannot know whether its instances have been given to somebody else, so
    /// it stops rather than serve state that may already be history.
    ///
    /// Note what this does *not* do: it is not what keeps the cluster safe. A
    /// node discovers it has lost quorum only after a timeout, and a frozen
    /// process discovers it even later. Safety comes from the journal's
    /// conditional append, which rejects a stale writer whenever it wakes up.
    /// This bounds how long a displaced node keeps answering *reads*, which
    /// nothing else does.
    #[must_use]
    pub fn serving(&self) -> bool {
        self.serving.load(Ordering::Relaxed)
    }

    /// Watch this node's serving state.
    #[must_use]
    pub fn serving_watch(&self) -> tokio::sync::watch::Receiver<bool> {
        self.serving_tx.subscribe()
    }

    /// The Raft handle, for membership changes and metrics.
    #[must_use]
    pub fn raft(&self) -> &Raft<Membership, RaftStore> {
        &self.raft
    }

    /// Which node should host the actor at `path`.
    ///
    /// Hashed over the agreed live set, so every node answers this identically
    /// without asking anyone. `None` when no member is live — including this
    /// one, which is the state a stood-down node is in.
    #[must_use]
    pub fn owner_of(&self, path: &str) -> Option<NodeId> {
        self.table.lock().owner_of(path)
    }

    /// Whether this node should host the actor at `path`.
    #[must_use]
    pub fn owns(&self, path: &str) -> bool {
        self.serving() && self.owner_of(path) == Some(self.local)
    }

    /// The nodes placement is currently using.
    ///
    /// This is the agreed live set as this node last applied it, so two nodes
    /// reporting different lists means one of them has not caught up yet — not
    /// that they disagree.
    #[must_use]
    pub fn live_members(&self) -> Vec<NodeId> {
        self.table.lock().members().iter().copied().collect()
    }

    /// Replace the set of nodes placement may use.
    ///
    /// Driven only by the agreed live set. There is deliberately no way for a
    /// caller to mark a node down: a failed send means the send failed, and
    /// letting it rewrite placement is precisely how a partitioned node used to
    /// elect itself host of everything.
    fn set_live(&self, live: &[NodeId]) {
        self.table.lock().set_members(live.iter().copied());
    }

    /// Send an already-encoded command to whichever node hosts `route`.
    ///
    /// `route` is the shard the target belongs to, which is what placement
    /// decides over, and `address` is the actor itself. They differ because many
    /// entities share one shard, so the envelope has to carry the full path for
    /// the receiving node to know which of them it is for.
    ///
    /// Retries, because a caller cannot usefully do it: each attempt re-resolves
    /// the owner, so a send racing a failover lands on the new host rather than
    /// failing. It does **not** retry forever, and it makes no durability
    /// promise — an undeliverable command is dropped and its caller told.
    pub async fn send(
        &self,
        route: &str,
        address: &str,
        payload: Vec<u8>,
        message_id: u128,
    ) -> Result<(), TransportError> {
        let mut last = None;
        for attempt in 0..SEND_ATTEMPTS {
            let Some(owner) = self.owner_of(route) else {
                return Err(TransportError::Io(format!(
                    "no live member can host {route}"
                )));
            };
            let env = Envelope {
                path: address.to_owned(),
                message_id,
                payload: payload.clone(),
            };
            match self.transport.send(owner, Message::Command(env)).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    // Back off and re-resolve. The placement table is not
                    // touched: whether that host is really gone is the leader's
                    // observation to make, not this caller's.
                    last = Some(e);
                    tokio::time::sleep(RETRY_BACKOFF * (attempt + 1)).await;
                }
            }
        }
        Err(last.unwrap_or_else(|| TransportError::Io(format!("gave up delivering to {address}"))))
    }

    /// The stream of messages arriving here, taken once.
    pub fn incoming(&self) -> Option<tokio::sync::mpsc::Receiver<Message>> {
        self.transport.incoming()
    }

    /// Hand an inbound answer to whoever is waiting for it.
    ///
    /// An answer with nobody waiting is dropped and logged, not an error: the
    /// caller may have timed out, been cancelled, or gone away with the actor
    /// that asked. It is the ordinary end of a request nobody needed any more.
    ///
    /// A reply with no payload is the far end saying it will never answer.
    /// Dropping the entry drops the channel behind it, so the caller fails now
    /// rather than waiting on nothing — which is what dropping the handle would
    /// have done had the actor been in this process.
    pub fn deliver_reply(&self, reply: Reply) {
        let Some(deliver) = self.waiting.lock().remove(&reply.correlation) else {
            tracing::debug!(
                correlation = reply.correlation,
                "an answer arrived for a caller that had gone"
            );
            return;
        };
        match reply.payload {
            Some(payload) => deliver(payload),
            None => drop(deliver),
        }
    }

    /// How many callers on this node are waiting for an answer from elsewhere.
    ///
    /// A number that should sit near zero and come back down: every request that
    /// ends — answered, refused, dropped, cancelled — takes its entry with it.
    /// One that climbs is a leak, and one at [`WAITING_CAPACITY`] is failing the
    /// oldest caller for every new one.
    #[must_use]
    pub fn waiting(&self) -> usize {
        self.waiting.lock().len()
    }

    /// Fail every caller on this node that is still waiting for an answer.
    ///
    /// Called when the node stands down. Its instances may already belong to
    /// somebody else and its peers have stopped talking to it, so no answer it
    /// is waiting on is still coming. Failing them here is the same loss the
    /// stand-down already imposes on actors, extended to the callers that are
    /// not actors.
    pub fn abandon_waiting(&self) {
        let waiting = std::mem::take(&mut *self.waiting.lock());
        if !waiting.is_empty() {
            tracing::debug!(
                count = waiting.len(),
                "standing down; failing every caller still waiting for an answer"
            );
        }
    }
}

impl ReplyRouter for ClusterNode {
    fn local(&self) -> NodeId {
        self.local
    }

    fn register(&self, deliver: crate::reply::Deliver) -> u128 {
        let n = self.correlations.fetch_add(1, Ordering::Relaxed);
        let correlation = (u128::from(self.local.0) << 64) | u128::from(n);
        let mut waiting = self.waiting.lock();
        waiting.insert(correlation, deliver);
        while waiting.len() > WAITING_CAPACITY {
            // Dropping the entry drops the sender behind it, so the caller's
            // `ask` fails now rather than waiting on an answer this node has
            // just forgotten how to deliver.
            if let Some((evicted, _)) = waiting.pop_first() {
                tracing::warn!(
                    correlation = evicted,
                    "the waiting-caller table is full; failing the longest-waiting request"
                );
            }
        }
        correlation
    }

    fn answer(&self, origin: NodeId, correlation: u128, payload: Vec<u8>) {
        self.route_reply(
            origin,
            Reply {
                correlation,
                payload: Some(payload),
            },
        );
    }

    fn abandon(&self, origin: NodeId, correlation: u128) {
        self.route_reply(
            origin,
            Reply {
                correlation,
                payload: None,
            },
        );
    }

    fn forget(&self, correlation: u128) {
        self.waiting.lock().remove(&correlation);
    }
}

impl ClusterNode {
    /// Get a reply — an answer or the absence of one — back to `origin`.
    fn route_reply(&self, origin: NodeId, reply: Reply) {
        // The caller is on this node: hand it over directly rather than
        // sending a message to ourselves. This is the common case once an
        // instance has been reached locally after all.
        if origin == self.local {
            self.deliver_reply(reply);
            return;
        }
        let transport = self.transport.clone();
        // `send` is async and answering is not, because an actor replies from
        // inside a handler. Spawning is what keeps a slow or unreachable origin
        // from blocking the actor that answered it.
        tokio::spawn(async move {
            if let Err(e) = transport.send(origin, Message::Reply(reply)).await {
                tracing::debug!(error = %e, %origin, "could not return an answer");
            }
        });
    }
}

/// Keep placement and the serving flag in step with consensus, and — while
/// leader — keep the agreed live set in step with reality.
async fn watch_cluster(node: Arc<ClusterNode>, liveness_window: Duration) {
    let mut metrics = node.raft.metrics();
    let mut ticks = tokio::time::interval(LIVENESS_TICK);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            changed = metrics.changed() => {
                if changed.is_err() {
                    return; // raft has shut down
                }
            }
            _ = ticks.tick() => {}
        }

        let current = WatchReceiver::borrow_watched(&metrics).clone();

        // Serving means "a quorum still has me". A follower asks whether it can
        // see a leader; cut off from one it campaigns and never wins, so this
        // stays false for as long as it is in the minority.
        //
        // A leader has to be asked the other way round, and this is the part
        // that is easy to get wrong: a partitioned leader goes on reporting
        // *itself* as the current leader, because nothing has told it
        // otherwise. What it cannot fake is an acknowledgement, so the question
        // is how long since a quorum last answered.
        let serving = match current.state {
            ServerState::Leader => current
                .last_quorum_acked
                .as_ref()
                .is_some_and(|at| at.elapsed() <= liveness_window),
            ServerState::Learner
            | ServerState::Follower
            | ServerState::Candidate
            | ServerState::Shutdown => current.current_leader.is_some(),
        };
        node.serving.store(serving, Ordering::Relaxed);
        let changed = node.serving_tx.send_if_modified(|current| {
            let changed = *current != serving;
            *current = serving;
            changed
        });
        // On the transition only. Clearing on every tick while in a minority
        // would also throw away asks issued after standing down, whose answers
        // can still arrive — inbound replies are delivered whatever this node's
        // serving state.
        if changed && !serving {
            node.abandon_waiting();
        }

        // Placement follows what was *agreed*, not what this node believes.
        let (live, voters) = node.store.live_and_voters();
        let voters: BTreeSet<NodeIdx> = voters.into_iter().collect();
        let usable: Vec<NodeId> = live
            .into_iter()
            .filter(|n| voters.contains(n))
            .map(NodeId)
            .collect();
        node.set_live(&usable);

        if current.state != ServerState::Leader {
            continue;
        }

        // Only the leader observes liveness, and only it proposes. Two
        // observers would disagree, which is the thing being eliminated.
        let observed = observed_live(&current, node.local, liveness_window, &voters);
        let agreed: BTreeSet<NodeIdx> = usable.iter().map(|n| n.0).collect();
        if observed != agreed
            && let Err(e) = node
                .raft
                .client_write(LiveSet {
                    nodes: observed.clone(),
                })
                .await
        {
            // Losing leadership mid-propose is ordinary. The next leader
            // observes for itself.
            tracing::debug!(error = %e, "could not publish the live set");
        }
    }
}

/// Which voters the leader has heard from recently, plus itself.
fn observed_live(
    metrics: &openraft::RaftMetrics<Membership>,
    local: NodeId,
    window: Duration,
    voters: &BTreeSet<NodeIdx>,
) -> BTreeSet<NodeIdx> {
    let mut live = BTreeSet::new();
    // A leader can always reach itself, and it must be in the set or it would
    // hash its own instances onto somebody else.
    live.insert(local.0);
    if let Some(heartbeat) = &metrics.heartbeat {
        for (node, last_ack) in heartbeat {
            if !voters.contains(node) {
                continue;
            }
            // `None` is a peer never yet acknowledged — a node that has not come
            // up, or one that went away before the current leadership began.
            if last_ack.as_ref().is_some_and(|at| at.elapsed() <= window) {
                live.insert(*node);
            }
        }
    }
    live
}
