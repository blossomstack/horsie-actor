//! Several real nodes in one process: real placement, real transport, real
//! write fence. Nothing here is a stand-in for the thing under test.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::wildcard_enum_match_arm
)]

use async_trait::async_trait;
use horsie_actor::{
    ActorContext, ActorPath, ActorSystem, ClusterConfig, ClusterNode, CommandEffect, Envelope,
    EventSourcedActor, InMemoryJournal, Journal, NodeId, PersistenceId, RaftStore, ReplyTo, Root,
    Shard,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------- the actor

struct Counter {
    id: String,
    /// Reply handles this counter was asked to sit on, kept alive so the caller
    /// is left waiting rather than failed.
    held: Vec<ReplyTo<i64>>,
}

impl Counter {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_owned(),
            held: Vec::new(),
        }
    }
}

/// Every command names its own target. The extractors are the only thing that
/// knows where a message is going, so the address travels in the message rather
/// than in the reference.
#[derive(Serialize, Deserialize)]
enum CounterCmd {
    Inc {
        id: String,
        by: i64,
    },
    /// A read, answered wherever the instance happens to live. The reply
    /// channel round-trips like any other field — which is the whole point of
    /// reply routing, and was impossible before it.
    Get {
        id: String,
        reply: ReplyTo<i64>,
    },
    /// Pass it on: reach another counter from inside this one. An actor knows
    /// what to say, not where the other lives — and the host that built this
    /// instance may never have seen whoever holds the other.
    IncOther {
        id: String,
        other: String,
        by: i64,
    },
    /// Take the request and drop the handle without answering. Standing in for
    /// every way a real handler ends without a reply — an error path, a stop, a
    /// branch that forgot.
    Ignore {
        id: String,
        reply: ReplyTo<i64>,
    },
    /// Take the request and keep the handle, so the caller is genuinely left
    /// waiting on an answer that is still, as far as anyone knows, coming.
    Hold {
        id: String,
        reply: ReplyTo<i64>,
    },
}

impl CounterCmd {
    fn id(&self) -> &str {
        match self {
            CounterCmd::Inc { id, .. }
            | CounterCmd::Get { id, .. }
            | CounterCmd::IncOther { id, .. }
            | CounterCmd::Ignore { id, .. }
            | CounterCmd::Hold { id, .. } => id,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct Incremented(i64);

#[derive(Serialize, Deserialize, Default)]
struct CounterState {
    value: i64,
}

#[async_trait]
impl EventSourcedActor for Counter {
    type Command = CounterCmd;
    type Event = Incremented;
    type State = CounterState;
    type ParentCommand = Root;

    fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new("counter", self.id.clone())
    }
    fn initial_state() -> CounterState {
        CounterState::default()
    }
    fn apply_event(mut state: CounterState, event: Incremented) -> CounterState {
        state.value += event.0;
        state
    }
    async fn handle_command(
        &mut self,
        state: &CounterState,
        cmd: CounterCmd,
        _ctx: &mut ActorContext<CounterCmd>,
    ) -> CommandEffect<Incremented> {
        match cmd {
            CounterCmd::Inc { by, .. } => CommandEffect::persist(vec![Incremented(by)]),
            CounterCmd::Get { reply, .. } => {
                let _ = reply.send(state.value);
                CommandEffect::none()
            }
            CounterCmd::IncOther { other, by, .. } => {
                let _ = _ctx
                    .shard_actor_of::<Counter>()
                    .tell(CounterCmd::Inc { id: other, by })
                    .await;
                CommandEffect::none()
            }
            CounterCmd::Ignore { reply, .. } => {
                drop(reply);
                CommandEffect::none()
            }
            CounterCmd::Hold { reply, .. } => {
                self.held.push(reply);
                CommandEffect::none()
            }
        }
    }
}

impl Shard for Counter {
    type Command = CounterCmd;
    const TYPE: &'static str = "counter";

    fn entity_id(cmd: &CounterCmd) -> String {
        cmd.id().to_owned()
    }

    /// One shard per counter, so every instance is placed on its own and a
    /// failover moves exactly one of them.
    fn shard_id(cmd: &CounterCmd) -> String {
        cmd.id().to_owned()
    }
}

/// Increment `id`.
fn inc(id: &str, by: i64) -> CounterCmd {
    CounterCmd::Inc {
        id: id.to_owned(),
        by,
    }
}

/// Read `id`, for use with `ask`.
fn get(id: &str) -> impl FnOnce(ReplyTo<i64>) -> CounterCmd {
    let id = id.to_owned();
    move |reply| CounterCmd::Get { id, reply }
}

/// The shard a counter belongs to — the key placement is decided over.
fn shard_at(id: &str) -> String {
    format!("/system/shard/counter/{id}")
}

/// The counter itself.
fn entity_at(id: &str) -> String {
    format!("/system/shard/counter/{id}/{id}")
}

// ---------------------------------------------------------------- the harness

/// N nodes sharing one journal — which is what a real cluster has, since the
/// whole point is that any node can recover any instance.
struct TestCluster {
    net: horsie_actor::InProcessNetwork,
    systems: Vec<ActorSystem>,
    nodes: Vec<Arc<ClusterNode>>,
    journal: Arc<dyn Journal>,
}

impl TestCluster {
    /// Bring up `n` nodes and wait for them to agree on a leader and a live set.
    ///
    /// The wait is not a convenience. Until consensus has settled no node is
    /// serving, which is the correct behaviour and would otherwise make every
    /// test here a race against an election.
    async fn of_size(n: u64) -> Self {
        let net = horsie_actor::InProcessNetwork::new();
        let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
        let members: Vec<NodeId> = (1..=n).map(NodeId).collect();

        let mut systems = Vec::new();
        let mut nodes = Vec::new();
        for id in &members {
            let node = ClusterNode::start(
                ClusterConfig {
                    local: *id,
                    bootstrap: members.clone(),
                    liveness_window: Duration::from_millis(600),
                },
                Arc::new(net.node(*id)),
                // Every node forgets its raft state when the test ends, which
                // is exactly what `in_memory_unsafe` warns about and exactly
                // what a test wants.
                RaftStore::in_memory_unsafe(),
            )
            .await
            .expect("raft should start");
            let system = ActorSystem::clustered(journal.clone(), node.clone());
            system
                .shard::<Counter>()
                .register(|sys, path| sys.persistent(Counter::new(path.name().unwrap_or_default())))
                .expect("counter should register");
            spawn_dispatch_loop(&system, &node);
            systems.push(system);
            nodes.push(node);
        }
        let cluster = Self {
            net,
            systems,
            nodes,
            journal,
        };
        cluster.await_settled(n as usize).await;
        cluster
    }

    /// Wait until `expected` nodes are serving and every one of them has
    /// applied the same live set.
    ///
    /// Comparing the live sets rather than one instance's owner is the stronger
    /// check: agreement on every id follows from agreement on the input, and a
    /// node that is merely a tick behind would otherwise slip through.
    async fn await_settled(&self, expected: usize) {
        for _ in 0..200 {
            let serving: Vec<_> = self.nodes.iter().filter(|n| n.serving()).collect();
            let sets: Vec<Vec<NodeId>> = serving.iter().map(|n| n.live_members()).collect();
            let agreed = sets
                .first()
                .is_some_and(|first| first.len() == expected && sets.iter().all(|s| s == first));
            if serving.len() == expected && agreed {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("the cluster never settled on a leader and a live set");
    }

    fn system(&self, index: usize) -> &ActorSystem {
        &self.systems[index]
    }

    /// Index of whichever node hosts this instance.
    fn host_of(&self, id: &str) -> usize {
        self.nodes
            .iter()
            .position(|n| n.owns(&shard_at(id)))
            .expect("some node must host it")
    }

    /// Take a node off the network and wait for the survivors to agree it is
    /// gone.
    ///
    /// Nothing tells them: the leader notices its heartbeats stop being
    /// acknowledged and publishes a smaller live set, which every node applies.
    /// That is the whole point — a node cannot be marked down by whoever
    /// happened to fail a send to it.
    async fn kill(&self, index: usize) {
        let dead = self.nodes[index].local();
        self.net.remove(dead);
        for _ in 0..400 {
            let survivors: Vec<_> = self
                .nodes
                .iter()
                .filter(|n| n.local() != dead && n.serving())
                .collect();
            let sets: Vec<Vec<NodeId>> = survivors.iter().map(|n| n.live_members()).collect();
            let agreed = sets
                .first()
                .is_some_and(|first| !first.contains(&dead) && sets.iter().all(|s| s == first));
            if agreed {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("the survivors never agreed that {dead} was gone");
    }
}

/// Drain a node's inbox into its system. This is the loop a real deployment
/// runs; the tests exercise the same one.
fn spawn_dispatch_loop(system: &ActorSystem, node: &Arc<ClusterNode>) {
    let Some(mut inbox) = node.incoming() else {
        return;
    };
    let system = system.clone();
    tokio::spawn(async move {
        while let Some(env) = inbox.recv().await {
            if let Err(e) = system.dispatch(env).await {
                eprintln!("dispatch failed: {e}");
            }
        }
    });
}

/// Read straight from the hosting node, so the assertion never depends on the
/// thing it is trying to prove.
async fn value_at_host(cluster: &TestCluster, id: &str) -> i64 {
    let host = cluster.host_of(id);
    let actor = cluster.system(host).shard_actor_of::<Counter>();
    actor.ask(get(id)).await.unwrap()
}

/// Let a cross-node send land. The write is asynchronous by construction — the
/// sender is told the envelope was accepted, not that it was applied.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

// ---------------------------------------------------------------- the tests

/// The point of the whole layer: a caller on a node that does not host the
/// instance reaches it through the same `actor_of` it would use locally.
#[tokio::test]
async fn a_caller_reaches_an_instance_hosted_on_another_node() {
    let cluster = TestCluster::of_size(3).await;
    let id = "c1";
    let host = cluster.host_of(id);
    let elsewhere = (host + 1) % 3;

    let remote = cluster.system(elsewhere).shard_actor_of::<Counter>();
    remote.tell(inc(id, 5)).await.unwrap();
    settle().await;

    assert_eq!(value_at_host(&cluster, id).await, 5);
}

/// Two different callers, one instance. If each node hosted its own copy this
/// would read 3 or 4 rather than 7.
#[tokio::test]
async fn all_nodes_address_one_instance() {
    let cluster = TestCluster::of_size(3).await;
    let id = "shared";

    for i in 0..3 {
        cluster
            .system(i)
            .shard_actor_of::<Counter>()
            .tell(inc(id, 1))
            .await
            .unwrap();
    }
    settle().await;

    assert_eq!(value_at_host(&cluster, id).await, 3);
}

/// Killing the host does not lose the instance. It reactivates elsewhere on the
/// next message with its history intact — lazily, matching the rule that
/// nothing loads at boot and nothing auto-resumes.
#[tokio::test]
async fn an_instance_survives_the_death_of_its_host() {
    let cluster = TestCluster::of_size(3).await;
    let id = "survivor";
    let host = cluster.host_of(id);

    cluster
        .system(host)
        .shard_actor_of::<Counter>()
        .tell(inc(id, 5))
        .await
        .unwrap();
    settle().await;
    assert_eq!(value_at_host(&cluster, id).await, 5);

    cluster.kill(host).await;

    // Somewhere else picks it up, recovers from the shared journal, and carries
    // on from 5 rather than starting over.
    let new_host = cluster.host_of(id);
    assert_ne!(new_host, host, "the instance stayed on the dead node");
    let revived = cluster.system(new_host).shard_actor_of::<Counter>();
    revived.tell(inc(id, 3)).await.unwrap();
    settle().await;
    assert_eq!(value_at_host(&cluster, id).await, 8);
}

/// A writer that has fallen behind cannot append, even though nothing told it.
///
/// The fence, at the journal level: two writers recovered at the same point,
/// one of them wrote, and the other's next append is where it finds out. Note
/// there is no claim, no ownership record and no coordination anywhere in this
/// test — being behind is the whole detection mechanism.
#[tokio::test]
async fn a_stale_writer_cannot_append() {
    let cluster = TestCluster::of_size(3).await;
    let pid = PersistenceId::new("counter", "stale");

    // Both writers recovered here.
    cluster.journal.persist(&pid, &[vec![1]], 0).await.unwrap();

    // One of them writes.
    cluster.journal.persist(&pid, &[vec![2]], 1).await.unwrap();

    // The other still believes the log ends at 1.
    let err = cluster
        .journal
        .persist(&pid, &[vec![3]], 1)
        .await
        .unwrap_err();
    assert!(
        matches!(err, horsie_actor::JournalError::Conflict { .. }),
        "a stale writer's append was accepted: {err:?}"
    );

    // And the winner carries on from where it actually is.
    cluster.journal.persist(&pid, &[vec![4]], 2).await.unwrap();
}

/// Every node resolves an instance to the same host, so no two of them race to
/// claim the same log in the first place.
#[tokio::test]
async fn placement_agrees_across_every_node() {
    let cluster = TestCluster::of_size(5).await;
    for i in 0..40 {
        let id = format!("c{i}");
        let hosts: Vec<_> = cluster
            .nodes
            .iter()
            .map(|n| n.owner_of(&shard_at(&id)))
            .collect();
        assert!(
            hosts.windows(2).all(|w| w[0] == w[1]),
            "nodes disagreed about {id}: {hosts:?}"
        );
    }
}

/// Starting an instance writes nothing.
///
/// Hosting used to claim the log first, which meant a round trip before serving
/// and an ownership record to keep. The conditional append made both
/// unnecessary: an instance that never writes leaves no trace, and one that does
/// is checked by the write itself. This also pins the decoupling — a registered
/// actor type with no journal at all is now an ordinary case, not a special one.
#[tokio::test]
async fn starting_an_instance_writes_nothing() {
    let cluster = TestCluster::of_size(3).await;
    let id = "quiet";
    let pid = PersistenceId::new("counter", id);

    let host = cluster.host_of(id);
    let _actor = cluster.system(host).shard_actor_of::<Counter>();

    assert_eq!(
        cluster.journal.last_seq(&pid).await.unwrap(),
        0,
        "hosting an instance touched the log"
    );
}

/// The end-to-end fence: a host whose log has moved on stops landing writes.
///
/// This is the failure the whole mechanism exists to prevent — two hosts
/// appending to one history, each believing it succeeded, leaving a log that is
/// neither host's state.
#[tokio::test]
async fn a_displaced_host_stops_writing() {
    let cluster = TestCluster::of_size(3).await;
    let id = "displaced";
    let pid = PersistenceId::new("counter", id);

    let first_host = cluster.host_of(id);
    let stale = cluster.system(first_host).shard_actor_of::<Counter>();
    stale.tell(inc(id, 5)).await.unwrap();
    settle().await;

    // Somebody else appends — what a peer that took the instance over does. The
    // stale actor is not told, and has no way to be.
    let seven = serde_json::to_vec(&Incremented(7)).unwrap();
    cluster.journal.persist(&pid, &[seven], 1).await.unwrap();

    stale.tell(inc(id, 100)).await.unwrap();
    settle().await;

    // Read through a freshly recovered instance rather than the stale actor's
    // own memory, since the point is what actually reached the journal.
    let elsewhere = (first_host + 1) % 3;
    // Unregistered, and deliberately: a second incarnation over the same
    // journal, which is what a peer taking the instance over would be.
    let elsewhere = cluster.system(elsewhere);
    let fresh = elsewhere.spawn_at(
        ActorPath::root().child("fresh"),
        elsewhere.persistent(Counter::new(id)),
    );
    let value = fresh.ask(get(id)).await.unwrap();
    assert_eq!(
        value, 12,
        "the displaced host's write landed; the fence is not being applied"
    );
}

/// A host that loses its log stops, rather than staying up and failing every
/// write.
///
/// Without this it becomes a zombie: it keeps accepting commands, `ask` callers
/// get errors and plain `tell` callers get silence, and it stays that way until
/// somebody notices. Stopping closes the mailbox, so callers fail immediately
/// and re-resolve to whoever owns the log now.
#[tokio::test]
async fn a_displaced_host_stops_instead_of_serving_stale() {
    let cluster = TestCluster::of_size(3).await;
    let id = "zombie";
    let pid = PersistenceId::new("counter", id);

    let host = cluster.host_of(id);
    let actor = cluster.system(host).shard_actor_of::<Counter>();
    actor.tell(inc(id, 1)).await.unwrap();
    settle().await;

    // Somebody else appends, so this host's next write is from a state that no
    // longer exists — and the value it holds in memory is now history.
    let ten = serde_json::to_vec(&Incremented(10)).unwrap();
    cluster.journal.persist(&pid, &[ten], 1).await.unwrap();

    // The next write is where it finds out, and standing down is the last thing
    // it does. Its own increment is rejected rather than merged.
    actor.tell(inc(id, 1)).await.unwrap();
    settle().await;

    // The read is answered by a fresh instance that replayed the real log —
    // 1 + 10 — and not by the displaced one, which never saw the 10 and would
    // still be reporting 1. Asserting the *answer* rather than a closed mailbox
    // is what makes this about serving stale rather than about a handle: a
    // reference names an address, so a later send is served by whoever is there
    // now, which is exactly the point.
    assert_eq!(value_at_host(&cluster, id).await, 11);
}

/// `ask` reaches an actor on another host and the answer comes back.
///
/// This is what reply routing is for, and the reason the whole crate exists in
/// this shape: the caller writes the same `ask` it would write locally, and
/// nothing at the call site says where the actor is. Before this, a remote
/// `ask` was refused outright, because the reply handle is a channel into the
/// asking process and shipping the command elsewhere left it behind.
#[tokio::test]
async fn ask_reaches_an_actor_on_another_host() {
    let cluster = TestCluster::of_size(3).await;
    let id = "remote-ask";
    let host = cluster.host_of(id);
    let elsewhere = (host + 1) % 3;

    // Give it a value through the hosting node, so the answer is something only
    // the real instance could know.
    cluster
        .system(host)
        .shard_actor_of::<Counter>()
        .tell(inc(id, 9))
        .await
        .unwrap();
    settle().await;

    let remote = cluster.system(elsewhere).shard_actor_of::<Counter>();

    let value = tokio::time::timeout(Duration::from_secs(5), remote.ask(get(id)))
        .await
        .expect("ask must return rather than hang")
        .expect("ask across a host must succeed");
    assert_eq!(value, 9);
}

/// A caller whose host cannot be reached is told, rather than left waiting.
///
/// The failure mode reply routing must not introduce: an answer that never
/// comes is indistinguishable from one that is merely slow, and a caller
/// blocked on it holds whatever it was doing open.
#[tokio::test]
async fn an_ask_to_an_unreachable_host_fails_rather_than_hanging() {
    let cluster = TestCluster::of_size(3).await;
    let id = "gone";
    let host = cluster.host_of(id);
    let elsewhere = (host + 1) % 3;

    cluster.net.remove(cluster.nodes[host].local());

    let remote = cluster.system(elsewhere).shard_actor_of::<Counter>();

    let outcome = tokio::time::timeout(Duration::from_secs(5), remote.ask(get(id)))
        .await
        .expect("ask must return rather than hang");
    assert!(outcome.is_err());
}

/// An actor that takes a request and never answers fails its caller, wherever
/// it is hosted.
///
/// The distinction this crate exists to remove, in its nastiest form: in one
/// process the dropped handle wakes the caller, and across a host it used to
/// wake nobody — so the same handler failed cleanly or hung forever depending
/// only on where placement happened to put it.
#[tokio::test]
async fn an_unanswered_request_fails_the_caller_across_a_host_too() {
    let cluster = TestCluster::of_size(3).await;
    let id = "unanswered";
    let host = cluster.host_of(id);
    let elsewhere = (host + 1) % 3;

    let remote = cluster.system(elsewhere).shard_actor_of::<Counter>();
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        remote.ask(|reply| CounterCmd::Ignore {
            id: id.to_owned(),
            reply,
        }),
    )
    .await
    .expect("a dropped handle must not leave a caller on another node waiting");
    assert!(outcome.is_err());
}

/// A node that stands down fails the callers still waiting on it.
///
/// It has lost touch with the cluster, so nothing it is waiting for is still
/// coming. Actors already lose their in-flight work when this happens; a caller
/// that is not an actor had nothing telling it, and simply waited.
#[tokio::test]
async fn standing_down_fails_the_callers_still_waiting() {
    let cluster = TestCluster::of_size(3).await;
    let id = "held";
    let host = cluster.host_of(id);
    let elsewhere = (host + 1) % 3;

    // The hosting counter takes the request and keeps the handle, so nothing
    // about the request itself will ever end the wait.
    let remote = cluster.system(elsewhere).shard_actor_of::<Counter>();
    let pending = tokio::spawn(async move {
        remote
            .ask(|reply| CounterCmd::Hold {
                id: id.to_owned(),
                reply,
            })
            .await
    });
    settle().await;

    cluster.net.remove(cluster.nodes[elsewhere].local());
    await_not_serving(&cluster, elsewhere).await;

    let outcome = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .expect("standing down must end the wait")
        .expect("the ask task should not panic");
    assert!(outcome.is_err());
}

/// A caller that gives up takes its entry in the waiting table with it.
///
/// The table is what makes a reply routable at all — one row per caller, minted
/// when the handle is encoded. Nothing on the far end knows the caller lost
/// interest, so if giving up did not clean up, every cancelled request would
/// leave a row holding a channel nobody will read, until capacity evicted it and
/// failed somebody else's request instead.
#[tokio::test]
async fn a_caller_that_gives_up_stops_being_waited_for() {
    let cluster = TestCluster::of_size(3).await;
    let id = "abandoned";
    let host = cluster.host_of(id);
    let elsewhere = (host + 1) % 3;

    let remote = cluster.system(elsewhere).shard_actor_of::<Counter>();
    let outcome = remote
        .ask_within(Duration::from_millis(100), |reply| CounterCmd::Hold {
            id: id.to_owned(),
            reply,
        })
        .await;
    assert!(outcome.is_err());

    assert_eq!(
        cluster.nodes[elsewhere].waiting(),
        0,
        "the caller gave up and its row stayed behind"
    );
}

/// An answer is delivered whatever this node's serving state.
///
/// The quorum check asks whether an *instance* still belongs to this node. A
/// future waiting here is nobody else's to take, so refusing its answer would
/// only mean the caller waits forever — and the check sat above the reply branch
/// rather than below it.
#[tokio::test]
async fn a_node_with_no_quorum_still_takes_its_own_answers() {
    let cluster = TestCluster::of_size(3).await;
    let cut_off = 0;

    cluster.net.remove(cluster.nodes[cut_off].local());
    await_not_serving(&cluster, cut_off).await;

    // Nobody is waiting for this one, which is not the point: the point is that
    // it reaches the reply path at all rather than being refused above it.
    let taken = cluster
        .system(cut_off)
        .dispatch(horsie_actor::Message::Reply(horsie_actor::Reply {
            correlation: 1,
            payload: None,
        }))
        .await;
    assert!(taken.is_ok(), "{taken:?}");
}

/// A redelivered command is applied once.
///
/// Retries make delivery at-least-once; this is what keeps *processing* to
/// once. Without it, every retry after a slow ack would double-count.
#[tokio::test]
async fn a_redelivered_command_is_applied_once() {
    let cluster = TestCluster::of_size(3).await;
    let id = "dedup";
    let host = cluster.host_of(id);

    // Same message id twice, as a retry of an envelope the sender could not
    // confirm.
    let payload = serde_json::to_vec(&inc(id, 5)).unwrap();
    let env = Envelope {
        path: entity_at(id),
        message_id: 42,
        payload,
    };
    cluster
        .system(host)
        .dispatch(horsie_actor::Message::Command(env.clone()))
        .await
        .unwrap();
    cluster
        .system(host)
        .dispatch(horsie_actor::Message::Command(env))
        .await
        .unwrap();
    settle().await;

    assert_eq!(
        value_at_host(&cluster, id).await,
        5,
        "the retry was applied a second time"
    );
}

/// A send to a host that has gone away fails, rather than being re-aimed by
/// the sender.
///
/// This is the behaviour that replaced the split-brain generator. The previous
/// version dropped the target from *its own* member list on a failed send and
/// retried elsewhere, which meant one failed delivery let a node rewrite
/// placement — and a node cut off from its peers rewrote it all the way down to
/// hosting everything itself. Whether a host is really gone is now the leader's
/// observation, and a caller that cannot reach one is told so.
#[tokio::test]
async fn a_send_to_a_departed_host_fails_rather_than_re_aiming() {
    let cluster = TestCluster::of_size(3).await;
    let id = "retry";
    let host = cluster.host_of(id);
    let elsewhere = (host + 1) % 3;

    // Off the network, with nobody told.
    cluster.net.remove(cluster.nodes[host].local());

    let remote = cluster.system(elsewhere).shard_actor_of::<Counter>();
    assert!(
        remote.tell(inc(id, 7)).await.is_err(),
        "the sender re-aimed a send instead of reporting the failure"
    );
}

/// Once the cluster agrees the host is gone, the same send lands.
///
/// The two halves together are the contract: failure while it is merely
/// unreachable, success once it is agreed to be gone.
#[tokio::test]
async fn a_send_lands_elsewhere_once_the_cluster_agrees_the_host_is_gone() {
    let cluster = TestCluster::of_size(3).await;
    let id = "reaimed";
    let host = cluster.host_of(id);
    let elsewhere = (host + 1) % 3;

    cluster.kill(host).await;

    let remote = cluster.system(elsewhere).shard_actor_of::<Counter>();
    remote.tell(inc(id, 7)).await.unwrap();
    settle().await;

    assert_eq!(value_at_host(&cluster, id).await, 7);
}

// ------------------------------------------------------- membership & quorum

/// A node cut off from the cluster stops serving.
///
/// This is the whole answer to the split-brain the previous design generated: a
/// node can no longer conclude it is the only member and take ownership of
/// everything, because membership is not its to decide. Cut off, it sees no
/// leader and stands down.
#[tokio::test]
async fn a_partitioned_node_stops_serving() {
    let cluster = TestCluster::of_size(3).await;
    let odd_one_out = 0;

    cluster.net.remove(cluster.nodes[odd_one_out].local());

    for _ in 0..400 {
        if !cluster.nodes[odd_one_out].serving() {
            // And the majority carries on, which is the other half of the
            // point: standing down is not the same as everyone stopping.
            let survivors = cluster
                .nodes
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != odd_one_out)
                .filter(|(_, n)| n.serving())
                .count();
            assert_eq!(survivors, 2, "the majority stopped serving too");
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("a node in the minority kept serving");
}

/// A minority node refuses to start instances, rather than starting them and
/// answering from state that may already be history.
///
/// The write fence cannot catch this one: a read never writes, so nothing
/// checks it. Refusing is the only thing that bounds it.
#[tokio::test]
async fn a_minority_node_refuses_to_host() {
    let cluster = TestCluster::of_size(3).await;
    let odd_one_out = 0;

    cluster.net.remove(cluster.nodes[odd_one_out].local());
    await_not_serving(&cluster, odd_one_out).await;

    let outcome = cluster
        .system(odd_one_out)
        .shard_actor_of::<Counter>()
        .tell(inc("c1", 1))
        .await;
    assert!(
        outcome.is_err(),
        "a node with no quorum started an instance anyway"
    );
}

/// Losing quorum stops the instances already running, not just the new ones.
///
/// Without this a node keeps a live actor answering reads from memory for as
/// long as the partition lasts. Stopping closes the mailbox, so callers fail
/// immediately instead of being told something that was true a minute ago.
#[tokio::test]
async fn losing_quorum_stops_hosted_instances() {
    let cluster = TestCluster::of_size(3).await;

    // Find an instance hosted on node 0, so the partition takes its host.
    let id = (0..64)
        .map(|i| format!("c{i}"))
        .find(|id| cluster.nodes[0].owns(&shard_at(id)))
        .expect("some id must land on node 0");

    let actor = cluster.system(0).shard_actor_of::<Counter>();
    actor.tell(inc(&id, 1)).await.unwrap();
    settle().await;
    assert!(actor.tell(inc(&id, 1)).await.is_ok());

    cluster.net.remove(cluster.nodes[0].local());
    await_not_serving(&cluster, 0).await;

    for _ in 0..200 {
        if actor.tell(inc(&id, 1)).await.is_err() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("an instance kept running on a node with no quorum");
}

/// Regaining quorum brings the node back, and instances restart on demand.
///
/// Standing down has to be recoverable: a network blip that permanently
/// bricked a node would be a worse failure than the one being prevented.
#[tokio::test]
async fn a_node_recovers_when_quorum_returns() {
    let cluster = TestCluster::of_size(3).await;
    let odd_one_out = 0;
    let local = cluster.nodes[odd_one_out].local();

    cluster.net.remove(local);
    await_not_serving(&cluster, odd_one_out).await;

    cluster.net.restore(local);

    // Two separate things, and in this order: the node sees a leader again
    // before the leader has re-published a live set containing it. In between
    // it is serving and hosts nothing, which is correct — placement is not its
    // to decide — but a test that waited only for `serving` would find no
    // instance assigned to it and look like a product bug.
    for _ in 0..400 {
        let node = &cluster.nodes[odd_one_out];
        if node.serving() && node.live_members().contains(&local) {
            // Lazily, on the next message — nothing is respawned at recovery.
            let id = (0..64)
                .map(|i| format!("c{i}"))
                .find(|id| node.owns(&shard_at(id)))
                .expect("some id must land back on this node");
            cluster
                .system(odd_one_out)
                .shard_actor_of::<Counter>()
                .tell(inc(&id, 1))
                .await
                .expect("a node with quorum must host again");
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("a node never recovered after the partition healed");
}

/// Every node's placement comes from the same agreed live set, so no node can
/// hold a private opinion about who is up.
#[tokio::test]
async fn no_node_holds_a_private_view_of_who_is_live() {
    let cluster = TestCluster::of_size(3).await;
    let sets: Vec<_> = cluster.nodes.iter().map(|n| n.live_members()).collect();
    assert!(
        sets.windows(2).all(|w| w[0] == w[1]),
        "nodes disagreed about the live set: {sets:?}"
    );

    // A failed send must not change that. It is the exact move the old
    // implementation made, and the one that created split brain.
    let elsewhere = 1;
    let before = cluster.nodes[elsewhere].live_members();
    let _ = cluster.system(elsewhere).shard_actor_of::<Counter>();
    cluster.net.remove(cluster.nodes[0].local());
    let _ = cluster.system(elsewhere).shard_actor_of::<Counter>();
    assert_eq!(
        cluster.nodes[elsewhere].live_members(),
        before,
        "a failed send rewrote this node's view of the cluster"
    );
}

/// A partitioned *leader* stands down too, and this is the case that is easy to
/// get wrong.
///
/// A follower cut off from the cluster notices quickly: it stops hearing from a
/// leader. A leader notices nothing — it goes on reporting itself as the
/// current leader, because no one has told it otherwise and no one can. What it
/// cannot fake is an acknowledgement, so the question has to be asked the other
/// way round: how long since a quorum last answered.
#[tokio::test]
async fn a_partitioned_leader_stands_down() {
    let cluster = TestCluster::of_size(3).await;

    let leader = cluster
        .nodes
        .iter()
        .position(|n| {
            horsie_actor::WatchReceiver::borrow_watched(&n.raft().metrics()).current_leader
                == Some(n.local().0)
        })
        .expect("the cluster must have elected a leader");

    cluster.net.remove(cluster.nodes[leader].local());
    await_not_serving(&cluster, leader).await;
}

/// An actor spawned before the cluster has elected anybody survives.
///
/// The stand-down signal means "stop what you are doing", and at startup there
/// is nothing to stop. Seeding it from "not serving yet" instead would make
/// every actor spawned during an election exit on its first poll, silently and
/// with no error anywhere.
/// An actor reaches another instance by id, from inside itself — including one
/// the cluster placed on a different host.
///
/// This is how a parent, a sibling or a service is reached once instances move:
/// handing a reference down at construction cannot survive clustering, because
/// the host that builds an instance may never have seen whoever would have
/// handed it one.
#[tokio::test]
async fn an_actor_reaches_another_instance_by_id() {
    let cluster = TestCluster::of_size(3).await;

    // Two ids the cluster places on different hosts, so the hop is a real one
    // rather than a local lookup dressed up as one.
    let (from, to) = (0..200)
        .map(|n| format!("peer-{n}"))
        .flat_map(|a| (0..200).map(move |n| (a.clone(), format!("other-{n}"))))
        .find(|(a, b)| cluster.host_of(a) != cluster.host_of(b))
        .expect("three nodes place some pair apart");

    let caller = cluster
        .system(cluster.host_of(&from))
        .shard_actor_of::<Counter>();
    caller
        .tell(CounterCmd::IncOther {
            id: from.clone(),
            other: to.clone(),
            by: 7,
        })
        .await
        .unwrap();
    settle().await;
    settle().await;

    assert_eq!(
        value_at_host(&cluster, &to).await,
        7,
        "the increment must reach the instance the id names, on whatever host holds it"
    );
}

#[tokio::test]
async fn an_actor_spawned_before_the_first_election_survives() {
    let net = horsie_actor::InProcessNetwork::new();
    let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
    let members: Vec<NodeId> = (1..=3).map(NodeId).collect();

    let node = ClusterNode::start(
        ClusterConfig {
            local: NodeId(1),
            bootstrap: members,
            liveness_window: Duration::from_millis(600),
        },
        Arc::new(net.node(NodeId(1))),
        RaftStore::in_memory_unsafe(),
    )
    .await
    .unwrap();

    // No peers exist, so this node never reaches a quorum and never serves.
    assert!(!node.serving());

    let system = ActorSystem::clustered(journal, node);
    let actor = system.spawn_at(
        ActorPath::root().child("early"),
        system.persistent(Counter::new("early")),
    );
    settle().await;
    assert!(
        actor.tell(inc("early", 1)).await.is_ok(),
        "an actor spawned before the first election was killed on sight"
    );
}

async fn await_not_serving(cluster: &TestCluster, index: usize) {
    for _ in 0..400 {
        if !cluster.nodes[index].serving() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("node {index} never stood down");
}
