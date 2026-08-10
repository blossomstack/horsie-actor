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
    ActorContext, ActorRef, ActorSystem, ClusterActor, ClusterConfig, ClusterNode, CommandEffect,
    Envelope, EventSourcedActor, InMemoryJournal, Journal, NodeId, PersistenceId, RaftStore,
    ReplyTo,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------- the actor

struct Counter {
    id: String,
}

#[derive(Serialize, Deserialize)]
enum CounterCmd {
    Inc(i64),
    /// The reply channel does not survive a hop, so a remote `Get` is not
    /// something this test asks for — reads go to the hosting node directly.
    #[serde(skip)]
    Get(Option<ReplyTo<i64>>),
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
            CounterCmd::Inc(n) => CommandEffect::persist(vec![Incremented(n)]),
            CounterCmd::Get(reply) => {
                if let Some(reply) = reply {
                    let _ = reply.send(state.value);
                }
                CommandEffect::none()
            }
        }
    }
}

impl ClusterActor for Counter {
    const KIND: &'static str = "counter";
    type Command = CounterCmd;
    type Deps = ();

    fn spawn(id: &str, _deps: (), system: &ActorSystem) -> ActorRef<CounterCmd> {
        system.spawn_persistent(Counter { id: id.to_owned() })
    }
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
            system.register::<Counter>(());
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
            .position(|n| n.owns("counter", id))
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
    let actor = cluster.system(host).actor_of::<Counter>(id).await.unwrap();
    actor
        .ask(|reply| CounterCmd::Get(Some(reply)))
        .await
        .unwrap()
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

    let remote = cluster
        .system(elsewhere)
        .actor_of::<Counter>(id)
        .await
        .unwrap();
    remote.tell(CounterCmd::Inc(5)).await.unwrap();
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
            .actor_of::<Counter>(id)
            .await
            .unwrap()
            .tell(CounterCmd::Inc(1))
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
        .actor_of::<Counter>(id)
        .await
        .unwrap()
        .tell(CounterCmd::Inc(5))
        .await
        .unwrap();
    settle().await;
    assert_eq!(value_at_host(&cluster, id).await, 5);

    cluster.kill(host).await;

    // Somewhere else picks it up, recovers from the shared journal, and carries
    // on from 5 rather than starting over.
    let new_host = cluster.host_of(id);
    assert_ne!(new_host, host, "the instance stayed on the dead node");
    let revived = cluster
        .system(new_host)
        .actor_of::<Counter>(id)
        .await
        .unwrap();
    revived.tell(CounterCmd::Inc(3)).await.unwrap();
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
            .map(|n| n.owner_of("counter", &id))
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
    let _actor = cluster.system(host).actor_of::<Counter>(id).await.unwrap();

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
    let stale = cluster
        .system(first_host)
        .actor_of::<Counter>(id)
        .await
        .unwrap();
    stale.tell(CounterCmd::Inc(5)).await.unwrap();
    settle().await;

    // Somebody else appends — what a peer that took the instance over does. The
    // stale actor is not told, and has no way to be.
    let seven = serde_json::to_vec(&Incremented(7)).unwrap();
    cluster.journal.persist(&pid, &[seven], 1).await.unwrap();

    stale.tell(CounterCmd::Inc(100)).await.unwrap();
    settle().await;

    // Read through a freshly recovered instance rather than the stale actor's
    // own memory, since the point is what actually reached the journal.
    let elsewhere = (first_host + 1) % 3;
    let fresh = cluster
        .system(elsewhere)
        .spawn_persistent(Counter { id: id.to_owned() });
    let value = fresh
        .ask(|reply| CounterCmd::Get(Some(reply)))
        .await
        .unwrap();
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
    let actor = cluster.system(host).actor_of::<Counter>(id).await.unwrap();
    actor.tell(CounterCmd::Inc(1)).await.unwrap();
    settle().await;

    // Somebody else appends, so this host's next write is from a state that no
    // longer exists.
    let one = serde_json::to_vec(&Incremented(1)).unwrap();
    cluster.journal.persist(&pid, &[one], 1).await.unwrap();

    // The next write is where it finds out — and it must be the last thing it
    // does.
    actor.tell(CounterCmd::Inc(1)).await.unwrap();

    for _ in 0..100 {
        if actor.tell(CounterCmd::Inc(1)).await.is_err() {
            return; // mailbox closed: the host stood down
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("a displaced host kept accepting commands");
}

/// `ask` to an actor on another host fails immediately rather than hanging.
///
/// A reply handle is a channel into the asking process; shipping the command
/// elsewhere leaves it behind, so nobody answers. Until replies can be routed
/// back across a host boundary, refusing is the honest behaviour — a caller can
/// see an error, and cannot see a request that never returns.
#[tokio::test]
async fn ask_to_a_remote_host_is_refused_not_hung() {
    let cluster = TestCluster::of_size(3).await;
    let id = "remote-ask";
    let host = cluster.host_of(id);
    let elsewhere = (host + 1) % 3;

    let remote = cluster
        .system(elsewhere)
        .actor_of::<Counter>(id)
        .await
        .unwrap();

    // Would otherwise wait forever.
    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        remote.ask(|reply| CounterCmd::Get(Some(reply))),
    )
    .await
    .expect("ask must return rather than hang");

    assert!(matches!(
        outcome,
        Err(horsie_actor::TellError::AskNotRoutable)
    ));
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
    let payload = serde_json::to_vec(&CounterCmd::Inc(5)).unwrap();
    let env = Envelope {
        kind: "counter".into(),
        id: id.into(),
        correlation: None,
        message_id: 42,
        payload,
    };
    cluster.system(host).dispatch(env.clone()).await.unwrap();
    cluster.system(host).dispatch(env).await.unwrap();
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

    let remote = cluster
        .system(elsewhere)
        .actor_of::<Counter>(id)
        .await
        .unwrap();
    assert!(
        remote.tell(CounterCmd::Inc(7)).await.is_err(),
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

    let remote = cluster
        .system(elsewhere)
        .actor_of::<Counter>(id)
        .await
        .unwrap();
    remote.tell(CounterCmd::Inc(7)).await.unwrap();
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
        .actor_of::<Counter>("orphan")
        .await;
    assert!(
        matches!(outcome, Err(horsie_actor::ActorOfError::NotServing)),
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
        .find(|id| cluster.nodes[0].owns("counter", id))
        .expect("some id must land on node 0");

    let actor = cluster.system(0).actor_of::<Counter>(&id).await.unwrap();
    actor.tell(CounterCmd::Inc(1)).await.unwrap();
    settle().await;
    assert!(actor.is_alive());

    cluster.net.remove(cluster.nodes[0].local());
    await_not_serving(&cluster, 0).await;

    for _ in 0..200 {
        if !actor.is_alive() {
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

    for _ in 0..400 {
        if cluster.nodes[odd_one_out].serving() {
            // Lazily, on the next message — nothing is respawned at recovery.
            let id = (0..64)
                .map(|i| format!("c{i}"))
                .find(|id| cluster.nodes[odd_one_out].owns("counter", id))
                .expect("some id must land back on this node");
            cluster
                .system(odd_one_out)
                .actor_of::<Counter>(&id)
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
    let _ = cluster
        .system(elsewhere)
        .actor_of::<Counter>("nowhere")
        .await;
    cluster.net.remove(cluster.nodes[0].local());
    let _ = cluster
        .system(elsewhere)
        .actor_of::<Counter>("nowhere")
        .await;
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

async fn await_not_serving(cluster: &TestCluster, index: usize) {
    for _ in 0..400 {
        if !cluster.nodes[index].serving() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("node {index} never stood down");
}
