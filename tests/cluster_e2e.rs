//! Several real nodes in one process: real placement, real transport, real
//! fencing. Nothing here is a stand-in for the thing under test.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::wildcard_enum_match_arm
)]

use async_trait::async_trait;
use horsie_actor::{
    ActorContext, ActorRef, ActorSystem, ClusterActor, ClusterConfig, ClusterNode, CommandEffect,
    Epoch, EventSourcedActor, InMemoryJournal, Journal, NodeId, PersistenceId, ReplyTo,
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

    fn persistence_id(id: &str) -> Option<PersistenceId> {
        Some(PersistenceId::new("counter", id))
    }

    fn spawn(
        id: &str,
        _deps: (),
        system: &ActorSystem,
        fence: Option<Epoch>,
    ) -> ActorRef<CounterCmd> {
        // The fence must reach the journal, or a host that lost this instance
        // keeps writing to it.
        system.spawn_fenced(Counter { id: id.to_owned() }, fence)
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
    fn of_size(n: u64) -> Self {
        let net = horsie_actor::InProcessNetwork::new();
        let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
        let members: Vec<NodeId> = (1..=n).map(NodeId).collect();

        let mut systems = Vec::new();
        let mut nodes = Vec::new();
        for id in &members {
            let node = Arc::new(ClusterNode::new(
                ClusterConfig {
                    local: *id,
                    members: members.clone(),
                },
                Arc::new(net.node(*id)),
            ));
            let system = ActorSystem::clustered(journal.clone(), node.clone());
            system.register::<Counter>(());
            spawn_dispatch_loop(&system, &node);
            systems.push(system);
            nodes.push(node);
        }
        Self {
            net,
            systems,
            nodes,
            journal,
        }
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

    /// Take a node off the network and tell the survivors it is gone.
    fn kill(&self, index: usize) {
        let dead = self.nodes[index].local();
        self.net.remove(dead);
        for node in &self.nodes {
            node.mark_down(dead);
        }
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
    let cluster = TestCluster::of_size(3);
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
    let cluster = TestCluster::of_size(3);
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
    let cluster = TestCluster::of_size(3);
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

    cluster.kill(host);

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

/// A host that claimed a log and then lost it cannot write to it any more.
///
/// This is the fence doing its job end to end: the second host's claim mints a
/// higher epoch, and the first host's writes stop landing even though nothing
/// told it so.
#[tokio::test]
async fn a_former_host_cannot_write_after_someone_else_claims() {
    let cluster = TestCluster::of_size(3);
    let pid = PersistenceId::new("counter", "fenced");

    let first = cluster.journal.claim_ownership(&pid).await.unwrap();
    cluster
        .journal
        .persist(&pid, &[vec![1]], Some(first))
        .await
        .unwrap();

    // A different host takes over.
    let second = cluster.journal.claim_ownership(&pid).await.unwrap();
    assert!(second > first);

    // The old one has not been told, and its next write is where it finds out.
    let err = cluster
        .journal
        .persist(&pid, &[vec![2]], Some(first))
        .await
        .unwrap_err();
    assert!(
        matches!(err, horsie_actor::JournalError::Fenced { .. }),
        "a stale host's write was accepted: {err:?}"
    );

    cluster
        .journal
        .persist(&pid, &[vec![3]], Some(second))
        .await
        .unwrap();
}

/// Every node resolves an instance to the same host, so no two of them race to
/// claim the same log in the first place.
#[tokio::test]
async fn placement_agrees_across_every_node() {
    let cluster = TestCluster::of_size(5);
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

/// Hosting an instance claims its log. Without this the fence exists but is
/// never applied, and two hosts write under no generation at all.
#[tokio::test]
async fn hosting_an_instance_claims_its_log() {
    let cluster = TestCluster::of_size(3);
    let id = "claimed";
    let pid = PersistenceId::new("counter", id);

    assert_eq!(cluster.journal.current_epoch(&pid).await.unwrap(), None);

    let host = cluster.host_of(id);
    let _actor = cluster.system(host).actor_of::<Counter>(id).await.unwrap();

    assert!(
        cluster.journal.current_epoch(&pid).await.unwrap().is_some(),
        "hosting did not claim the log, so its writes carry no generation"
    );
}

/// The end-to-end fence: once a second host claims, the first host's actor
/// stops being able to write, even though nothing told it so.
///
/// This is the failure the whole epoch mechanism exists to prevent — two hosts
/// appending to one history and each believing it succeeded.
#[tokio::test]
async fn a_displaced_host_stops_writing() {
    let cluster = TestCluster::of_size(3);
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

    // Somebody else takes the log — what a partitioned peer taking over does.
    let usurper = cluster.journal.claim_ownership(&pid).await.unwrap();

    // The old host has not been told. Its next write is where it finds out.
    stale.tell(CounterCmd::Inc(100)).await.unwrap();
    settle().await;

    // Read through a freshly recovered instance rather than the stale actor's
    // own memory, since the point is what actually reached the journal.
    let elsewhere = (first_host + 1) % 3;
    let fresh = cluster
        .system(elsewhere)
        .spawn_fenced(Counter { id: id.to_owned() }, Some(usurper));
    let value = fresh
        .ask(|reply| CounterCmd::Get(Some(reply)))
        .await
        .unwrap();
    assert_eq!(
        value, 5,
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
async fn a_fenced_host_stops_instead_of_serving_stale() {
    let cluster = TestCluster::of_size(3);
    let id = "zombie";
    let pid = PersistenceId::new("counter", id);

    let host = cluster.host_of(id);
    let actor = cluster.system(host).actor_of::<Counter>(id).await.unwrap();
    actor.tell(CounterCmd::Inc(1)).await.unwrap();
    settle().await;

    // Somebody else takes the log out from under it.
    cluster.journal.claim_ownership(&pid).await.unwrap();

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
