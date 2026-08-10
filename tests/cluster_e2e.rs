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
    Envelope, EventSourcedActor, InMemoryJournal, Journal, NodeId, PersistenceId, ReplyTo,
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

/// A writer that has fallen behind cannot append, even though nothing told it.
///
/// The fence, at the journal level: two writers recovered at the same point,
/// one of them wrote, and the other's next append is where it finds out. Note
/// there is no claim, no ownership record and no coordination anywhere in this
/// test — being behind is the whole detection mechanism.
#[tokio::test]
async fn a_stale_writer_cannot_append() {
    let cluster = TestCluster::of_size(3);
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

/// Starting an instance writes nothing.
///
/// Hosting used to claim the log first, which meant a round trip before serving
/// and an ownership record to keep. The conditional append made both
/// unnecessary: an instance that never writes leaves no trace, and one that does
/// is checked by the write itself. This also pins the decoupling — a registered
/// actor type with no journal at all is now an ordinary case, not a special one.
#[tokio::test]
async fn starting_an_instance_writes_nothing() {
    let cluster = TestCluster::of_size(3);
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
    let cluster = TestCluster::of_size(3);
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
    let cluster = TestCluster::of_size(3);
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
    let cluster = TestCluster::of_size(3);
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

/// A send to a host that has gone away lands somewhere alive instead of
/// failing, because each attempt resolves the owner afresh.
#[tokio::test]
async fn a_send_retries_onto_a_live_host() {
    let cluster = TestCluster::of_size(3);
    let id = "retry";
    let host = cluster.host_of(id);
    let elsewhere = (host + 1) % 3;

    // Take the host off the network without telling anyone — the sender only
    // finds out by trying.
    cluster.net.remove(cluster.nodes[host].local());

    let remote = cluster
        .system(elsewhere)
        .actor_of::<Counter>(id)
        .await
        .unwrap();
    remote
        .tell(CounterCmd::Inc(7))
        .await
        .expect("the send should have re-resolved onto a live host");
    settle().await;

    assert_eq!(value_at_host(&cluster, id).await, 7);
}
