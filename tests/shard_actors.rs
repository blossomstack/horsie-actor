//! Shard actors across real nodes: real placement, real transport, real
//! recipes.
//!
//! An actor tree is node-local and clustering happens at its roots. What that
//! buys is here: one actor per address across the whole cluster, reachable from
//! any node, rebuilt by whoever owns it — with the caller doing nothing and
//! knowing nothing.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::wildcard_enum_match_arm
)]

use async_trait::async_trait;
use horsie_actor::{
    Actor, ActorContext, ActorSystem, ClusterConfig, ClusterNode, Flow, InMemoryJournal, Journal,
    NodeId, RaftStore, ReplyTo, Shard,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// A session, sharded by the account it belongs to — so every session of one
/// account is placed together, without any of them being a child of anything.
struct Session {
    /// Where it was built. Answering with this is how a test tells one instance
    /// from another, and one *address* from another.
    address: String,
}

#[derive(Serialize, Deserialize)]
enum SessionCmd {
    Which {
        account: String,
        session: String,
        reply: ReplyTo<String>,
    },
}

impl SessionCmd {
    fn account(&self) -> &str {
        match self {
            SessionCmd::Which { account, .. } => account,
        }
    }
    fn session(&self) -> &str {
        match self {
            SessionCmd::Which { session, .. } => session,
        }
    }
}

#[async_trait]
impl Actor for Session {
    type Command = SessionCmd;

    async fn handle(&mut self, cmd: SessionCmd, _ctx: &mut ActorContext<SessionCmd>) -> Flow {
        match cmd {
            SessionCmd::Which { reply, .. } => {
                let _ = reply.send(self.address.clone());
                Flow::Continue
            }
        }
    }
}

impl Shard for Session {
    type Command = SessionCmd;
    type EntityId = String;
    const TYPE: &'static str = "session";

    fn entity_id(cmd: &SessionCmd) -> String {
        cmd.session().to_owned()
    }

    /// Sharded by account: this one function is the whole placement policy.
    fn shard_id(cmd: &SessionCmd) -> String {
        cmd.account().to_owned()
    }
}

fn which(account: &str, session: &str) -> impl FnOnce(ReplyTo<String>) -> SessionCmd {
    let (account, session) = (account.to_owned(), session.to_owned());
    move |reply| SessionCmd::Which {
        account,
        session,
        reply,
    }
}

/// A type nobody registers, to prove an unclaimed address is a clean failure.
struct Stranger;

#[derive(Serialize, Deserialize)]
struct Knock;

#[async_trait]
impl Actor for Stranger {
    type Command = Knock;
    async fn handle(&mut self, _cmd: Knock, _ctx: &mut ActorContext<Knock>) -> Flow {
        Flow::Continue
    }
}

impl Shard for Stranger {
    type Command = Knock;
    type EntityId = String;
    const TYPE: &'static str = "stranger";
    fn entity_id(_cmd: &Knock) -> String {
        "one".to_owned()
    }
    fn shard_id(_cmd: &Knock) -> String {
        "one".to_owned()
    }
}

// ------------------------------------------------------------------ one node

fn one_node() -> ActorSystem {
    let system = ActorSystem::in_memory();
    register(&system);
    system
}

fn register(system: &ActorSystem) {
    system
        .shard::<Session>()
        .register(|_sys, entity| Session {
            address: entity.path.to_string(),
        })
        .expect("session should register");
}

/// A shard actor is built on first contact, at an address derived from the
/// command — nobody creates it by hand.
#[tokio::test]
async fn a_shard_actor_is_built_on_first_contact() {
    let system = one_node();
    let address = system
        .shard_actor_of::<Session>()
        .ask(which("acct-7", "sess-1"))
        .await
        .unwrap();
    assert_eq!(address, "/system/shard/session/acct-7/sess-1");
}

/// Two commands naming one session reach one actor; a different session is a
/// different actor at a different address.
#[tokio::test]
async fn an_entity_id_names_one_actor() {
    let system = one_node();
    let sessions = system.shard_actor_of::<Session>();

    let first = sessions.ask(which("acct-7", "sess-1")).await.unwrap();
    let again = sessions.ask(which("acct-7", "sess-1")).await.unwrap();
    let other = sessions.ask(which("acct-7", "sess-2")).await.unwrap();

    assert_eq!(first, again);
    assert_ne!(first, other);
}

/// A type nobody registered cannot be reached — a named failure rather than an
/// actor spawned with no wiring.
#[tokio::test]
async fn an_unregistered_type_cannot_be_reached() {
    let system = one_node();
    assert!(
        system
            .shard_actor_of::<Stranger>()
            .tell(Knock)
            .await
            .is_err()
    );
}

// --------------------------------------------------------------- many nodes

struct Cluster {
    net: horsie_actor::InProcessNetwork,
    systems: Vec<ActorSystem>,
    nodes: Vec<Arc<ClusterNode>>,
}

impl Cluster {
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
                RaftStore::in_memory_unsafe(),
            )
            .await
            .expect("raft should start");
            let system = ActorSystem::clustered(journal.clone(), node.clone());
            // Every node registers the same recipe, each closing over its own
            // wiring. Nothing about a recipe crosses the wire.
            register(&system);
            if let Some(mut inbox) = node.incoming() {
                let system = system.clone();
                tokio::spawn(async move {
                    while let Some(message) = inbox.recv().await {
                        if let Err(e) = system.dispatch(message).await {
                            eprintln!("dispatch failed: {e}");
                        }
                    }
                });
            }
            systems.push(system);
            nodes.push(node);
        }
        let cluster = Self {
            net,
            systems,
            nodes,
        };
        cluster.await_settled(n as usize).await;
        cluster
    }

    async fn await_settled(&self, expected: usize) {
        for _ in 0..200 {
            let serving: Vec<_> = self.nodes.iter().filter(|n| n.serving()).collect();
            let sets: Vec<Vec<NodeId>> = serving.iter().map(|n| n.live_members()).collect();
            if serving.len() == expected
                && sets
                    .first()
                    .is_some_and(|first| first.len() == expected && sets.iter().all(|s| s == first))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("the cluster never settled");
    }

    /// Index of whichever live node owns an account's shard.
    ///
    /// `excluding` is how a test names nodes it has killed: a node cut off from
    /// the network keeps its own opinion for a moment, and asking it is asking
    /// somebody who has not heard the news.
    async fn host_of(&self, account: &str, excluding: &[usize]) -> usize {
        let shard = format!("/system/shard/session/{account}");
        for _ in 0..200 {
            let found = self
                .nodes
                .iter()
                .enumerate()
                .position(|(i, n)| !excluding.contains(&i) && n.serving() && n.owns(&shard));
            if let Some(index) = found {
                return index;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("no live node ever owned the shard for {account}");
    }

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
            if sets
                .first()
                .is_some_and(|first| !first.contains(&dead) && sets.iter().all(|s| s == first))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("the survivors never agreed that {dead} was gone");
    }

    async fn address_of(&self, node: usize, account: &str, session: &str) -> String {
        self.systems[node]
            .shard_actor_of::<Session>()
            .ask(which(account, session))
            .await
            .unwrap()
    }
}

/// One address, one actor, reached the same way from every node — and
/// indistinguishable at the call site from a local one.
#[tokio::test]
async fn every_node_reaches_one_actor() {
    let cluster = Cluster::of_size(3).await;

    for node in 0..3 {
        assert_eq!(
            cluster.address_of(node, "acct-7", "sess-1").await,
            "/system/shard/session/acct-7/sess-1",
            "node {node} reached a different actor"
        );
    }
}

/// **The payoff.** The owning node dies; the address moves; the same reference
/// keeps working, and the actor is rebuilt by whoever owns it now — with the
/// caller doing nothing.
///
/// Nobody recreates anything by hand here, which is the difference the recipe
/// makes: the node that owns an address can build what belongs there.
#[tokio::test]
async fn an_address_survives_its_host() {
    let cluster = Cluster::of_size(3).await;
    let host = cluster.host_of("acct-7", &[]).await;
    let onlooker = (host + 1) % 3;

    let before = cluster.address_of(onlooker, "acct-7", "sess-1").await;
    assert_eq!(before, "/system/shard/session/acct-7/sess-1");

    cluster.kill(host).await;

    let new_host = cluster.host_of("acct-7", &[host]).await;
    assert_ne!(new_host, host, "placement should have moved the shard");

    // Same command, same answer. Nothing was re-registered, re-created or
    // re-resolved by the test.
    let onlooker = if onlooker == host { new_host } else { onlooker };
    assert_eq!(
        cluster.address_of(onlooker, "acct-7", "sess-1").await,
        before
    );
}

/// `shard_id` is the whole placement policy: two sessions of one account share
/// a shard, so they are placed together — without either being a child of
/// anything, and so without a tree spanning machines.
#[tokio::test]
async fn one_shard_id_places_actors_together() {
    let cluster = Cluster::of_size(3).await;
    let host = cluster.host_of("acct-7", &[]).await;

    for session in ["sess-1", "sess-2", "sess-3"] {
        assert_eq!(
            cluster.address_of(2, "acct-7", session).await,
            format!("/system/shard/session/acct-7/{session}")
        );
    }

    // A different account is a different shard, and placed on its own.
    let other = (0..64)
        .map(|i| format!("acct-{i}"))
        .find(|account| {
            let shard = format!("/system/shard/session/{account}");
            !cluster.nodes[host].owns(&shard)
        })
        .expect("some account must land elsewhere");
    assert_ne!(cluster.host_of(&other, &[]).await, host);
}
