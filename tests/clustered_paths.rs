//! Clustering is a property of an address, chosen by configuration.
//!
//! The same actor tree runs on one node with no configuration at all and across
//! a cluster with it, and the code is identical either way. What changes is
//! which addresses the cluster places, and nothing a caller can see.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::wildcard_enum_match_arm
)]

use async_trait::async_trait;
use horsie_actor::{
    Actor, ActorContext, ActorOfError, ActorPath, ActorRef, ActorSettings, ActorSystem,
    AddressPattern, ClusterConfig, ClusterNode, DispatchError, Envelope, Flow, InMemoryJournal,
    Journal, Message, NodeId, RaftStore, ReplyTo, Root, SettingsTable,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Says which instance it is. Its commands round-trip, so it may be clustered.
struct Worker {
    generation: u32,
}

#[derive(Serialize, Deserialize)]
enum Job {
    Which(ReplyTo<u32>),
}

#[async_trait]
impl Actor for Worker {
    type Command = Job;
    type ParentCommand = Root;

    async fn handle(&mut self, cmd: Job, _ctx: &mut ActorContext<Job>) -> Flow {
        match cmd {
            Job::Which(reply) => {
                let _ = reply.send(self.generation);
                Flow::Continue
            }
        }
    }
}

/// Carries a reply handle that is not serializable, so it can never be
/// clustered — which is the point of it.
struct Homebody;

enum Chore {
    /// Never sent: this actor exists to be *unable* to receive from elsewhere,
    /// and one unconstructed variant is what gives it a command type that does
    /// not round-trip.
    #[allow(dead_code, reason = "the type is the point, not the traffic")]
    Poke,
}

#[async_trait]
impl Actor for Homebody {
    type Command = Chore;
    type ParentCommand = Root;

    async fn handle(&mut self, cmd: Chore, _ctx: &mut ActorContext<Chore>) -> Flow {
        match cmd {
            Chore::Poke => Flow::Continue,
        }
    }
}

fn pattern(text: &str) -> AddressPattern {
    AddressPattern::parse(text).unwrap()
}

fn everything_clustered() -> SettingsTable {
    SettingsTable::new()
        .with(pattern("/*"), ActorSettings::clustered())
        .unwrap()
}

async fn generation(actor: &ActorRef<Job>) -> u32 {
    actor.ask(Job::Which).await.unwrap()
}

// ------------------------------------------------------------------ one node

/// **The single-node claim.** Configure every top-level address as a clustered
/// singleton and, on one node, nothing changes: same creation, same reference,
/// same answer. So the same binary serves both deployments and one of them
/// mentions none of this.
#[tokio::test]
async fn one_node_behaves_the_same_clustered_or_not() {
    let mut answers = Vec::new();
    for settings in [SettingsTable::new(), everything_clustered()] {
        let system = ActorSystem::with_settings(Arc::new(InMemoryJournal::new()), settings);
        system.register_clusterable::<Worker>();

        let worker = system.actor_of("w", Worker { generation: 1 }).unwrap();
        answers.push((worker.path().to_string(), generation(&worker).await));
    }
    let (unclustered, clustered) = (&answers[0], &answers[1]);
    assert_eq!(unclustered, clustered);
}

/// Config *chooses* what is clustered; it cannot *grant* it. Paths appear at
/// runtime, so this cannot be caught at boot — being caught at creation, naming
/// the path and the type, is the next best thing.
#[tokio::test]
async fn a_clustered_address_refuses_an_actor_that_cannot_encode() {
    let system =
        ActorSystem::with_settings(Arc::new(InMemoryJournal::new()), everything_clustered());
    // Deliberately not registered as clusterable — its commands do not round-trip.
    let err = system.actor_of("h", Homebody).unwrap_err();
    match err {
        ActorOfError::NotClusterable { path, actor } => {
            assert_eq!(path.to_string(), "/h");
            assert!(actor.ends_with("Homebody"), "should name the type: {actor}");
        }
        other => panic!("expected NotClusterable, got {other}"),
    }
}

/// The same actor at an address the config leaves alone is fine, because
/// nothing was ever going to send to it from elsewhere.
#[tokio::test]
async fn an_unclustered_address_takes_any_actor() {
    let settings = SettingsTable::new()
        .with(pattern("/*"), ActorSettings::clustered())
        .unwrap()
        .with(pattern("/*/*"), ActorSettings::local())
        .unwrap();
    let system = ActorSystem::with_settings(Arc::new(InMemoryJournal::new()), settings);
    system.register_clusterable::<Worker>();

    // `/w` is clustered, and Worker can be. `/w/h` is not, so Homebody may live
    // there even though it could never cross a host.
    let worker = system.actor_of("w", Worker { generation: 1 }).unwrap();
    assert!(system.settings_at(worker.path()).clustered.value);

    let child = worker.path().child("h");
    let settings = system.settings_at(&child);
    assert!(!settings.clustered.value);
    assert_eq!(settings.clustered.set_by, Some(pattern("/*/*")));
}

/// Patterns compose invisibly, so the system has to be able to say what applies
/// at an address and which entry decided it. Without that the first surprising
/// configuration is unanswerable.
#[tokio::test]
async fn the_system_explains_which_entry_decided_a_setting() {
    let settings = SettingsTable::new()
        .with(pattern("/*"), ActorSettings::clustered())
        .unwrap()
        .with(pattern("/acct-7"), ActorSettings::local())
        .unwrap();
    let system = ActorSystem::with_settings(Arc::new(InMemoryJournal::new()), settings);

    let broad = system.settings_at(&ActorPath::root().child("acct-8"));
    assert!(broad.clustered.value);
    assert_eq!(broad.clustered.set_by, Some(pattern("/*")));

    let narrow = system.settings_at(&ActorPath::root().child("acct-7"));
    assert!(!narrow.clustered.value);
    assert_eq!(narrow.clustered.set_by, Some(pattern("/acct-7")));

    // Nothing matched, so this is the default and says so.
    let deep = system.settings_at(&ActorPath::root().child("a").child("b"));
    assert!(!deep.clustered.value);
    assert_eq!(deep.clustered.set_by, None);
}

/// An actor at a local address has no wire format, so a message that arrives for
/// it from another node is refused — and the refusal says *why*, because
/// "nothing is there" would send somebody looking for the wrong bug.
#[tokio::test]
async fn a_local_only_actor_refuses_a_message_from_another_host() {
    let system = ActorSystem::in_memory();
    let actor = system.actor_of("h", Homebody).unwrap();

    let err = system
        .dispatch(Message::Command(Envelope {
            path: actor.path().to_string(),
            message_id: 1,
            payload: b"{}".to_vec(),
        }))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, DispatchError::LocalOnly(path) if path == actor.path()),
        "expected LocalOnly, got {err}"
    );
}

/// An address nothing is at is a different failure from one that is local-only.
#[tokio::test]
async fn an_empty_address_is_reported_as_empty() {
    let system = ActorSystem::in_memory();
    let err = system
        .dispatch(Message::Command(Envelope {
            path: "/nobody".to_owned(),
            message_id: 1,
            payload: b"{}".to_vec(),
        }))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, DispatchError::NoActor(path) if path.to_string() == "/nobody"),
        "expected NoActor, got {err}"
    );
}

// --------------------------------------------------------------- many nodes

/// Nodes sharing one journal and one settings table.
struct Cluster {
    net: horsie_actor::InProcessNetwork,
    systems: Vec<ActorSystem>,
    nodes: Vec<Arc<ClusterNode>>,
}

impl Cluster {
    async fn of_size(n: u64, settings: &SettingsTable) -> Self {
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
            let system = ActorSystem::clustered(journal.clone(), node.clone(), settings.clone());
            system.register_clusterable::<Worker>();
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
            let agreed = sets
                .first()
                .is_some_and(|first| first.len() == expected && sets.iter().all(|s| s == first));
            if serving.len() == expected && agreed {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("the cluster never settled");
    }

    /// Index of whichever live node the address belongs to.
    ///
    /// `excluding` is how a test names nodes it has killed: a node cut off from
    /// the network keeps its own opinion for a moment, and asking it is asking
    /// somebody who has not heard the news.
    async fn host_of(&self, path: &str, excluding: &[usize]) -> usize {
        for _ in 0..200 {
            let found = self
                .nodes
                .iter()
                .enumerate()
                .position(|(i, n)| !excluding.contains(&i) && n.serving() && n.owns(path));
            if let Some(index) = found {
                return index;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("no live node ever owned {path}");
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
}

/// One address, one actor, reachable from every node — and indistinguishable at
/// the call site from a local one.
#[tokio::test]
async fn a_clustered_address_resolves_to_one_actor_from_every_node() {
    let cluster = Cluster::of_size(3, &everything_clustered()).await;
    let host = cluster.host_of("/w", &[]).await;

    let owner = cluster.systems[host]
        .actor_of("w", Worker { generation: 7 })
        .unwrap();
    assert_eq!(generation(&owner).await, 7);

    for (index, system) in cluster.systems.iter().enumerate() {
        // Every node asks for the same address the same way. The `Worker` handed
        // in is only used if this node turns out to own it — the others get a
        // reference to the one that exists and drop what they were given.
        let elsewhere = system.actor_of("w", Worker { generation: 99 }).unwrap();
        assert_eq!(
            generation(&elsewhere).await,
            7,
            "node {index} reached a different actor"
        );
    }
}

/// **P1's test, across hosts.** A reference held on another node, across the
/// actor moving to a different host, keeps working — the holder does nothing and
/// knows nothing.
#[tokio::test]
async fn a_ref_survives_a_relocation() {
    let cluster = Cluster::of_size(3, &everything_clustered()).await;
    let host = cluster.host_of("/w", &[]).await;
    let onlooker = (host + 1) % 3;

    cluster.systems[host]
        .actor_of("w", Worker { generation: 1 })
        .unwrap();

    // Held on a node that does not host it, so this is a remote reference from
    // the first send onwards.
    let held = cluster.systems[onlooker]
        .actor_of("w", Worker { generation: 0 })
        .unwrap();
    assert_eq!(generation(&held).await, 1);

    cluster.kill(host).await;

    // The address now belongs to somebody else, and a fresh instance is created
    // there — which is what an offload-and-reactivate looks like from the
    // outside too.
    let new_host = cluster.host_of("/w", &[host]).await;
    assert_ne!(new_host, host, "placement should have moved the address");
    cluster.systems[new_host]
        .actor_of("w", Worker { generation: 2 })
        .unwrap();

    // Nothing was done to `held`.
    assert_eq!(generation(&held).await, 2);
}
