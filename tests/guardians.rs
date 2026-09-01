//! An actor owns the actors below it.
//!
//! Before this, stopping a parent left its children running under an address
//! whose parent no longer existed: `ctx.parent()` addressed nothing, "offload
//! this session" had to be repeated by hand at every level, and the registry
//! grew a row per actor that had ever run.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use async_trait::async_trait;
use horsie_actor::{Actor, ActorContext, ActorPath, ActorRef, ActorSystem, Flow, ReplyTo};
use parking_lot::Mutex;
use std::sync::Arc;

/// A chain-builder: told to grow, it creates one child like itself.
///
/// The same type at every level, so a three-deep tree needs no three types and
/// the recursion under test is the runtime's rather than the fixture's.
struct Link {
    name: String,
    /// Every actor in one tree shares this, and each appends its own name as it
    /// is dropped — which is when its task has ended.
    gone: Arc<Mutex<Vec<String>>>,
}

enum Grow {
    /// Make a child named `0`, and hand back a reference to it.
    Child(ReplyTo<ActorRef<Grow>>),
    /// Stop, from the inside.
    Stop,
}

#[async_trait]
impl Actor for Link {
    type Command = Grow;
    // Every level takes the same commands, so a child's parent does too.

    async fn handle(&mut self, cmd: Grow, ctx: &mut ActorContext<Grow>) -> Flow {
        match cmd {
            Grow::Child(reply) => {
                let child = ctx
                    .actor_of(
                        "0",
                        Link {
                            name: format!("{}/0", self.name),
                            gone: Arc::clone(&self.gone),
                        },
                    )
                    .expect("a child should be creatable");
                let _ = reply.send(child);
                Flow::Continue
            }
            Grow::Stop => Flow::Stop,
        }
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        self.gone.lock().push(self.name.clone());
    }
}

/// A root and two levels below it, plus the record of who ends when.
async fn chain(system: &ActorSystem) -> (ActorRef<Grow>, ActorRef<Grow>, Arc<Mutex<Vec<String>>>) {
    let gone = Arc::new(Mutex::new(Vec::new()));
    let root = system
        .actor_of(
            "top",
            Link {
                name: "top".into(),
                gone: Arc::clone(&gone),
            },
        )
        .unwrap();
    let child = root.ask(Grow::Child).await.unwrap();
    let grandchild = child.ask(Grow::Child).await.unwrap();
    (root, grandchild, gone)
}

/// The headline. Stopping an actor stops everything beneath it, however deep,
/// and the caller knows it has happened rather than that it is going to.
#[tokio::test]
async fn stopping_an_actor_stops_everything_under_it() {
    let system = ActorSystem::in_memory();
    let (root, grandchild, gone) = chain(&system).await;

    assert!(root.stop().await);

    assert_eq!(gone.lock().len(), 3, "not every level ended");
    assert!(
        grandchild.ask(Grow::Child).await.is_err(),
        "a grandchild outlived the actor two levels above it"
    );
}

/// Children end before their parent does, so a parent's last act runs over a
/// subtree that is already quiet. Without the order there is no moment at which
/// a parent can rely on its children being gone rather than going.
#[tokio::test]
async fn children_end_before_the_parent_does() {
    let system = ActorSystem::in_memory();
    let (root, _grandchild, gone) = chain(&system).await;

    root.stop().await;

    assert_eq!(
        gone.lock().as_slice(),
        ["top/0/0", "top/0", "top"],
        "the tree came down the wrong way up"
    );
}

/// An actor that stops itself is stopped the same way, so `Flow::Stop` and an
/// external stop cannot drift apart — one taking its children and the other
/// leaving them.
#[tokio::test]
async fn an_actor_that_stops_itself_takes_its_children_too() {
    let system = ActorSystem::in_memory();
    let (root, grandchild, gone) = chain(&system).await;

    root.tell(Grow::Stop).await.unwrap();

    for _ in 0..100 {
        if gone.lock().len() == 3 {
            assert!(grandchild.ask(Grow::Child).await.is_err());
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("stopping from the inside left {:?} behind", gone.lock());
}

/// A stopped actor leaves the registry, so a path that held one holds nothing
/// again. Otherwise every actor that ever ran keeps a row — and, for an
/// event-sourced one, the journal handle behind it.
#[tokio::test]
async fn a_stopped_subtree_leaves_the_registry() {
    let system = ActorSystem::in_memory();
    let (root, _grandchild, _gone) = chain(&system).await;
    let deep = ActorPath::parse("/top/0/0").unwrap();

    root.stop().await;

    // Nothing is at either address, and a reference to one reaches nothing —
    // which is the observable form of "the row is gone".
    assert!(!system.stop(&deep).await, "the grandchild's row survived");
    assert!(!system.stop(root.path()).await, "the root's row survived");
}

/// An actor that ends on its own takes its row with it.
///
/// Something else removes the row when a stop comes from outside, so this is
/// the case that would otherwise leak: every actor that ever ran keeps a row —
/// and a journal handle — until something happens to be created at exactly the
/// same address.
#[tokio::test]
async fn an_actor_that_ends_on_its_own_leaves_the_registry() {
    let system = ActorSystem::in_memory();
    let (root, _grandchild, _gone) = chain(&system).await;
    assert_eq!(system.hosted(), 3);

    root.tell(Grow::Stop).await.unwrap();

    for _ in 0..100 {
        if system.hosted() == 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("{} rows outlived the actors behind them", system.hosted());
}

/// A branch with no actor at its head can still be cleared, which is what a
/// shard address looks like on the way down to an entity: `/system/shard/x/s`
/// holds nothing itself and everything underneath it.
#[tokio::test]
async fn a_branch_can_be_cleared_from_a_path_that_holds_no_actor() {
    let system = ActorSystem::in_memory();
    let (_root, grandchild, gone) = chain(&system).await;

    let held_nothing = system.stop(&ActorPath::parse("/top/0").unwrap()).await;

    assert!(held_nothing, "there was an actor at /top/0");
    assert!(grandchild.ask(Grow::Child).await.is_err());
    assert_eq!(gone.lock().as_slice(), ["top/0/0", "top/0"]);
}

/// Stopping is not deleting: the path is unchanged, so an actor created there
/// afterwards is reached by every reference held across the gap. The same
/// promise `Flow::Stop` already made, extended to a stop from outside — the two
/// must not differ, because a holder cannot tell which one happened.
#[tokio::test]
async fn a_ref_survives_a_stop_from_outside() {
    let system = ActorSystem::in_memory();
    let gone = Arc::new(Mutex::new(Vec::new()));
    let make = |name: &str| Link {
        name: name.to_owned(),
        gone: Arc::clone(&gone),
    };

    let held = system.actor_of("top", make("first")).unwrap();
    held.stop().await;
    system.actor_of("top", make("second")).unwrap();

    assert!(
        held.ask(Grow::Child).await.is_ok(),
        "a reference held across a stop stopped working"
    );
    assert_eq!(gone.lock().as_slice(), ["first"]);
}

/// Stopping a path nothing is at is not an error. A caller clearing a branch
/// does not have to know what is under it, which is the whole reason to address
/// one by path.
#[tokio::test]
async fn stopping_nothing_is_not_an_error() {
    let system = ActorSystem::in_memory();
    assert!(!system.stop(&ActorPath::parse("/nobody").unwrap()).await);
}

/// A stopped actor's children do not come back with it. The tree is rebuilt by
/// whoever wanted it, not recovered by the runtime — nothing in the registry
/// records what *used* to be under a path.
#[tokio::test]
async fn a_recreated_actor_starts_with_no_children() {
    let system = ActorSystem::in_memory();
    let (root, _grandchild, gone) = chain(&system).await;

    root.stop().await;
    let fresh = system
        .actor_of(
            "top",
            Link {
                name: "again".into(),
                gone: Arc::clone(&gone),
            },
        )
        .unwrap();

    // If a child had survived, stopping the fresh root would end it now.
    fresh.stop().await;
    assert_eq!(
        gone.lock().as_slice(),
        ["top/0/0", "top/0", "top", "again"],
        "something from the old tree was still under the path"
    );
}

/// Draining what this node hosts of a shard type: the address a shard reference
/// carries holds no actor of its own, and everything of that type sits beneath
/// it.
///
/// The reason `stop` sweeps a path rather than only stopping the actor at it.
#[tokio::test]
async fn stopping_a_shard_reference_drains_what_this_node_hosts() {
    struct Entity;
    #[async_trait]
    impl Actor for Entity {
        type Command = Ping;
        async fn handle(&mut self, _cmd: Ping, _ctx: &mut ActorContext<Ping>) -> Flow {
            Flow::Continue
        }
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Ping(String);
    impl horsie_actor::Shard for Entity {
        type Command = Ping;
        type EntityId = String;
        type ShardId = String;
        const TYPE: &'static str = "entity";
        fn entity_id(cmd: &Ping) -> String {
            cmd.0.clone()
        }
        fn shard_id(cmd: &Ping) -> String {
            cmd.0.clone()
        }
    }

    let system = ActorSystem::in_memory();
    system
        .shard::<Entity>()
        .register(|_sys, _entity| Entity)
        .unwrap();
    let entities = system.shard_actor_of::<Entity>();
    entities.tell(Ping("a".into())).await.unwrap();
    entities.tell(Ping("b".into())).await.unwrap();
    assert_eq!(system.hosted(), 2);

    entities.stop().await;

    assert_eq!(system.hosted(), 0, "the node was not drained");
    // And the type is still usable — draining is not deregistering.
    entities.tell(Ping("a".into())).await.unwrap();
    assert_eq!(system.hosted(), 1);
}

/// A guard against the tree's types being the reason it works: root's parent is
/// `Root`, so `ParentCommand = Grow` cannot be what holds the chain together.
#[tokio::test]
async fn the_tree_is_the_paths_not_the_types() {
    struct Loner;
    #[async_trait]
    impl Actor for Loner {
        type Command = ();
        async fn handle(&mut self, _cmd: (), _ctx: &mut ActorContext<()>) -> Flow {
            Flow::Continue
        }
    }

    let system = ActorSystem::in_memory();
    let (root, _grandchild, _gone) = chain(&system).await;
    let unrelated = system.actor_of("elsewhere", Loner).unwrap();

    root.stop().await;

    assert!(
        unrelated.tell(()).await.is_ok(),
        "an actor on another branch was stopped too"
    );
}

// ---------------------------------------------------------------- death-watch

/// Pin a watch and prove it has not fired yet — the half of every death-watch
/// assertion that keeps it honest.
macro_rules! still_watching {
    ($fut:expr) => {
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut $fut)
                .await
                .is_err(),
            "the watch fired while the actor was alive"
        )
    };
}

async fn fires(fut: impl std::future::Future<Output = ()>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), fut)
        .await
        .expect("the watch never fired");
}

/// The watch resolves when the actor ends — here, a stop from outside.
#[tokio::test]
async fn a_watcher_learns_when_an_actor_ends() {
    let system = ActorSystem::in_memory();
    let (root, _grandchild, _gone) = chain(&system).await;

    let mut watch = std::pin::pin!(root.terminated());
    still_watching!(watch);

    root.stop().await;
    fires(watch).await;
}

/// `Flow::Stop` resolves it identically: a holder cannot tell how an actor
/// ended, so the watch must not differ either.
#[tokio::test]
async fn a_watcher_learns_when_an_actor_stops_itself() {
    let system = ActorSystem::in_memory();
    let (root, _grandchild, _gone) = chain(&system).await;

    let mut watch = std::pin::pin!(root.terminated());
    still_watching!(watch);

    root.tell(Grow::Stop).await.unwrap();
    fires(watch).await;
}

/// A path that holds nothing resolves at once — to a watcher, "never existed"
/// and "already ended" are the same news.
#[tokio::test]
async fn watching_an_empty_path_resolves_immediately() {
    let system = ActorSystem::in_memory();
    let (root, _grandchild, _gone) = chain(&system).await;
    root.stop().await;

    fires(root.terminated()).await;
}

/// The watch is bound to the instance, not the path: one taken before a stop
/// is resolved by that stop, however promptly something new takes the name.
#[tokio::test]
async fn a_watch_follows_the_instance_not_the_path() {
    let system = ActorSystem::in_memory();
    let gone = Arc::new(Mutex::new(Vec::new()));
    let make = |name: &str| Link {
        name: name.to_owned(),
        gone: Arc::clone(&gone),
    };

    let first = system.actor_of("top", make("first")).unwrap();
    let watch = first.terminated();
    first.stop().await;
    system.actor_of("top", make("second")).unwrap();

    // The old instance's end already resolved it; the newcomer is a new life.
    fires(watch).await;
}

/// **The case #40 called out.** An actor whose `on_start` fails used to just
/// disappear — logged, mailbox closed, nobody told. It still stops without
/// processing anything, but now the failure is *heard*: the watch fires, and a
/// caller's `ask` fails rather than hangs.
#[tokio::test]
async fn a_failed_start_resolves_the_watch() {
    struct Stillborn;
    #[async_trait]
    impl Actor for Stillborn {
        type Command = ReplyTo<()>;
        async fn handle(
            &mut self,
            _cmd: ReplyTo<()>,
            _ctx: &mut ActorContext<ReplyTo<()>>,
        ) -> Flow {
            Flow::Continue
        }
        async fn on_start(
            &mut self,
            _ctx: &mut ActorContext<ReplyTo<()>>,
        ) -> Result<(), horsie_actor::StartError> {
            Err(horsie_actor::JournalError::Backend("no history to stand on".into()).into())
        }
    }

    let system = ActorSystem::in_memory();
    let doomed = system.actor_of("doomed", Stillborn).unwrap();

    fires(doomed.terminated()).await;
    assert!(
        doomed.ask(|reply| reply).await.is_err(),
        "an ask against a failed start should fail, not hang"
    );
}

/// A handler that panics is still an end, and the watch still fires — the
/// failure a supervisor least expects is the one it most needs to hear.
#[tokio::test]
async fn a_panicking_actor_resolves_the_watch() {
    struct Bomb;
    #[async_trait]
    impl Actor for Bomb {
        type Command = ();
        async fn handle(&mut self, _cmd: (), _ctx: &mut ActorContext<()>) -> Flow {
            panic!("boom");
        }
    }

    let system = ActorSystem::in_memory();
    let bomb = system.actor_of("bomb", Bomb).unwrap();
    let mut watch = std::pin::pin!(bomb.terminated());
    still_watching!(watch);

    bomb.tell(()).await.unwrap();
    fires(watch).await;
}

/// The supervision pattern the docs promise: restart and policy are a watcher
/// with a counter, at the parent, as plain code. The parent re-creates its
/// child when it ends, gives up after two lives, and a reference held across
/// every restart keeps working — because a restart is a fresh instance at the
/// same path.
#[tokio::test]
async fn a_parent_supervises_by_watching() {
    /// Stops on any command — a child that keeps failing.
    struct Quitter;
    #[async_trait]
    impl Actor for Quitter {
        type Command = ();
        async fn handle(&mut self, _cmd: (), _ctx: &mut ActorContext<()>) -> Flow {
            Flow::Stop
        }
    }

    struct Parent {
        lives: u32,
    }
    enum Supervise {
        Start(ReplyTo<ActorRef<()>>),
        ChildDown,
        Lives(ReplyTo<u32>),
    }
    #[async_trait]
    impl Actor for Parent {
        type Command = Supervise;
        async fn handle(&mut self, cmd: Supervise, ctx: &mut ActorContext<Supervise>) -> Flow {
            match cmd {
                Supervise::Start(reply) => {
                    let child = ctx.actor_of("worker", Quitter).unwrap();
                    watch(&child, ctx);
                    let _ = reply.send(child);
                    Flow::Continue
                }
                Supervise::ChildDown => {
                    self.lives += 1;
                    // The policy: two lives, then let it lie.
                    if self.lives < 2 {
                        let child = ctx.actor_of("worker", Quitter).unwrap();
                        watch(&child, ctx);
                    }
                    Flow::Continue
                }
                Supervise::Lives(reply) => {
                    let _ = reply.send(self.lives);
                    Flow::Continue
                }
            }
        }
    }

    /// Death-watch, folded back into the parent's own mailbox.
    fn watch(child: &ActorRef<()>, ctx: &ActorContext<Supervise>) {
        let child = child.clone();
        let parent = ctx.self_ref();
        tokio::spawn(async move {
            child.terminated().await;
            let _ = parent.tell(Supervise::ChildDown).await;
        });
    }

    let system = ActorSystem::in_memory();
    let parent = system.actor_of("parent", Parent { lives: 0 }).unwrap();
    let child = parent.ask(Supervise::Start).await.unwrap();

    let wait_for_lives = |want: u32| {
        let parent = parent.clone();
        async move {
            for _ in 0..200 {
                let lives = parent.ask(Supervise::Lives).await.unwrap();
                if lives >= want {
                    return lives;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            panic!("the parent never counted {want} lives");
        }
    };

    // First death: the policy restarts it, and the held reference reaches the
    // replacement.
    child.tell(()).await.unwrap();
    assert_eq!(wait_for_lives(1).await, 1);
    child.tell(()).await.unwrap();

    // Second death: the policy gives up, and the path stays empty.
    assert_eq!(wait_for_lives(2).await, 2);
    assert!(
        child.tell(()).await.is_err(),
        "the parent's policy said two lives, but something answered a third"
    );
}
