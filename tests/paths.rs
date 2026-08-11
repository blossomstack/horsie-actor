//! A reference addresses a name, not an instance.
//!
//! Everything in the paths design exists so these hold. An `ActorRef` used to be
//! an `mpsc::Sender` — a handle to one mailbox in one process — so it died with
//! the actor it pointed at, and every caller holding one had to notice and
//! re-fetch. A path outlives the instance, so a held reference does not.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use async_trait::async_trait;
use horsie_actor::{Actor, ActorContext, ActorOfError, ActorRef, ActorSystem, Flow, ReplyTo, Root};
use tokio::sync::oneshot;

/// Answers which instance it is, and reports when it is gone.
struct Instance {
    generation: u32,
    /// Fired when the actor value is dropped, which is when its task has ended
    /// and its mailbox is closed. A test that waits on this is asserting the
    /// instance is *gone*, rather than inferring it from a send that happens to
    /// fail.
    gone: Option<oneshot::Sender<()>>,
}

enum Which {
    Ask(ReplyTo<u32>),
    MakeHelper(ReplyTo<ActorRef<HelperCmd>>),
    Stop,
}

impl Instance {
    fn new(generation: u32) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                generation,
                gone: Some(tx),
            },
            rx,
        )
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        if let Some(tx) = self.gone.take() {
            let _ = tx.send(());
        }
    }
}

#[async_trait]
impl Actor for Instance {
    type Command = Which;
    type ParentCommand = Root;

    async fn handle(&mut self, cmd: Which, ctx: &mut ActorContext<Which>) -> Flow {
        match cmd {
            Which::Ask(reply) => {
                let _ = reply.send(self.generation);
                Flow::Continue
            }
            Which::MakeHelper(reply) => {
                // A child is created by its parent, under its parent's path.
                match ctx.actor_of("helper", Helper { parent: None }) {
                    Ok(helper) => {
                        let _ = reply.send(helper);
                    }
                    Err(e) => panic!("could not create the helper: {e}"),
                }
                Flow::Continue
            }
            Which::Stop => Flow::Stop,
        }
    }
}

/// A child that reaches its parent by name, having been handed nothing at
/// construction — which is what an actor built on a host that never saw its
/// parent has to be able to do.
struct Helper {
    /// Resolved once and then held, so a later send goes through a cached link
    /// rather than resolving afresh. Holding it is the realistic case, and the
    /// only one that exercises a link going stale under its holder.
    parent: Option<ActorRef<Which>>,
}

enum HelperCmd {
    AskUpwards(ReplyTo<u32>),
}

#[async_trait]
impl Actor for Helper {
    type Command = HelperCmd;
    type ParentCommand = Which;

    async fn handle(&mut self, cmd: HelperCmd, ctx: &mut ActorContext<HelperCmd, Which>) -> Flow {
        match cmd {
            HelperCmd::AskUpwards(reply) => {
                let parent = self.parent.get_or_insert_with(|| ctx.parent());
                let generation = parent.ask(Which::Ask).await.unwrap_or(0);
                let _ = reply.send(generation);
                Flow::Continue
            }
        }
    }
}

async fn generation(actor: &ActorRef<Which>) -> u32 {
    actor.ask(Which::Ask).await.unwrap()
}

/// **The point of the whole design.** A reference held across a stop and a
/// recreate keeps working, with the caller doing nothing — no liveness check, no
/// re-fetch, no knowledge that anything happened.
///
/// The generations are what make it a real assertion: the second answer comes
/// from a genuinely different instance, so it cannot pass by the first one
/// having quietly stayed alive.
#[tokio::test]
async fn a_ref_survives_a_stop_and_a_recreate() {
    let system = ActorSystem::in_memory();

    let (first, first_gone) = Instance::new(1);
    let held = system.actor_of("worker", first).unwrap();
    assert_eq!(generation(&held).await, 1);

    held.tell(Which::Stop).await.unwrap();
    first_gone.await.expect("the first instance should be gone");

    let (second, _second_gone) = Instance::new(2);
    let _fresh = system.actor_of("worker", second).unwrap();

    // Nothing was done to `held`. It named `/worker` before and it names
    // `/worker` now.
    assert_eq!(generation(&held).await, 2);
}

/// A ref to a path with nothing at it fails, and keeps failing, rather than
/// resurrecting an actor nobody created.
///
/// The counterpart to the test above: re-resolution recovers a name that has
/// been recreated, not one that never was. Recreating on send is what would
/// break the rule that reading must not wake a session.
#[tokio::test]
async fn a_ref_to_an_empty_path_fails_rather_than_creating() {
    let system = ActorSystem::in_memory();

    let (first, first_gone) = Instance::new(1);
    let held = system.actor_of("worker", first).unwrap();
    held.tell(Which::Stop).await.unwrap();
    first_gone.await.expect("the first instance should be gone");

    assert!(held.tell(Which::Stop).await.is_err());
    // Twice, because a re-resolve that resurrected on the first failure would
    // make the second send succeed.
    assert!(held.tell(Which::Stop).await.is_err());
}

/// Two callers naming one path get one actor. The second `actor_of` hands back
/// what is already there and never starts the actor it was given.
#[tokio::test]
async fn actor_of_is_get_or_create() {
    let system = ActorSystem::in_memory();

    let (first, _first_gone) = Instance::new(1);
    let a = system.actor_of("worker", first).unwrap();
    let (second, second_gone) = Instance::new(2);
    let b = system.actor_of("worker", second).unwrap();

    assert_eq!(generation(&a).await, 1);
    assert_eq!(generation(&b).await, 1);
    // The loser was dropped, never started — otherwise two actors would share
    // one name, and for an event-sourced pair, one journal.
    second_gone.await.expect("the loser should be dropped");
}

/// A name that could not be one path segment is refused where it is given,
/// rather than producing a path with two readings.
#[tokio::test]
async fn an_unusable_name_is_refused() {
    let system = ActorSystem::in_memory();

    for name in ["", "a/b"] {
        let (instance, gone) = Instance::new(1);
        let err = system.actor_of(name, instance).unwrap_err();
        assert!(matches!(err, ActorOfError::InvalidName(n) if n == name));
        // Nothing was started, so nothing has to be cleaned up.
        gone.await.expect("the rejected actor should be dropped");
    }
}

/// Two actor types cannot share one name. The caller is told, rather than handed
/// a reference of the wrong type or silently given somebody else's actor.
#[tokio::test]
async fn a_path_held_by_another_type_is_reported() {
    let system = ActorSystem::in_memory();

    let (instance, _gone) = Instance::new(1);
    system.actor_of("worker", instance).unwrap();

    let err = system
        .actor_of("worker", Helper { parent: None })
        .unwrap_err();
    assert!(matches!(err, ActorOfError::PathTaken(path) if path.to_string() == "/worker"));
}

/// A path is where an actor is, so the same child name under different parents
/// is different actors.
#[tokio::test]
async fn a_child_lives_under_its_parent() {
    let system = ActorSystem::in_memory();

    let (a, _a_gone) = Instance::new(1);
    let (b, _b_gone) = Instance::new(2);
    let a = system.actor_of("worker-a", a).unwrap();
    let b = system.actor_of("worker-b", b).unwrap();

    let helper_a = a.ask(Which::MakeHelper).await.unwrap();
    let helper_b = b.ask(Which::MakeHelper).await.unwrap();

    assert_eq!(helper_a.path().to_string(), "/worker-a/helper");
    assert_eq!(helper_b.path().to_string(), "/worker-b/helper");
    assert_ne!(helper_a.path(), helper_b.path());
}

/// `ctx.parent()` is an ordinary ref to the parent's path — resolved by name,
/// not handed down.
#[tokio::test]
async fn a_child_names_its_parent() {
    let system = ActorSystem::in_memory();

    let (parent, _parent_gone) = Instance::new(3);
    let parent = system.actor_of("worker", parent).unwrap();
    let helper = parent.ask(Which::MakeHelper).await.unwrap();

    assert_eq!(helper.ask(HelperCmd::AskUpwards).await.unwrap(), 3);
}

/// A child's ref survives its parent being recreated too — the child is
/// addressed by its own path, and the parent it names is resolved per send.
#[tokio::test]
async fn a_parent_ref_survives_the_parent_being_recreated() {
    let system = ActorSystem::in_memory();

    let (first, first_gone) = Instance::new(1);
    let parent = system.actor_of("worker", first).unwrap();
    let helper = parent.ask(Which::MakeHelper).await.unwrap();
    assert_eq!(helper.ask(HelperCmd::AskUpwards).await.unwrap(), 1);

    parent.tell(Which::Stop).await.unwrap();
    first_gone.await.expect("the first parent should be gone");

    let (second, _second_gone) = Instance::new(2);
    let _fresh = system.actor_of("worker", second).unwrap();

    assert_eq!(helper.ask(HelperCmd::AskUpwards).await.unwrap(), 2);
}
