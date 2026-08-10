use crate::actor::EventSourcedActor;
use crate::behaviour::Actor;
use crate::journal::{InMemoryJournal, Journal};
use crate::persistent::Persistent;
use crate::runtime::{ActorContext, ActorRef, MAILBOX_CAPACITY, run_actor};
use parking_lot::Mutex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

/// A registered actor type, hostable by id.
///
/// This is a *registration descriptor*, not a behaviour — deliberately not a
/// subtrait of [`Actor`]. Keeping it separate is what lets a registered type
/// choose its own hosting: [`ClusterActor::spawn`] may call
/// [`ActorSystem::spawn`] for a plain actor or [`ActorSystem::spawn_persistent`]
/// for an event-sourced one, so being cluster-hostable and being event-sourced
/// stay independent.
///
/// The `Command` bounds are the compile-time guarantee: a type whose commands
/// cannot round-trip through serde cannot be registered at all, so a command
/// that could not survive a hop between hosts is a type error rather than a
/// runtime surprise on the first send.
pub trait ClusterActor: Send + 'static {
    /// Stable name for this actor type, and half of every instance's identity.
    /// Changing it orphans existing journals.
    const KIND: &'static str;

    /// Messages instances of this type accept.
    type Command: Send + Serialize + DeserializeOwned + 'static;

    /// Node-local wiring — pools, clients, registries. Never serialized and
    /// never sent anywhere; each host supplies its own.
    type Deps: Clone + Send + Sync + 'static;

    /// Build and spawn the instance for `id`.
    ///
    /// This is what a host that never executed the original request calls, which
    /// is why it takes an id and deps rather than a constructed actor.
    fn spawn(id: &str, deps: Self::Deps, system: &ActorSystem) -> ActorRef<Self::Command>;
}

/// Why [`ActorSystem::actor_of`] could not produce a reference.
#[derive(Debug, Error)]
pub enum ActorOfError {
    /// No factory was registered for this kind. `register` is called once at
    /// startup, so this is a wiring mistake, not a runtime condition.
    #[error("no actor type is registered under the kind '{0}'")]
    NotRegistered(&'static str),

    /// Two actor types declared the same `KIND`. Their instances would share a
    /// registry slot and hand each other's callers the wrong `ActorRef`.
    #[error("two actor types are registered under the kind '{0}'")]
    KindCollision(&'static str),
}

/// A live instance's `ActorRef`, with its command type erased so refs of
/// different kinds can share one registry.
type ErasedRef = Arc<dyn Any + Send + Sync>;

/// Builds an instance of some registered kind and returns a type-erased ref.
type Factory = Arc<dyn Fn(&str, &ActorSystem) -> ErasedRef + Send + Sync>;

/// Process-wide state shared by every actor in a system.
pub(crate) struct SystemInner {
    pub(crate) journal: Arc<dyn Journal>,
    factories: Mutex<HashMap<&'static str, Factory>>,
    /// Live instances, keyed by kind and id.
    live: Mutex<HashMap<(&'static str, String), ErasedRef>>,
}

/// The runtime an actor tree lives in: its journal, its registered actor types,
/// and the instances currently hosted.
#[derive(Clone)]
pub struct ActorSystem {
    inner: Arc<SystemInner>,
}

impl ActorSystem {
    /// A system backed by `journal`.
    #[must_use]
    pub fn new(journal: Arc<dyn Journal>) -> Self {
        Self {
            inner: Arc::new(SystemInner {
                journal,
                factories: Mutex::new(HashMap::new()),
                live: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// A system backed by an [`InMemoryJournal`] — tests and single-process runs.
    #[must_use]
    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemoryJournal::new()))
    }

    /// Spawn a plain actor. It is not addressable by id; the returned reference
    /// is the only way to reach it.
    pub fn spawn<A: Actor>(&self, actor: A) -> ActorRef<A::Command> {
        spawn_in(actor, self.inner.clone())
    }

    /// Spawn an event-sourced actor. It recovers from its own `persistence_id`
    /// before accepting commands.
    pub fn spawn_persistent<A: EventSourcedActor>(&self, actor: A) -> ActorRef<A::Command> {
        spawn_persistent_in(actor, self.inner.clone())
    }

    /// Register an actor type so instances of it can be reached by id.
    ///
    /// Called once per type at startup. `deps` is cloned into every instance.
    pub fn register<A: ClusterActor>(&self, deps: A::Deps) {
        let factory: Factory = Arc::new(move |id: &str, system: &ActorSystem| {
            Arc::new(A::spawn(id, deps.clone(), system)) as ErasedRef
        });
        self.inner.factories.lock().insert(A::KIND, factory);
    }

    /// The instance of `A` with this id, starting it if it is not running.
    ///
    /// Idempotent by construction: two concurrent callers get one instance, not
    /// two. That matters most for event-sourced actors, where two instances on
    /// one id means two actors writing one journal.
    pub fn actor_of<A: ClusterActor>(
        &self,
        id: &str,
    ) -> Result<ActorRef<A::Command>, ActorOfError> {
        let key = (A::KIND, id.to_owned());

        // One lock across the check and the insert. Two callers racing here is
        // exactly the case this exists to prevent, so the factory runs under it.
        let mut live = self.inner.live.lock();
        if let Some(existing) = live.get(&key) {
            return downcast::<A>(existing);
        }

        let factory = self
            .inner
            .factories
            .lock()
            .get(A::KIND)
            .cloned()
            .ok_or(ActorOfError::NotRegistered(A::KIND))?;

        let erased = factory(id, self);
        let typed = downcast::<A>(&erased)?;
        live.insert(key, erased);
        Ok(typed)
    }
}

/// Recover the concrete reference from a registry slot.
///
/// The downcast can only fail if two actor types share a `KIND`, since `KIND` is
/// the registry key. That is a wiring mistake worth naming rather than a panic.
fn downcast<A: ClusterActor>(erased: &ErasedRef) -> Result<ActorRef<A::Command>, ActorOfError> {
    erased
        .downcast_ref::<ActorRef<A::Command>>()
        .cloned()
        .ok_or(ActorOfError::KindCollision(A::KIND))
}

pub(crate) fn spawn_in<A: Actor>(actor: A, inner: Arc<SystemInner>) -> ActorRef<A::Command> {
    let (tx, rx) = tokio::sync::mpsc::channel(MAILBOX_CAPACITY);
    let ctx = ActorContext {
        inner,
        self_tx: tx.clone(),
    };
    tokio::spawn(run_actor(actor, rx, ctx));
    ActorRef::new(tx)
}

pub(crate) fn spawn_persistent_in<A: EventSourcedActor>(
    actor: A,
    inner: Arc<SystemInner>,
) -> ActorRef<A::Command> {
    let journal = inner.journal.clone();
    spawn_in(Persistent::new(actor, journal), inner)
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
    use crate::actor::CommandEffect;
    use crate::persistence_id::PersistenceId;
    use crate::reply::ReplyTo;
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    enum CounterCmd {
        Inc(i64),
        #[serde(skip)]
        Get(Option<ReplyTo<i64>>),
    }

    struct Counter {
        id: String,
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

        // An event-sourced registered type spawns itself persistently. A
        // stateless one would call `system.spawn` here instead — being
        // registered and being event-sourced are independent.
        fn spawn(id: &str, _deps: (), system: &ActorSystem) -> ActorRef<CounterCmd> {
            system.spawn_persistent(Counter { id: id.to_owned() })
        }
    }

    async fn current_value(actor: &ActorRef<CounterCmd>) -> i64 {
        actor
            .ask(|reply| CounterCmd::Get(Some(reply)))
            .await
            .unwrap()
    }

    /// Two callers asking for the same (kind, id) get one actor, not two.
    ///
    /// This is the single-instance guarantee the cluster layer later extends
    /// across nodes, and it is exactly the hazard a caller otherwise works
    /// around by hand: two concurrent first requests spawning two event-sourced
    /// actors onto one journal.
    #[tokio::test]
    async fn actor_of_is_idempotent_for_one_id() {
        let system = ActorSystem::in_memory();
        system.register::<Counter>(());
        let a = system.actor_of::<Counter>("c1").unwrap();
        let b = system.actor_of::<Counter>("c1").unwrap();
        a.tell(CounterCmd::Inc(2)).await.unwrap();
        b.tell(CounterCmd::Inc(3)).await.unwrap();
        // One actor, one journal, one folded total — not two actors at 2 and 3.
        assert_eq!(current_value(&a).await, 5);
    }

    /// Different ids are different instances with independent journals.
    #[tokio::test]
    async fn different_ids_are_different_instances() {
        let system = ActorSystem::in_memory();
        system.register::<Counter>(());
        let a = system.actor_of::<Counter>("a").unwrap();
        let b = system.actor_of::<Counter>("b").unwrap();
        a.tell(CounterCmd::Inc(2)).await.unwrap();
        b.tell(CounterCmd::Inc(3)).await.unwrap();
        assert_eq!(current_value(&a).await, 2);
        assert_eq!(current_value(&b).await, 3);
    }

    /// Asking for a kind nobody registered is a named error, not a panic and
    /// not a silently spawned actor with no wiring.
    #[tokio::test]
    async fn actor_of_reports_an_unregistered_kind() {
        let system = ActorSystem::in_memory();
        let err = system.actor_of::<Counter>("c1").unwrap_err();
        assert!(matches!(err, ActorOfError::NotRegistered("counter")));
    }

    /// Two types sharing a KIND collide in the registry. The second
    /// registration wins the slot, so a lookup for the first gets a reference
    /// of the wrong command type — reported rather than downcast-panicked.
    #[tokio::test]
    async fn colliding_kinds_are_reported() {
        struct Impostor;
        #[derive(Serialize, Deserialize)]
        struct OtherCmd;

        #[async_trait]
        impl Actor for Impostor {
            type Command = OtherCmd;
            async fn handle(
                &mut self,
                _cmd: OtherCmd,
                _ctx: &mut ActorContext<OtherCmd>,
            ) -> crate::behaviour::Flow {
                crate::behaviour::Flow::Continue
            }
        }

        impl ClusterActor for Impostor {
            // Same KIND as Counter — the mistake under test.
            const KIND: &'static str = "counter";
            type Command = OtherCmd;
            type Deps = ();
            fn spawn(_id: &str, _deps: (), system: &ActorSystem) -> ActorRef<OtherCmd> {
                system.spawn(Impostor)
            }
        }

        let system = ActorSystem::in_memory();
        system.register::<Counter>(());
        system.register::<Impostor>(());
        let err = system.actor_of::<Counter>("c1").unwrap_err();
        assert!(matches!(err, ActorOfError::KindCollision("counter")));
    }
}
