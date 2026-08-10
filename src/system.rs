use crate::actor::EventSourcedActor;
use crate::behaviour::Actor;
use crate::cluster::{ClusterNode, Dedup};
use crate::envelope::{Envelope, Epoch};
use crate::error::{JournalError, TellError};
use crate::journal::{InMemoryJournal, Journal};
use crate::persistence_id::PersistenceId;
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

    /// The log this instance's state lives in, when it has one.
    ///
    /// `Some` makes the instance fenceable: the host claims this log before
    /// serving, which mints an epoch and locks out whoever held it before.
    /// `None` is a stateless type — nothing to fence, because nothing is
    /// written.
    ///
    /// Deliberately has no default. A default of `None` would mean an
    /// event-sourced actor that forgot to override it silently ran unfenced,
    /// and the symptom of that is two hosts merging into one history — the one
    /// failure this whole mechanism exists to prevent. Writing `None` by hand
    /// is a claim that the type is stateless.
    fn persistence_id(id: &str) -> Option<PersistenceId>;

    /// Build and spawn the instance for `id`.
    ///
    /// This is what a host that never executed the original request calls, which
    /// is why it takes an id and deps rather than a constructed actor.
    ///
    /// `fence` is the generation this host claimed the log at. An event-sourced
    /// type must pass it to [`ActorSystem::spawn_fenced`], or its writes carry
    /// no claim and a host that lost the instance keeps writing to it. `None`
    /// means nothing is arbitrating ownership.
    fn spawn(
        id: &str,
        deps: Self::Deps,
        system: &ActorSystem,
        fence: Option<Epoch>,
    ) -> ActorRef<Self::Command>;
}

/// Why an inbound envelope could not be delivered.
#[derive(Debug, Error)]
pub enum DispatchError {
    /// No actor type is registered under this kind here. Two nodes running
    /// different builds is the usual cause.
    #[error("no actor type is registered under the kind '{0}'")]
    UnknownKind(String),

    /// The payload did not decode into the registered command type — again,
    /// usually a version skew between nodes.
    #[error("could not decode the command: {0}")]
    Decode(String),

    /// The instance could not be started here.
    #[error(transparent)]
    Resolve(ActorOfError),

    /// The instance stopped between being resolved and being told.
    #[error("the instance stopped before the message was delivered")]
    MailboxClosed,
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

    /// The log could not be claimed, so hosting would mean writing without a
    /// generation — which is how two hosts end up merged into one history.
    #[error("could not claim ownership: {0}")]
    Claim(String),
}

/// How many recent message ids a node remembers. Large enough that a retry
/// storm cannot push a legitimate repeat out of the window, small enough to be
/// free.
const DEDUP_WINDOW: usize = 4096;

/// A live instance's `ActorRef`, with its command type erased so refs of
/// different kinds can share one registry.
type ErasedRef = Arc<dyn Any + Send + Sync>;

/// Builds an instance of some registered kind and returns a type-erased ref.
type Factory = Arc<dyn Fn(&str, &ActorSystem, Option<Epoch>) -> ErasedRef + Send + Sync>;

/// Decodes an inbound payload and hands it to the local instance.
///
/// Type-erased for the same reason the factory is: the dispatch loop knows a
/// kind string, not a Rust type.
type Deliver = Arc<
    dyn Fn(
            String,
            Vec<u8>,
            ActorSystem,
        ) -> futures_util::future::BoxFuture<'static, Result<(), DispatchError>>
        + Send
        + Sync,
>;

/// Process-wide state shared by every actor in a system.
pub(crate) struct SystemInner {
    pub(crate) journal: Arc<dyn Journal>,
    factories: Mutex<HashMap<&'static str, Factory>>,
    deliverers: Mutex<HashMap<&'static str, Deliver>>,
    cluster: Option<Arc<ClusterNode>>,
    /// Message ids this node has already handled.
    ///
    /// Retries make delivery at-least-once; this is what makes *processing*
    /// once. Node-scoped rather than per-actor, which is the honest limit of it:
    /// a repeat arriving after this host restarted is not recognised, so a
    /// command must still be one whose second application is survivable. Moving
    /// this into each actor's own event-sourced state is what would close that,
    /// and it is not done yet.
    seen: Mutex<Dedup>,
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
                deliverers: Mutex::new(HashMap::new()),
                cluster: None,
                seen: Mutex::new(Dedup::with_capacity(DEDUP_WINDOW)),
                live: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// A system that hosts registered actors across a cluster.
    ///
    /// Instances resolve to a node; the ones this node owns run here, and the
    /// rest are reached through the transport. Business logic sees no
    /// difference — `actor_of` returns an `ActorRef` either way.
    #[must_use]
    pub fn clustered(journal: Arc<dyn Journal>, cluster: Arc<ClusterNode>) -> Self {
        Self {
            inner: Arc::new(SystemInner {
                journal,
                factories: Mutex::new(HashMap::new()),
                deliverers: Mutex::new(HashMap::new()),
                cluster: Some(cluster),
                seen: Mutex::new(Dedup::with_capacity(DEDUP_WINDOW)),
                live: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// This node's cluster, if it is in one.
    #[must_use]
    pub fn cluster(&self) -> Option<&Arc<ClusterNode>> {
        self.inner.cluster.as_ref()
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
        let factory: Factory = Arc::new(move |id: &str, system: &ActorSystem, fence| {
            Arc::new(A::spawn(id, deps.clone(), system, fence)) as ErasedRef
        });
        self.inner.factories.lock().insert(A::KIND, factory);

        let deliver: Deliver = Arc::new(|id, payload, system| {
            Box::pin(async move {
                let cmd: A::Command = serde_json::from_slice(&payload)
                    .map_err(|e| DispatchError::Decode(e.to_string()))?;
                let target = system
                    .local_instance::<A>(&id)
                    .await
                    .map_err(DispatchError::Resolve)?;
                target
                    .tell(cmd)
                    .await
                    .map_err(|_| DispatchError::MailboxClosed)
            })
        });
        self.inner.deliverers.lock().insert(A::KIND, deliver);
    }

    /// Start, or return, the instance hosted *here* — no cluster resolution.
    ///
    /// Claims the log first when the type has one, so this host's writes carry a
    /// generation above whatever held it before. Used by the dispatch loop,
    /// which has already been told this node is the right place.
    pub async fn local_instance<A: ClusterActor>(
        &self,
        id: &str,
    ) -> Result<ActorRef<A::Command>, ActorOfError> {
        let key = (A::KIND, id.to_owned());
        {
            let live = self.inner.live.lock();
            if let Some(existing) = live.get(&key) {
                return downcast::<A>(existing);
            }
        }

        // Claim before spawning, so the instance's very first write already
        // outranks whatever held the log before. Claiming after would leave a
        // window where two hosts write under the same generation.
        let fence = self
            .claim::<A>(id)
            .await
            .map_err(|e| ActorOfError::Claim(e.to_string()))?;

        let factory = self
            .inner
            .factories
            .lock()
            .get(A::KIND)
            .cloned()
            .ok_or(ActorOfError::NotRegistered(A::KIND))?;

        let mut live = self.inner.live.lock();
        // Somebody else won the race while we were claiming. Theirs has the
        // higher epoch, so ours would be fenced anyway — use theirs.
        if let Some(existing) = live.get(&key) {
            return downcast::<A>(existing);
        }
        let erased = factory(id, self, fence);
        let typed = downcast::<A>(&erased)?;
        live.insert(key, erased);
        if let Some(cluster) = &self.inner.cluster {
            cluster.record_local_assignment(A::KIND, id);
        }
        Ok(typed)
    }

    /// Claim the log for `(A::KIND, id)`, returning the epoch to write under.
    ///
    /// `None` when the type is stateless or there is no cluster — nothing is
    /// arbitrating ownership, so nothing needs fencing.
    pub async fn claim<A: ClusterActor>(&self, id: &str) -> Result<Option<Epoch>, JournalError> {
        if self.inner.cluster.is_none() {
            return Ok(None);
        }
        let Some(pid) = A::persistence_id(id) else {
            return Ok(None);
        };
        self.inner.journal.claim_ownership(&pid).await.map(Some)
    }

    /// Spawn an event-sourced actor whose writes carry `fence`.
    pub fn spawn_fenced<A: EventSourcedActor>(
        &self,
        actor: A,
        fence: Option<Epoch>,
    ) -> ActorRef<A::Command> {
        spawn_fenced_in(actor, self.inner.clone(), fence)
    }

    /// Feed one inbound envelope to the instance it addresses.
    pub async fn dispatch(&self, env: Envelope) -> Result<(), DispatchError> {
        // A repeat is a success, not a failure: the sender retried because it
        // could not tell "lost" from "slow", and the answer to both is that the
        // command has already been applied.
        if !self.inner.seen.lock().accept(&env) {
            tracing::debug!(kind = %env.kind, id = %env.id, "dropped a duplicate delivery");
            return Ok(());
        }
        let deliver = self
            .inner
            .deliverers
            .lock()
            .get(env.kind.as_str())
            .cloned()
            .ok_or_else(|| DispatchError::UnknownKind(env.kind.clone()))?;
        deliver(env.id, env.payload, self.clone()).await
    }

    /// The instance of `A` with this id, starting it if it is not running.
    ///
    /// Idempotent by construction: two concurrent callers get one instance, not
    /// two. That matters most for event-sourced actors, where two instances on
    /// one id means two actors writing one journal.
    pub async fn actor_of<A: ClusterActor>(
        &self,
        id: &str,
    ) -> Result<ActorRef<A::Command>, ActorOfError> {
        // Already running here: hand back the same reference regardless of what
        // placement now says. Migrating a live instance mid-conversation would
        // strand whatever it was doing, and the fence makes a brief overlap
        // survivable anyway.
        {
            let live = self.inner.live.lock();
            if let Some(existing) = live.get(&(A::KIND, id.to_owned())) {
                return downcast::<A>(existing);
            }
        }

        if let Some(cluster) = &self.inner.cluster
            && !cluster.owns(A::KIND, id)
        {
            return Ok(self.remote_ref::<A>(id, cluster.clone()));
        }

        self.local_instance::<A>(id).await
    }

    /// A reference that encodes commands and ships them to the hosting node.
    ///
    /// Indistinguishable from a local one at the call site: same type, same
    /// `tell`, same `ask`. The encoder closure is built here, where
    /// `A::Command: Serialize` is known, which is what keeps the serde bound
    /// off `ActorRef` itself.
    fn remote_ref<A: ClusterActor>(
        &self,
        id: &str,
        cluster: Arc<ClusterNode>,
    ) -> ActorRef<A::Command> {
        let kind = A::KIND;
        let id = id.to_owned();
        ActorRef::remote(Arc::new(move |cmd: A::Command| {
            let cluster = cluster.clone();
            let id = id.clone();
            Box::pin(async move {
                let payload = serde_json::to_vec(&cmd).map_err(|_| TellError::Undeliverable)?;
                // The sender does not know the host's generation, so it asserts
                // nothing; the host's own claim is what fences its writes.
                cluster
                    .send(kind, &id, Epoch(0), payload, cluster.next_message_id())
                    .await
                    .map_err(|_| TellError::Undeliverable)
            })
        }))
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
    spawn_fenced_in(actor, inner, None)
}

pub(crate) fn spawn_fenced_in<A: EventSourcedActor>(
    actor: A,
    inner: Arc<SystemInner>,
    fence: Option<Epoch>,
) -> ActorRef<A::Command> {
    let journal = inner.journal.clone();
    spawn_in(Persistent::fenced(actor, journal, fence), inner)
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

        fn persistence_id(id: &str) -> Option<PersistenceId> {
            Some(PersistenceId::new("counter", id))
        }

        // An event-sourced registered type spawns itself persistently. A
        // stateless one would call `system.spawn` here instead — being
        // registered and being event-sourced are independent.
        fn spawn(
            id: &str,
            _deps: (),
            system: &ActorSystem,
            fence: Option<Epoch>,
        ) -> ActorRef<CounterCmd> {
            system.spawn_fenced(Counter { id: id.to_owned() }, fence)
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
        let a = system.actor_of::<Counter>("c1").await.unwrap();
        let b = system.actor_of::<Counter>("c1").await.unwrap();
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
        let a = system.actor_of::<Counter>("a").await.unwrap();
        let b = system.actor_of::<Counter>("b").await.unwrap();
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
        let err = system.actor_of::<Counter>("c1").await.unwrap_err();
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

            // Stateless: nothing written, so nothing to fence.
            fn persistence_id(_id: &str) -> Option<PersistenceId> {
                None
            }
            fn spawn(
                _id: &str,
                _deps: (),
                system: &ActorSystem,
                _fence: Option<Epoch>,
            ) -> ActorRef<OtherCmd> {
                system.spawn(Impostor)
            }
        }

        let system = ActorSystem::in_memory();
        system.register::<Counter>(());
        system.register::<Impostor>(());
        let err = system.actor_of::<Counter>("c1").await.unwrap_err();
        assert!(matches!(err, ActorOfError::KindCollision("counter")));
    }
}
