use crate::actor::EventSourcedActor;
use crate::behaviour::Actor;
use crate::cluster::{ClusterNode, Dedup};
use crate::envelope::Message;
use crate::error::TellError;
use crate::journal::{InMemoryJournal, Journal};
use crate::path::ActorPath;
use crate::persistent::Persistent;
use crate::runtime::{ActorRef, Link, check_name, spawn_at};
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
/// [`ActorSystem::spawn_at`] for a plain actor or
/// [`ActorSystem::spawn_persistent_at`] for an event-sourced one, so being
/// cluster-hostable and being event-sourced stay independent.
///
/// Nothing here mentions persistence, and that is deliberate. A singleton with
/// no event log is an ordinary member of this trait; an earlier version required
/// every registered type to declare a `persistence_id` so the system could claim
/// a log before hosting, which forced a journal on types that had no state to
/// keep. The write fence now lives entirely inside the event-sourcing layer, so
/// the cluster layer no longer has to know that journals exist.
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

    /// Build and spawn the instance for `id`, at `path`.
    ///
    /// This is what a host that never executed the original request calls, which
    /// is why it takes an id and deps rather than a constructed actor.
    ///
    /// Registering the result is the registry's job, not this one's — spawn with
    /// [`ActorSystem::spawn_at`] or [`ActorSystem::spawn_persistent_at`] and hand
    /// the reference back.
    fn spawn(
        id: &str,
        deps: Self::Deps,
        system: &ActorSystem,
        path: ActorPath,
    ) -> ActorRef<Self::Command>;
}

/// Where a registered singleton sits while the cluster layer still addresses by
/// `(kind, id)`.
///
/// A bridge, and a short-lived one: clustering becomes a property of a path
/// rather than of a registration, at which point instances live wherever their
/// parents put them and this disappears along with `KIND`.
fn singleton_path<A: ClusterActor>(id: &str) -> ActorPath {
    ActorPath::root().child(A::KIND).child(id)
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
    Resolve(#[from] ActorOfError),

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

    /// The name could not be one path segment — it was empty, or it contained
    /// the separator, which would make one actor's path ambiguous with another's.
    #[error("'{0}' is not a usable actor name")]
    InvalidName(String),

    /// An actor of a different command type already lives at this path. Two
    /// actors cannot share a name, and handing the caller the one that is there
    /// would give it a reference of the wrong type.
    #[error("an actor of another type already lives at '{0}'")]
    PathTaken(ActorPath),

    /// This node cannot see a quorum, so it is not hosting anything.
    ///
    /// Refused rather than served. A node in a minority cannot know whether its
    /// instances have already been given to somebody else, and answering from
    /// state that may be history is the one failure the write fence cannot
    /// catch — because a read never writes.
    #[error("this node has no quorum and is not serving")]
    NotServing,
}

/// How many recent message ids a node remembers. Large enough that a retry
/// storm cannot push a legitimate repeat out of the window, small enough to be
/// free.
const DEDUP_WINDOW: usize = 4096;

/// A live instance's `ActorRef`, with its command type erased so refs of
/// different kinds can share one registry.
type ErasedRef = Arc<dyn Any + Send + Sync>;

/// Builds an instance of some registered kind and returns a type-erased ref.
type Factory = Arc<dyn Fn(&str, &ActorSystem, ActorPath) -> ErasedRef + Send + Sync>;

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
    /// Every actor on this node, keyed by its path.
    ///
    /// One flat map, not a tree of per-actor child maps. A lookup is one step,
    /// and "everything under `/acct-7/session-3`" is a prefix scan. A tree is the
    /// obvious shape and the wrong one: it would record which actors exist a
    /// second time, alongside this, and two records of one fact drifting apart is
    /// what generated the ownership bugs this design set out to remove.
    live: Mutex<HashMap<ActorPath, ErasedRef>>,
    /// Raised when this node stops serving. Every actor watches it and stops.
    ///
    /// A single-node system holds a sender that is never used, so the signal
    /// never fires and nothing pays for it.
    pub(crate) stand_down: tokio::sync::watch::Receiver<bool>,
    /// Kept alive so the receiver above never reports its sender dropped.
    _stand_down_tx: tokio::sync::watch::Sender<bool>,
}

impl SystemInner {
    /// How to reach whatever is at `path` right now.
    ///
    /// `None` when the path holds nothing, or holds an actor that has stopped —
    /// the two are the same answer to a caller, and neither is a reason to start
    /// anything. Resolution never creates: a reference that woke an actor up
    /// would break the rule that reading a session must not load it.
    pub(crate) fn resolve<C: Send + 'static>(&self, path: &ActorPath) -> Option<Link<C>> {
        let live = self.live.lock();
        let link = live.get(path)?.downcast_ref::<ActorRef<C>>()?.cached()?;
        link.is_alive().then_some(link)
    }
}

/// The runtime an actor tree lives in: its journal, its registered actor types,
/// and the instances currently hosted.
#[derive(Clone)]
pub struct ActorSystem {
    inner: Arc<SystemInner>,
}

impl ActorSystem {
    /// The system an actor is running in, from inside that actor.
    ///
    /// Cheap — the system *is* this handle — and the reason `ActorContext` can
    /// offer everything the system does without carrying a second copy of it.
    pub(crate) fn from_inner(inner: Arc<SystemInner>) -> Self {
        Self { inner }
    }

    /// A system backed by `journal`.
    #[must_use]
    pub fn new(journal: Arc<dyn Journal>) -> Self {
        let (tx, rx) = tokio::sync::watch::channel(false);
        Self {
            inner: Arc::new(SystemInner {
                journal,
                factories: Mutex::new(HashMap::new()),
                deliverers: Mutex::new(HashMap::new()),
                cluster: None,
                seen: Mutex::new(Dedup::with_capacity(DEDUP_WINDOW)),
                live: Mutex::new(HashMap::new()),
                stand_down: rx,
                _stand_down_tx: tx,
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
        // Inverted: actors want to know when to stop, and the node reports when
        // it may serve.
        //
        // Starts false even though a node that has not yet elected anybody is
        // not serving. The signal means "stop what you are doing", and at
        // construction there is nothing doing it — seeding it true would make
        // every actor spawned before the first election exit on its first poll,
        // silently. Refusing to *start* an instance is `require_serving`'s job,
        // and it is a separate question with a separate answer.
        let (tx, rx) = tokio::sync::watch::channel(false);
        let mut serving = cluster.serving_watch();
        let relay = tx.clone();
        tokio::spawn(async move {
            while serving.changed().await.is_ok() {
                let stop = !*serving.borrow_and_update();
                if relay.send(stop).is_err() {
                    return; // the system is gone
                }
            }
        });

        Self {
            inner: Arc::new(SystemInner {
                journal,
                factories: Mutex::new(HashMap::new()),
                deliverers: Mutex::new(HashMap::new()),
                cluster: Some(cluster),
                seen: Mutex::new(Dedup::with_capacity(DEDUP_WINDOW)),
                live: Mutex::new(HashMap::new()),
                stand_down: rx,
                _stand_down_tx: tx,
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

    /// The top-level actor named `name`, creating it from `actor` if it is not
    /// there.
    ///
    /// A top-level actor is a child of root, so this is `/name`. Get-or-create:
    /// two callers naming one path get one actor, and the loser's `actor` is
    /// dropped without ever being started.
    ///
    /// # Errors
    /// If `name` is not a usable path segment, or an actor of another command
    /// type already lives at that path.
    pub fn actor_of<A: Actor>(
        &self,
        name: &str,
        actor: A,
    ) -> Result<ActorRef<A::Command>, ActorOfError> {
        self.get_or_create(&ActorPath::root(), name, actor)
    }

    /// The top-level event-sourced actor named `name`, creating it from `actor`
    /// if it is not there. It recovers from its own `persistence_id` before
    /// accepting commands.
    ///
    /// # Errors
    /// As [`actor_of`](Self::actor_of).
    pub fn actor_of_persistent<A: EventSourcedActor>(
        &self,
        name: &str,
        actor: A,
    ) -> Result<ActorRef<A::Command>, ActorOfError> {
        self.get_or_create(
            &ActorPath::root(),
            name,
            Persistent::new(actor, self.inner.journal.clone()),
        )
    }

    /// Get-or-create the child of `parent` named `name`.
    ///
    /// The whole map operation happens under one lock, so a race has no window
    /// to spawn a second actor in: the loser returns what is already there and
    /// its `actor` is dropped, never started. Two instances at one path means, for
    /// an event-sourced pair, two actors writing one journal.
    pub(crate) fn get_or_create<A: Actor>(
        &self,
        parent: &ActorPath,
        name: &str,
        actor: A,
    ) -> Result<ActorRef<A::Command>, ActorOfError> {
        self.require_serving()?;
        check_name(name)?;
        let path = parent.child(name);

        let mut live = self.inner.live.lock();
        if let Some(existing) = live.get(&path) {
            let cached = existing
                .downcast_ref::<ActorRef<A::Command>>()
                .ok_or_else(|| ActorOfError::PathTaken(path.clone()))?
                // Read through to the link rather than asking the stored
                // reference whether it is alive: that would re-resolve, and
                // re-resolving takes this very lock.
                .cached();
            // A stopped actor stays in the map until somebody asks for this
            // name again. Handing it back would be worse than a miss: every send
            // to it fails and the instance it stood down for never starts.
            if let Some(link) = cached.filter(Link::is_alive) {
                return Ok(self.reference(path, Some(link)));
            }
            live.remove(&path);
        }

        let link = spawn_at(actor, self.inner.clone(), path.clone());
        let entry = self.reference(path.clone(), Some(link.clone()));
        live.insert(path.clone(), Arc::new(entry) as ErasedRef);
        Ok(self.reference(path, Some(link)))
    }

    /// A reference to `path` with `link` as its starting cache.
    fn reference<C: Send + 'static>(&self, path: ActorPath, link: Option<Link<C>>) -> ActorRef<C> {
        ActorRef::at(path, link, Arc::downgrade(&self.inner))
    }

    /// Start an actor at `path` without registering it.
    ///
    /// The escape hatch a [`ClusterActor::spawn`] implementation uses: the
    /// registry decides what lives at a path, so a spawn that also registered
    /// would give that fact a second owner.
    pub fn spawn_at<A: Actor>(&self, path: ActorPath, actor: A) -> ActorRef<A::Command> {
        let link = spawn_at(actor, self.inner.clone(), path.clone());
        self.reference(path, Some(link))
    }

    /// Start an event-sourced actor at `path` without registering it. It recovers
    /// from its own `persistence_id` before accepting commands.
    pub fn spawn_persistent_at<A: EventSourcedActor>(
        &self,
        path: ActorPath,
        actor: A,
    ) -> ActorRef<A::Command> {
        self.spawn_at(path, Persistent::new(actor, self.inner.journal.clone()))
    }

    /// Register an actor type so instances of it can be reached by id.
    ///
    /// Called once per type at startup. `deps` is cloned into every instance.
    pub fn register<A: ClusterActor>(&self, deps: A::Deps) {
        let factory: Factory = Arc::new(move |id: &str, system: &ActorSystem, path: ActorPath| {
            Arc::new(A::spawn(id, deps.clone(), system, path)) as ErasedRef
        });
        self.inner.factories.lock().insert(A::KIND, factory);

        let deliver: Deliver = Arc::new(|id, payload, system| {
            Box::pin(async move {
                // Decoded inside the router context so a `ReplyTo` in the
                // command comes back knowing how to answer whoever asked. Out
                // of context it decodes to an error instead, which is the
                // difference between a failed request and a caller that waits
                // forever.
                let cmd: A::Command = match system.cluster() {
                    Some(cluster) => {
                        let router: Arc<dyn crate::reply::ReplyRouter> = cluster.clone();
                        crate::reply::with_router(router, || serde_json::from_slice(&payload))
                    }
                    None => serde_json::from_slice(&payload),
                }
                .map_err(|e| DispatchError::Decode(e.to_string()))?;
                let target = system.local_instance::<A>(&id)?;
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
    /// Used by the dispatch loop, which has already been told this node is the
    /// right place. Starting an instance takes no lock on anything shared: an
    /// event-sourced one recovers, and its first write is conditional on the log
    /// still ending where recovery left it, so a second host starting the same
    /// instance is caught by that write rather than by a claim taken here.
    pub fn local_instance<A: ClusterActor>(
        &self,
        id: &str,
    ) -> Result<ActorRef<A::Command>, ActorOfError> {
        self.require_serving()?;
        let path = singleton_path::<A>(id);
        let factory = self
            .inner
            .factories
            .lock()
            .get(A::KIND)
            .cloned()
            .ok_or(ActorOfError::NotRegistered(A::KIND))?;

        let mut live = self.inner.live.lock();
        if let Some(existing) = live.get(&path) {
            let cached = downcast::<A>(existing)?.cached();
            // A stopped instance stays in the map until somebody asks for it
            // again — there is no lifecycle callback to evict it, and polling
            // for corpses would cost more than checking here. Handing this one
            // back would be worse than a miss: every `tell` to it fails, and the
            // instance it stood down for never gets started.
            if let Some(link) = cached.filter(Link::is_alive) {
                return Ok(self.reference(path, Some(link)));
            }
            live.remove(&path);
        }
        let erased = factory(id, self, path.clone());
        let typed = downcast::<A>(&erased)?;
        live.insert(path.clone(), erased);
        if let Some(cluster) = &self.inner.cluster {
            cluster.record_local_assignment(A::KIND, id);
        }
        Ok(self.reference(path, typed.cached()))
    }

    /// Refuse everything while this node has no quorum.
    fn require_serving(&self) -> Result<(), ActorOfError> {
        match &self.inner.cluster {
            Some(cluster) if !cluster.serving() => Err(ActorOfError::NotServing),
            _ => Ok(()),
        }
    }

    /// Feed one inbound message to whoever it is for.
    pub async fn dispatch(&self, message: Message) -> Result<(), DispatchError> {
        self.require_serving()?;
        let env = match message {
            Message::Command(env) => env,
            Message::Reply(reply) => {
                // An answer is for a caller, not an actor, so it never goes
                // near the registry, the dedup window or placement.
                if let Some(cluster) = &self.inner.cluster {
                    cluster.deliver_reply(reply);
                }
                return Ok(());
            }
        };
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

    /// The instance of a registered type `A` with this id, starting it if it is
    /// not running.
    ///
    /// Idempotent by construction: two concurrent callers get one instance, not
    /// two. That matters most for event-sourced actors, where two instances on
    /// one id means two actors writing one journal.
    ///
    /// The `(kind, id)` addressing the cluster layer still uses. Superseded by
    /// paths — a clustered actor becomes an ordinary path that happens to resolve
    /// to another node — but that is the next step, not this one.
    pub async fn singleton_of<A: ClusterActor>(
        &self,
        id: &str,
    ) -> Result<ActorRef<A::Command>, ActorOfError> {
        self.require_serving()?;
        let path = singleton_path::<A>(id);

        // Already running here: hand back the same reference regardless of what
        // placement now says. Migrating a live instance mid-conversation would
        // strand whatever it was doing, and the conditional append makes a brief
        // overlap survivable anyway.
        if let Some(link) = self.inner.resolve::<A::Command>(&path) {
            return Ok(self.reference(path, Some(link)));
        }

        if let Some(cluster) = &self.inner.cluster
            && !cluster.owns(A::KIND, id)
        {
            return Ok(self.remote_ref::<A>(id, cluster.clone()));
        }

        self.local_instance::<A>(id)
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
        let path = singleton_path::<A>(id);
        let id = id.to_owned();
        let send: crate::runtime::RemoteSend<A::Command> = Arc::new(move |cmd: A::Command| {
            let cluster = cluster.clone();
            let id = id.clone();
            Box::pin(async move {
                // Encoded inside the router context, which is what registers any
                // reply handle in the command against this node before it
                // leaves. Encoding it outside is a loud error rather than a
                // handle addressed to nobody.
                let router: Arc<dyn crate::reply::ReplyRouter> = cluster.clone();
                let payload = crate::reply::with_router(router, || serde_json::to_vec(&cmd))
                    .map_err(|_| TellError::Undeliverable)?;
                cluster
                    .send(kind, &id, payload, cluster.next_message_id())
                    .await
                    .map_err(|_| TellError::Undeliverable)
            })
        });
        self.reference(path, Some(Link::Remote(send)))
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
    use crate::behaviour::{Flow, Root};
    use crate::persistence_id::PersistenceId;
    use crate::reply::ReplyTo;
    use crate::runtime::ActorContext;
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    enum CounterCmd {
        Inc(i64),
        Get(ReplyTo<i64>),
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
                CounterCmd::Inc(n) => CommandEffect::persist(vec![Incremented(n)]),
                CounterCmd::Get(reply) => {
                    let _ = reply.send(state.value);
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
        fn spawn(
            id: &str,
            _deps: (),
            system: &ActorSystem,
            path: ActorPath,
        ) -> ActorRef<CounterCmd> {
            system.spawn_persistent_at(path, Counter { id: id.to_owned() })
        }
    }

    async fn current_value(actor: &ActorRef<CounterCmd>) -> i64 {
        actor.ask(CounterCmd::Get).await.unwrap()
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
        let a = system.singleton_of::<Counter>("c1").await.unwrap();
        let b = system.singleton_of::<Counter>("c1").await.unwrap();
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
        let a = system.singleton_of::<Counter>("a").await.unwrap();
        let b = system.singleton_of::<Counter>("b").await.unwrap();
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
        let err = system.singleton_of::<Counter>("c1").await.unwrap_err();
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
            type ParentCommand = Root;
            async fn handle(&mut self, _cmd: OtherCmd, _ctx: &mut ActorContext<OtherCmd>) -> Flow {
                Flow::Continue
            }
        }

        impl ClusterActor for Impostor {
            // Same KIND as Counter — the mistake under test.
            const KIND: &'static str = "counter";
            type Command = OtherCmd;
            type Deps = ();

            // Stateless: no journal anywhere in sight, which is the point —
            // being registered for cluster hosting says nothing about
            // persistence.
            fn spawn(
                _id: &str,
                _deps: (),
                system: &ActorSystem,
                path: ActorPath,
            ) -> ActorRef<OtherCmd> {
                system.spawn_at(path, Impostor)
            }
        }

        let system = ActorSystem::in_memory();
        system.register::<Counter>(());
        system.register::<Impostor>(());
        let err = system.singleton_of::<Counter>("c1").await.unwrap_err();
        assert!(matches!(err, ActorOfError::KindCollision("counter")));
    }
}
