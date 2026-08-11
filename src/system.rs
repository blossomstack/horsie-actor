use crate::actor::EventSourcedActor;
use crate::address::SettingsTable;
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
use std::any::{Any, TypeId};
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

/// Read a bridge path back as the `(kind, id)` it stands for, or `None` if it is
/// an ordinary address that no factory could ever build.
fn singleton_parts(path: &ActorPath) -> Option<(&str, &str)> {
    match path.segments() {
        [kind, id] => Some((kind.as_str(), id.as_str())),
        _ => None,
    }
}

/// Build the closure that decodes for one actor and hands it the command.
///
/// Takes the reference rather than the path, so delivery cannot resolve to a
/// different instance than the one this entry is for.
fn deliver_to<C: Send + 'static>(actor: ActorRef<C>, wire: Wire<C>) -> DeliverHere {
    Arc::new(move |payload, system| {
        let actor = actor.clone();
        let wire = wire.clone();
        Box::pin(async move {
            // Decoded inside the router context so a `ReplyTo` in the command
            // comes back knowing how to answer whoever asked. Out of context it
            // decodes to an error instead, which is the difference between a
            // failed request and a caller that waits forever.
            let cmd = match system.cluster() {
                Some(cluster) => {
                    let router: Arc<dyn crate::reply::ReplyRouter> = cluster.clone();
                    crate::reply::with_router(router, || (wire.decode)(&payload))
                }
                None => (wire.decode)(&payload),
            }
            .ok_or_else(|| DispatchError::Decode(actor.path().to_string()))?;
            actor
                .tell(cmd)
                .await
                .map_err(|_| DispatchError::MailboxClosed)
        })
    })
}

/// Why an inbound envelope could not be delivered.
#[derive(Debug, Error)]
pub enum DispatchError {
    /// Nothing is at this address here, and nothing here knows how to make it.
    /// Two nodes running different builds, or a placement decision that has
    /// moved since the sender resolved it, are the usual causes.
    #[error("no actor is at '{0}' on this node")]
    NoActor(ActorPath),

    /// The envelope's address is not a path. Only a corrupt or foreign sender
    /// produces this.
    #[error(transparent)]
    BadAddress(#[from] crate::path::InvalidPath),

    /// The actor is here, but its address is not clustered, so it has no wire
    /// format and nothing off this node was ever meant to reach it.
    ///
    /// Named separately from "nothing is there" because the two call for
    /// different fixes: this one is a configuration that does not match what the
    /// code is doing.
    #[error("the actor at '{0}' is local to this node and takes nothing from elsewhere")]
    LocalOnly(ActorPath),

    /// The payload did not decode into the command type the actor at that
    /// address accepts — usually a version skew between nodes.
    #[error("could not decode a command for '{0}'")]
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

    /// The address is configured as clustered, but this actor's commands cannot
    /// cross a host — so no node but this one could ever be told anything.
    ///
    /// Config chooses what is clustered; it cannot grant it. Paths appear at
    /// runtime, so this cannot be caught at boot, and being caught at creation
    /// is the next best thing: it names the path and the type, and it happens
    /// the first time rather than on some later send.
    #[error("'{path}' is configured as clustered, but {actor}'s commands do not round-trip")]
    NotClusterable {
        path: ActorPath,
        actor: &'static str,
    },

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
/// path, not a Rust type.
type Deliver = Arc<
    dyn Fn(
            String,
            Vec<u8>,
            ActorSystem,
        ) -> futures_util::future::BoxFuture<'static, Result<(), DispatchError>>
        + Send
        + Sync,
>;

/// Hands an inbound payload to the actor at one specific path.
///
/// Built where the command type is known and stored beside the reference, so
/// dispatch needs neither a type registry nor a kind on the wire — it has a
/// path, and the path has an entry.
type DeliverHere = Arc<
    dyn Fn(
            Vec<u8>,
            ActorSystem,
        ) -> futures_util::future::BoxFuture<'static, Result<(), DispatchError>>
        + Send
        + Sync,
>;

/// How a command type crosses a host.
///
/// Both halves, kept together and looked up by the command's [`TypeId`]: an
/// encoder for a send that leaves, and a decoder for one that arrives. Recorded
/// by registration, which is where the round-trip bound is already proved —
/// which is why creating a clustered actor can *check* that its commands encode
/// without demanding serde bounds from every caller that merely names the type.
struct Wire<C> {
    encode: Encode<C>,
    decode: Decode<C>,
}

/// Turns a command into bytes. `None` is a command that would not serialize,
/// which for a registered type means a reply handle encoded outside a router
/// context rather than a type-level mistake.
type Encode<C> = Arc<dyn Fn(&C) -> Option<Vec<u8>> + Send + Sync>;

/// Turns bytes back into a command. `None` is a payload that does not fit the
/// type the actor at this address accepts.
type Decode<C> = Arc<dyn Fn(&[u8]) -> Option<C> + Send + Sync>;

impl<C> Clone for Wire<C> {
    fn clone(&self) -> Self {
        Self {
            encode: self.encode.clone(),
            decode: self.decode.clone(),
        }
    }
}

/// A [`Wire`] with its command type erased, and the actor type name to blame in
/// an error.
type ErasedWire = (Arc<dyn Any + Send + Sync>, &'static str);

/// One actor on this node.
struct Entry {
    reference: ErasedRef,
    /// How to hand this actor a command that arrived from another node. `None`
    /// for a local-only actor, which is what makes a send from another host fail
    /// cleanly and say why.
    deliver: Option<DeliverHere>,
}

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
    live: Mutex<HashMap<ActorPath, Entry>>,
    /// Which addresses are clustered, read once at startup.
    ///
    /// Empty is the single-node case: nothing matches, every address takes the
    /// default, and the default is local — so the same binary serves both
    /// deployments and one of them mentions none of this.
    settings: SettingsTable,
    /// Command types that have been proved to round-trip, by the command's
    /// [`TypeId`]. Config *chooses* what is clustered; this is what says whether
    /// it *can* be.
    wires: Mutex<HashMap<TypeId, ErasedWire>>,
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
        if let Some(link) = self.resolve_local::<C>(path) {
            return Some(link);
        }
        // Not here. If the address is clustered it may be somewhere else, and
        // the caller must not be able to tell the difference.
        let cluster = self.cluster.as_ref()?;
        if !self.settings.at(path).clustered.value || cluster.owns(&path.to_string()) {
            return None;
        }
        self.remote_link::<C>(path, cluster.clone())
    }

    /// Whatever is running here at `path`.
    fn resolve_local<C: Send + 'static>(&self, path: &ActorPath) -> Option<Link<C>> {
        let live = self.live.lock();
        let link = live
            .get(path)?
            .reference
            .downcast_ref::<ActorRef<C>>()?
            .cached()?;
        link.is_alive().then_some(link)
    }

    /// A link that encodes commands and ships them to whichever node hosts
    /// `path`.
    ///
    /// It captures the *address*, never a host: the owner is resolved on every
    /// send. So a remote link survives a relocation on its own, without the
    /// reference holding it having to drop and re-resolve — which is why a
    /// remote send that fails is final rather than retried. Re-resolving would
    /// build the identical link.
    ///
    /// `None` for a command type nobody registered as clusterable. That cannot
    /// happen for an actor this system created — creation refuses a clustered
    /// path whose commands do not round-trip — so it only arises for a bare
    /// reference to a path nothing here ever made.
    fn remote_link<C: Send + 'static>(
        &self,
        path: &ActorPath,
        cluster: Arc<ClusterNode>,
    ) -> Option<Link<C>> {
        let wire = self.wire::<C>()?;
        let address = path.to_string();
        Some(Link::Remote(Arc::new(move |cmd: C| {
            let cluster = cluster.clone();
            let address = address.clone();
            let wire = wire.clone();
            Box::pin(async move {
                // Encoded inside the router context, which is what registers any
                // reply handle in the command against this node before it
                // leaves. Encoding it outside is a loud error rather than a
                // handle addressed to nobody.
                let router: Arc<dyn crate::reply::ReplyRouter> = cluster.clone();
                let payload = crate::reply::with_router(router, || (wire.encode)(&cmd))
                    .ok_or(TellError::Undeliverable)?;
                cluster
                    .send(&address, payload, cluster.next_message_id())
                    .await
                    .map_err(|_| TellError::Undeliverable)
            })
        })))
    }

    /// The registered wire format for `C`, if it has one.
    fn wire<C: Send + 'static>(&self) -> Option<Wire<C>> {
        self.wires
            .lock()
            .get(&TypeId::of::<C>())?
            .0
            .downcast_ref::<Wire<C>>()
            .cloned()
    }

    /// How to hand an inbound payload to whatever is at `path`.
    ///
    /// The outer `Option` is whether anything is there at all; the inner one is
    /// whether it can be reached from another node.
    fn deliver_here(&self, path: &ActorPath) -> Option<Option<DeliverHere>> {
        Some(self.live.lock().get(path)?.deliver.clone())
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
    ///
    /// No cluster and no settings, so every address is local — which is the
    /// single-node deployment, and it mentions none of the addressing config.
    #[must_use]
    pub fn new(journal: Arc<dyn Journal>) -> Self {
        Self::build(journal, None, SettingsTable::new())
    }

    /// The one place a system is assembled.
    ///
    /// `stand_down` starts false even for a clustered node that has not yet
    /// elected anybody. The signal means "stop what you are doing", and at
    /// construction there is nothing doing it — seeding it true would make every
    /// actor spawned before the first election exit on its first poll, silently.
    /// Refusing to *start* an instance is `require_serving`'s job, and it is a
    /// separate question with a separate answer.
    fn build(
        journal: Arc<dyn Journal>,
        cluster: Option<Arc<ClusterNode>>,
        settings: SettingsTable,
    ) -> Self {
        let (tx, rx) = tokio::sync::watch::channel(false);
        if let Some(cluster) = &cluster {
            // Inverted: actors want to know when to stop, and the node reports
            // when it may serve.
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
        }
        Self {
            inner: Arc::new(SystemInner {
                journal,
                factories: Mutex::new(HashMap::new()),
                deliverers: Mutex::new(HashMap::new()),
                cluster,
                seen: Mutex::new(Dedup::with_capacity(DEDUP_WINDOW)),
                live: Mutex::new(HashMap::new()),
                settings,
                wires: Mutex::new(HashMap::new()),
                stand_down: rx,
                _stand_down_tx: tx,
            }),
        }
    }

    /// A single-node system that still reads an addressing config.
    ///
    /// The same tree and the same settings, on one node — which is what makes
    /// "clustering an address changes nothing a caller can see" a thing a test
    /// can assert rather than a claim.
    #[must_use]
    pub fn with_settings(journal: Arc<dyn Journal>, settings: SettingsTable) -> Self {
        Self::build(journal, None, settings)
    }

    /// A system that hosts registered actors across a cluster.
    ///
    /// Instances resolve to a node; the ones this node owns run here, and the
    /// rest are reached through the transport. Business logic sees no
    /// difference — `actor_of` returns an `ActorRef` either way.
    #[must_use]
    pub fn clustered(
        journal: Arc<dyn Journal>,
        cluster: Arc<ClusterNode>,
        settings: SettingsTable,
    ) -> Self {
        Self::build(journal, Some(cluster), settings)
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

        // Config *chooses* what is clustered; it cannot *grant* it. A clustered
        // actor's commands must encode, no setting makes them, and because paths
        // appear at runtime this cannot be checked at boot — so it is checked
        // here, loudly, the first time it happens.
        let clustered = self.inner.settings.at(&path).clustered.value;
        let wire = self.inner.wire::<A::Command>();
        if clustered && wire.is_none() {
            return Err(ActorOfError::NotClusterable {
                path,
                actor: std::any::type_name::<A>(),
            });
        }

        // Somebody else's. Hand back a reference that reaches them and drop the
        // actor value — an actor lives where its address says, not where the
        // request to create it happened to land.
        if clustered
            && let Some(cluster) = &self.inner.cluster
            && !cluster.owns(&path.to_string())
            && let Some(link) = self.inner.remote_link::<A::Command>(&path, cluster.clone())
        {
            return Ok(self.reference(path, Some(link)));
        }

        let mut live = self.inner.live.lock();
        if let Some(existing) = live.get(&path) {
            let cached = existing
                .reference
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
        let reference = self.reference(path.clone(), Some(link.clone()));
        live.insert(
            path.clone(),
            Entry {
                deliver: wire.map(|wire| deliver_to(reference.clone(), wire)),
                reference: Arc::new(reference) as ErasedRef,
            },
        );
        drop(live);

        if clustered && let Some(cluster) = &self.inner.cluster {
            cluster.record_local_assignment(&path.to_string());
        }
        Ok(self.reference(path, Some(link)))
    }

    /// What applies at `path`, and which configured pattern decided each part.
    ///
    /// Patterns compose invisibly, so without this the first surprising
    /// configuration is unanswerable — and config that cannot be explained gets
    /// worked around rather than fixed.
    #[must_use]
    pub fn settings_at(&self, path: &ActorPath) -> crate::address::Settings {
        self.inner.settings.at(path)
    }

    /// Record that `A`'s commands round-trip, so actors of this type may be
    /// created at a clustered address.
    ///
    /// Called once per type at startup, beside [`register`](Self::register). The
    /// bound is the whole content of it: a type whose commands cannot survive a
    /// hop between hosts cannot be recorded here, so clustering one is a named
    /// error at creation rather than a surprise on the first send.
    pub fn register_clusterable<A: Actor>(&self)
    where
        A::Command: Serialize + DeserializeOwned,
    {
        self.record_wire::<A::Command>(std::any::type_name::<A>());
    }

    fn record_wire<C: Serialize + DeserializeOwned + Send + 'static>(&self, actor: &'static str) {
        let wire = Wire::<C> {
            encode: Arc::new(|cmd| serde_json::to_vec(cmd).ok()),
            decode: Arc::new(|bytes| serde_json::from_slice(bytes).ok()),
        };
        self.inner
            .wires
            .lock()
            .insert(TypeId::of::<C>(), (Arc::new(wire) as Arc<_>, actor));
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
    ///
    /// Nothing else should reach for this. An unregistered actor is not at its
    /// path as far as resolution is concerned, so the returned reference is the
    /// only way to it and dies with it, and a second actor started at a path
    /// something is already registered at is invisible to everyone but its
    /// creator. [`actor_of`](Self::actor_of) is what an ordinary caller wants.
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
        // A registered type has already proved the round-trip bound, so it is
        // clusterable by construction — no separate declaration needed.
        self.record_wire::<A::Command>(std::any::type_name::<A>());
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
            let cached = downcast::<A>(&existing.reference)?.cached();
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
        let wire = self.inner.wire::<A::Command>();
        live.insert(
            path.clone(),
            Entry {
                deliver: wire.map(|wire| deliver_to(typed.clone(), wire)),
                reference: erased,
            },
        );
        drop(live);
        if let Some(cluster) = &self.inner.cluster {
            cluster.record_local_assignment(&path.to_string());
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
            tracing::debug!(path = %env.path, "dropped a duplicate delivery");
            return Ok(());
        }
        let path = ActorPath::parse(&env.path)?;

        // Something is already running here at that address: its entry knows how
        // to decode for it, so dispatch needs no type registry and the wire needs
        // no kind.
        match self.inner.deliver_here(&path) {
            Some(Some(deliver)) => return deliver(env.payload, self.clone()).await,
            // There, but local-only. Nothing off this node was meant to reach
            // it, and saying so beats "nothing is there".
            Some(None) => return Err(DispatchError::LocalOnly(path)),
            None => {}
        }

        // A registered singleton that has not been started here yet. The one
        // case where an inbound message creates an actor, and the last thing
        // `(kind, id)` addressing is still used for.
        if let Some((kind, id)) = singleton_parts(&path) {
            let deliver = self
                .inner
                .deliverers
                .lock()
                .get(kind)
                .cloned()
                .ok_or_else(|| DispatchError::NoActor(path.clone()))?;
            return deliver(id.to_owned(), env.payload, self.clone()).await;
        }

        Err(DispatchError::NoActor(path))
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
            && !cluster.owns(&path.to_string())
            && let Some(link) = self.inner.remote_link::<A::Command>(&path, cluster.clone())
        {
            return Ok(self.reference(path, Some(link)));
        }

        self.local_instance::<A>(id)
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
