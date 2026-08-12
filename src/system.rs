use crate::actor::EventSourcedActor;
use crate::behaviour::Actor;
use crate::cluster::{ClusterNode, Dedup};
use crate::envelope::Message;
use crate::error::TellError;
use crate::journal::{InMemoryJournal, Journal};
use crate::path::ActorPath;
use crate::persistent::Persistent;
use crate::runtime::{ActorRef, Link, check_name, spawn_at};
use crate::shard::{Shard, address_for, region_of, type_in};
use parking_lot::Mutex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

/// Build the closure that decodes for one actor and hands it the command.
///
/// Takes the *link* rather than a reference, so an inbound message is delivered
/// to the instance this entry is for and nothing else. A reference would
/// re-resolve when that instance is gone, and re-resolving could hand the
/// message straight back to another node — which, while two nodes disagree about
/// placement for a moment, is a forwarding loop that dedup cannot see, because
/// every hop mints a fresh message id. Dispatch delivers what is here or says it
/// cannot.
fn deliver_to<C: Send + 'static>(path: ActorPath, link: Link<C>, wire: Wire<C>) -> DeliverHere {
    Arc::new(move |payload, system| {
        let path = path.clone();
        let link = link.clone();
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
            .ok_or_else(|| DispatchError::Decode(path.to_string()))?;
            link.send(cmd)
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

    /// The actor is here, but its commands have no wire format, so nothing off
    /// this node was ever meant to reach it. Only a shard type's commands are
    /// registered to cross a host; an ordinary child is local by construction.
    ///
    /// Named separately from "nothing is there" because the two call for
    /// different fixes: this one is a sender addressing an actor that was never
    /// meant to be reachable from where it sits.
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
    /// No registered shard type claims this address. `register` is called once
    /// per type at startup, so this is a wiring mistake, not a runtime
    /// condition — or two nodes running different builds.
    #[error("no registered shard type claims '{0}'")]
    Unclaimed(ActorPath),

    /// Two types declared the same `Shard::TYPE`. Their actors would share
    /// addresses and each other's recipes.
    #[error("two shard types are registered as '{0}'")]
    TypeCollision(&'static str),

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

/// Builds and starts whatever belongs at a shard address, on the node that owns
/// it.
///
/// Type-erased because a node that has to build has a path, not a Rust type.
/// Closes over that node's own wiring, which is why it is registered on every
/// node rather than sent to one.
type Recipe = Arc<dyn Fn(&ActorSystem, &ActorPath) -> Result<(), ActorOfError> + Send + Sync>;

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
/// by registration, which is where the round-trip bound is already proved — so
/// a send that has to leave this node can encode without demanding serde bounds
/// from every caller that merely names the command type.
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
    /// Raise to stop this actor. Held here rather than in the reference,
    /// because a reference is a name that anyone may hold and stopping is
    /// something the tree does.
    stop: tokio::sync::watch::Sender<bool>,
    /// Closed once the actor's task has ended and its children with it.
    terminated: tokio::sync::watch::Receiver<()>,
}

/// Process-wide state shared by every actor in a system.
pub(crate) struct SystemInner {
    pub(crate) journal: Arc<dyn Journal>,
    /// How to build each registered shard type, by `Shard::TYPE` — which is the
    /// third segment of every shard address, so a node holding only a path can
    /// find the recipe for what belongs there.
    shards: Mutex<HashMap<&'static str, Recipe>>,
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
    /// Command types that have been proved to round-trip, by the command's
    /// [`TypeId`]. Written by shard registration, which is the one place the
    /// bound is stated — so this is what says whether an actor can be reached
    /// from another node at all.
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
        let live = self.live.lock();
        let link = live
            .get(path)?
            .reference
            .downcast_ref::<ActorRef<C>>()?
            .cached()?;
        link.is_alive().then_some(link)
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

    /// How many actors are registered here.
    pub(crate) fn hosted(&self) -> usize {
        self.live.lock().len()
    }

    /// Stop the actor at `path` and everything under it, and wait for it.
    ///
    /// Stopping the actor is enough on its own — its own shutdown takes its
    /// children — so the sweep afterwards is for the paths that hold no actor
    /// but do hold descendants, which is what a shard address looks like on the
    /// way down to an entity.
    pub(crate) async fn stop_at(&self, path: &ActorPath) -> bool {
        let stopped = self.halt(path).await;
        self.stop_descendants(path).await;
        stopped
    }

    /// Stop everything strictly under `path`.
    ///
    /// Children end before their parent does, so a parent's last act sees a
    /// quiet subtree — which is what makes `ctx.parent()` safe to treat as a
    /// plain read, since a child cannot outlive the actor it reaches up to.
    /// That order comes out of the recursion rather than out of this loop:
    /// halting an actor runs its own shutdown, which stops *its* children and
    /// waits, before it is reported stopped. Sorting these by depth would only
    /// change how many of them are already gone by the time they come up.
    pub(crate) async fn stop_descendants(&self, path: &ActorPath) {
        let doomed: Vec<ActorPath> = {
            let live = self.live.lock();
            live.keys()
                .filter(|p| p.starts_with(path) && *p != path)
                .cloned()
                .collect()
        };
        for path in doomed {
            self.halt(&path).await;
        }
    }

    /// Take one actor out of the registry, ask it to stop, and wait for it.
    async fn halt(&self, path: &ActorPath) -> bool {
        let Some(entry) = self.live.lock().remove(path) else {
            return false;
        };
        // Removed first: an actor being stopped must not be handed to anybody
        // resolving the path in the meantime.
        let _ = entry.stop.send(true);
        let mut terminated = entry.terminated;
        // `Err` is the task ending without a clean close — a panic — which is
        // still the actor being gone.
        let _ = terminated.changed().await;
        true
    }

    /// Drop the entry for `path` if it is still the instance behind `link`.
    ///
    /// Called by an actor on its own way out, which is what keeps the registry
    /// from growing a row per actor that ever ran. Checking the link is what
    /// stops a slow shutdown from evicting the instance that replaced it.
    pub(crate) fn retire<C: Send + 'static>(&self, path: &ActorPath, link: &Link<C>) {
        let mut live = self.live.lock();
        let ours = live
            .get(path)
            .and_then(|entry| entry.reference.downcast_ref::<ActorRef<C>>())
            .and_then(ActorRef::cached)
            .is_some_and(|current| current.is_same(link));
        if ours {
            live.remove(path);
        }
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
        Self::build(journal, None)
    }

    /// The one place a system is assembled.
    ///
    /// `stand_down` starts false even for a clustered node that has not yet
    /// elected anybody. The signal means "stop what you are doing", and at
    /// construction there is nothing doing it — seeding it true would make every
    /// actor spawned before the first election exit on its first poll, silently.
    /// Refusing to *start* an instance is `require_serving`'s job, and it is a
    /// separate question with a separate answer.
    fn build(journal: Arc<dyn Journal>, cluster: Option<Arc<ClusterNode>>) -> Self {
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
                shards: Mutex::new(HashMap::new()),
                cluster,
                seen: Mutex::new(Dedup::with_capacity(DEDUP_WINDOW)),
                live: Mutex::new(HashMap::new()),
                wires: Mutex::new(HashMap::new()),
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
        Self::build(journal, Some(cluster))
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

    /// Adapt an event-sourced actor into an ordinary one, over this system's
    /// journal.
    ///
    /// Event sourcing is a *wrapper*, not a property of being an actor, and this
    /// is where that shows: adapt it and it is an ordinary actor everywhere
    /// afterwards. So there is one way to create an actor rather than one per
    /// kind of actor.
    ///
    /// ```ignore
    /// system.actor_of("c1", system.persistent(Counter::new("c1")))?;
    /// ```
    #[must_use]
    pub fn persistent<A: EventSourcedActor>(&self, actor: A) -> Persistent<A> {
        Persistent::new(actor, self.inner.journal.clone())
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
        let wire = self.inner.wire::<A::Command>();

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

        let spawned = spawn_at(actor, self.inner.clone(), path.clone());
        let link = spawned.link;
        let reference = self.reference(path.clone(), Some(link.clone()));
        live.insert(
            path.clone(),
            Entry {
                deliver: wire.map(|wire| deliver_to(path.clone(), link.clone(), wire)),
                reference: Arc::new(reference) as ErasedRef,
                stop: spawned.stop,
                terminated: spawned.terminated,
            },
        );
        drop(live);
        Ok(self.reference(path, Some(link)))
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

    /// Stop the actor at `path` and everything under it.
    ///
    /// Returns once the subtree is quiet, deepest first, and `false` if nothing
    /// was there. The same operation [`ActorRef::stop`] performs, for a caller
    /// holding a path rather than a reference — including a path that holds no
    /// actor of its own, which is how a whole branch is cleared.
    pub async fn stop(&self, path: &ActorPath) -> bool {
        self.inner.stop_at(path).await
    }

    /// How many actors this node is running.
    ///
    /// Rises with what is being hosted and falls as things stop, because an
    /// actor leaves the registry on its way out. A number that only ever climbs
    /// would mean the tree is leaking rows — and, for event-sourced actors, the
    /// journal handles behind them.
    #[must_use]
    pub fn hosted(&self) -> usize {
        self.inner.hosted()
    }

    /// Start an actor at `path` without registering it.
    ///
    /// The escape hatch for the two things registration would otherwise make
    /// impossible: standing a second instance up at an address something already
    /// holds, and starting one on a node that is refusing to host. Both are
    /// things a test needs and a deployment does not.
    ///
    /// An unregistered actor is not at its path as far as resolution is
    /// concerned, so the returned reference is the only way to it, nothing can
    /// stop it, and it is not taken down with the tree above it.
    /// [`actor_of`](Self::actor_of) is what an ordinary caller wants.
    pub fn spawn_at<A: Actor>(&self, path: ActorPath, actor: A) -> ActorRef<A::Command> {
        let spawned = spawn_at(actor, self.inner.clone(), path.clone());
        self.reference(path, Some(spawned.link))
    }

    /// Registration for one shard type.
    ///
    /// The turbofish sits here so that [`register`](ShardOf::register) infers
    /// the recipe's types and needs none of its own.
    #[must_use]
    pub fn shard<S: Shard>(&self) -> ShardOf<'_, S> {
        ShardOf {
            system: self,
            marker: std::marker::PhantomData,
        }
    }

    /// A reference to the actors of a shard type.
    ///
    /// One reference for the whole type, not one per actor: each command names
    /// its own target through [`Shard::entity_id`], and is routed to whichever
    /// node owns [`Shard::shard_id`]. Indistinguishable at the call site from a
    /// reference to a local actor — same type, same `tell`, same `ask`.
    #[must_use]
    pub fn shard_actor_of<S: Shard>(&self) -> ActorRef<S::Command> {
        let system = self.clone();
        let route: crate::runtime::RemoteSend<S::Command> = Arc::new(move |cmd: S::Command| {
            let system = system.clone();
            Box::pin(async move {
                let (entity, shard) = address_for::<S>(&cmd);
                system.deliver_to_shard::<S>(entity, shard, cmd).await
            })
        });
        self.reference(region_of(S::TYPE), Some(Link::Remote(route)))
    }

    /// Hand `cmd` to the actor at `entity`, wherever the cluster puts it.
    async fn deliver_to_shard<S: Shard>(
        &self,
        entity: ActorPath,
        shard: ActorPath,
        cmd: S::Command,
    ) -> Result<(), TellError> {
        let ours = match &self.inner.cluster {
            Some(cluster) => cluster.owns(&shard.to_string()),
            // No cluster: this node owns everything, which is the single-node
            // deployment and mentions none of this.
            None => true,
        };

        if ours {
            let link = self
                .start_shard_actor::<S>(&entity)
                .map_err(|_| TellError::Undeliverable)?;
            return link.send(cmd).await.map_err(|(e, _)| e);
        }

        let Some(cluster) = self.inner.cluster.clone() else {
            return Err(TellError::Undeliverable);
        };
        let wire = self
            .inner
            .wire::<S::Command>()
            .ok_or(TellError::Undeliverable)?;
        // Encoded inside the router context, which is what registers any reply
        // handle in the command against this node before it leaves.
        let router: Arc<dyn crate::reply::ReplyRouter> = cluster.clone();
        let payload = crate::reply::with_router(router, || (wire.encode)(&cmd))
            .ok_or(TellError::Undeliverable)?;
        cluster
            .send(
                &shard.to_string(),
                &entity.to_string(),
                payload,
                cluster.next_message_id(),
            )
            .await
            .map_err(|_| TellError::Undeliverable)
    }

    /// The actor at a shard address on this node, building it from the
    /// registered recipe if it is not running yet.
    fn start_shard_actor<S: Shard>(
        &self,
        entity: &ActorPath,
    ) -> Result<Link<S::Command>, ActorOfError> {
        if let Some(link) = self.inner.resolve::<S::Command>(entity) {
            return Ok(link);
        }
        self.build_at(entity)?;
        self.inner
            .resolve::<S::Command>(entity)
            .ok_or_else(|| ActorOfError::Unclaimed(entity.clone()))
    }

    /// Run the registered recipe for whatever belongs at `path`.
    fn build_at(&self, path: &ActorPath) -> Result<(), ActorOfError> {
        let type_name = type_in(path).ok_or_else(|| ActorOfError::Unclaimed(path.clone()))?;
        let recipe = self
            .inner
            .shards
            .lock()
            .get(type_name)
            .cloned()
            .ok_or_else(|| ActorOfError::Unclaimed(path.clone()))?;
        recipe(self, path)?;
        Ok(())
    }

    /// Refuse everything while this node has no quorum.
    ///
    /// A node in a minority cannot know whether its instances have already been
    /// given to somebody else, and answering from state that may be history is
    /// the one failure the write fence cannot catch — because a read never
    /// writes.
    fn require_serving(&self) -> Result<(), ActorOfError> {
        match &self.inner.cluster {
            Some(cluster) if !cluster.serving() => Err(ActorOfError::NotServing),
            _ => Ok(()),
        }
    }

    /// Feed one inbound message to whoever it is for.
    pub async fn dispatch(&self, message: Message) -> Result<(), DispatchError> {
        let env = match message {
            Message::Command(env) => env,
            Message::Reply(reply) => {
                // An answer is for a caller, not an actor, so it never goes
                // near the registry, the dedup window or placement — and, for
                // the same reason, not near the quorum check either. That check
                // asks whether an *instance* still belongs to this node, and a
                // future waiting on this node is nobody else's to take.
                // Refusing here would only mean the caller waits forever.
                if let Some(cluster) = &self.inner.cluster {
                    cluster.deliver_reply(reply);
                }
                return Ok(());
            }
        };
        self.require_serving()?;
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

        // A shard actor this node owns but has not started yet. The one case
        // where an inbound message creates an actor — and it can, because the
        // address names the type and every node registered a recipe for it.
        if type_in(&path).is_some() {
            self.build_at(&path)
                .map_err(|_| DispatchError::NoActor(path.clone()))?;
            if let Some(Some(deliver)) = self.inner.deliver_here(&path) {
                return deliver(env.payload, self.clone()).await;
            }
        }

        Err(DispatchError::NoActor(path))
    }
}

/// The registration step for one shard type — see [`ActorSystem::shard`].
///
/// Carries only [`register`](Self::register), deliberately: there is one way to
/// reach a shard type's actors, and it is [`ActorSystem::shard_actor_of`].
pub struct ShardOf<'a, S: Shard> {
    system: &'a ActorSystem,
    marker: std::marker::PhantomData<fn() -> S>,
}

impl<S: Shard> ShardOf<'_, S> {
    /// Teach this node how to build actors of this type.
    ///
    /// Called once per type at startup on **every** node. `recipe` closes over
    /// this node's own wiring and is never sent anywhere: an actor is live
    /// state, so the node that owns an address has to be able to construct what
    /// belongs there without help from whoever wanted it.
    ///
    /// A plain recipe returns its actor; an event-sourced one adapts it first
    /// with [`ActorSystem::persistent`].
    ///
    /// # Errors
    /// If a type is already registered under the same [`Shard::TYPE`].
    pub fn register<A, F>(self, recipe: F) -> Result<(), ActorOfError>
    where
        A: Actor<Command = S::Command>,
        F: Fn(&ActorSystem, &ActorPath) -> A + Send + Sync + 'static,
    {
        let system = self.system;
        system.record_wire::<S::Command>(std::any::type_name::<S>());

        let built: Recipe = Arc::new(move |system: &ActorSystem, path: &ActorPath| {
            let (Some(parent), Some(name)) = (path.parent(), path.name()) else {
                return Err(ActorOfError::Unclaimed(path.clone()));
            };
            system
                .get_or_create(&parent, name, recipe(system, path))
                .map(|_| ())
        });

        let mut shards = system.inner.shards.lock();
        if shards.contains_key(S::TYPE) {
            return Err(ActorOfError::TypeCollision(S::TYPE));
        }
        shards.insert(S::TYPE, built);
        Ok(())
    }
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
    use crate::behaviour::Flow;
    use crate::persistence_id::PersistenceId;
    use crate::reply::ReplyTo;
    use crate::runtime::ActorContext;
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    /// Every command names its own target, because the extractors are the only
    /// thing that knows where a message is going.
    #[derive(Serialize, Deserialize)]
    enum CounterCmd {
        Inc { id: String, by: i64 },
        Get { id: String, reply: ReplyTo<i64> },
    }

    impl CounterCmd {
        fn id(&self) -> &str {
            match self {
                CounterCmd::Inc { id, .. } | CounterCmd::Get { id, .. } => id,
            }
        }
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
                CounterCmd::Inc { by, .. } => CommandEffect::persist(vec![Incremented(by)]),
                CounterCmd::Get { reply, .. } => {
                    let _ = reply.send(state.value);
                    CommandEffect::none()
                }
            }
        }
    }

    impl Shard for Counter {
        type Command = CounterCmd;
        const TYPE: &'static str = "counter";

        fn entity_id(cmd: &CounterCmd) -> String {
            cmd.id().to_owned()
        }

        /// One shard per counter, so each is placed on its own.
        fn shard_id(cmd: &CounterCmd) -> String {
            cmd.id().to_owned()
        }
    }

    fn counters() -> ActorSystem {
        let system = ActorSystem::in_memory();
        system
            .shard::<Counter>()
            // The recipe is handed the address, which is where the identity
            // comes from — `path.name()` is the entity id.
            .register(|sys, path| {
                sys.persistent(Counter {
                    id: path.name().unwrap_or_default().to_owned(),
                })
            })
            .unwrap();
        system
    }

    async fn value_of(system: &ActorSystem, id: &str) -> i64 {
        system
            .shard_actor_of::<Counter>()
            .ask(|reply| CounterCmd::Get {
                id: id.to_owned(),
                reply,
            })
            .await
            .unwrap()
    }

    /// Two sends naming one entity reach one actor, not two — the guarantee a
    /// caller would otherwise have to build by hand, and the one that matters
    /// most for an event-sourced actor, where two instances mean two writers on
    /// one journal.
    #[tokio::test]
    async fn one_entity_id_is_one_actor() {
        let system = counters();
        let counters = system.shard_actor_of::<Counter>();
        counters
            .tell(CounterCmd::Inc {
                id: "c1".into(),
                by: 2,
            })
            .await
            .unwrap();
        counters
            .tell(CounterCmd::Inc {
                id: "c1".into(),
                by: 3,
            })
            .await
            .unwrap();
        assert_eq!(value_of(&system, "c1").await, 5);
    }

    /// Different entity ids are different actors at different addresses.
    #[tokio::test]
    async fn different_entity_ids_are_different_actors() {
        let system = counters();
        let counters = system.shard_actor_of::<Counter>();
        counters
            .tell(CounterCmd::Inc {
                id: "a".into(),
                by: 2,
            })
            .await
            .unwrap();
        counters
            .tell(CounterCmd::Inc {
                id: "b".into(),
                by: 3,
            })
            .await
            .unwrap();
        assert_eq!(value_of(&system, "a").await, 2);
        assert_eq!(value_of(&system, "b").await, 3);
    }

    /// A shard address carries the type, the shard and the entity — so a node
    /// holding only a path can find the recipe for what belongs there.
    #[tokio::test]
    async fn a_shard_actor_lives_at_its_address() {
        let system = counters();
        system
            .shard_actor_of::<Counter>()
            .tell(CounterCmd::Inc {
                id: "c1".into(),
                by: 1,
            })
            .await
            .unwrap();
        let path = crate::shard::entity_of("counter", "c1", "c1");
        assert_eq!(path.to_string(), "/system/shard/counter/c1/c1");
        assert!(system.inner.resolve::<CounterCmd>(&path).is_some());
    }

    /// Sending to a type nobody registered is a named error rather than a
    /// silently spawned actor with no wiring.
    #[tokio::test]
    async fn an_unregistered_type_cannot_be_reached() {
        let system = ActorSystem::in_memory();
        let sent = system
            .shard_actor_of::<Counter>()
            .tell(CounterCmd::Inc {
                id: "c1".into(),
                by: 1,
            })
            .await;
        assert!(sent.is_err());
    }

    /// Two types sharing a `TYPE` would share addresses and each other's
    /// recipes, so the second registration is refused.
    #[tokio::test]
    async fn colliding_type_names_are_reported() {
        struct Impostor;
        #[async_trait]
        impl Actor for Impostor {
            type Command = CounterCmd;
            async fn handle(
                &mut self,
                _cmd: CounterCmd,
                _ctx: &mut ActorContext<CounterCmd>,
            ) -> Flow {
                Flow::Continue
            }
        }
        impl Shard for Impostor {
            type Command = CounterCmd;
            // The mistake under test.
            const TYPE: &'static str = "counter";
            fn entity_id(cmd: &CounterCmd) -> String {
                cmd.id().to_owned()
            }
            fn shard_id(cmd: &CounterCmd) -> String {
                cmd.id().to_owned()
            }
        }

        let system = counters();
        let err = system
            .shard::<Impostor>()
            .register(|_sys, _path| Impostor)
            .unwrap_err();
        assert!(matches!(err, ActorOfError::TypeCollision("counter")));
    }
}
