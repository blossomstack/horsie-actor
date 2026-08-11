# horsie-actor

[![crates.io](https://img.shields.io/crates/v/horsie-actor.svg)](https://crates.io/crates/horsie-actor)
[![docs.rs](https://docs.rs/horsie-actor/badge.svg)](https://docs.rs/horsie-actor)

An event-sourced actor runtime for Tokio. An actor's state is never mutated
directly — it is rebuilt by folding persisted events, so a fresh actor with the
same identity recovers exactly where the previous one left off.

```toml
[dependencies]
horsie-actor = "0.9"
```

## Two traits

`Actor` is the bare contract — a command type, and one command handled at a
time. It says nothing about storage.

`EventSourcedActor` is the durable one, and `Persistent<A>` adapts it into an
`Actor`. So persistence is a wrapper, not a property of being an actor: a
stateless router and an event-sourced aggregate are spawned, addressed and
supervised through exactly the same machinery, and you pay for a journal only
where you want one.

`ActorSystem` owns the journal, every actor currently running — keyed by path —
and the registry of actor types reachable by id.

## The idea

Most actor libraries give you a mailbox and leave durability to you. This one
inverts the ownership: the runtime owns persistence and state transitions, and
an actor only *decides* what should be persisted.

A command handler returns a `CommandEffect` describing its decision. The runtime
writes those events, folds them into state through `apply_event`, and only then
runs the post-persist actions. Because `apply_event` is the single path by which
state ever changes, live operation and crash recovery are the same code — there
is no separate recovery path that can drift.

## Example

This is [`examples/counter.rs`](examples/counter.rs) verbatim, so CI compiles
and runs it — if the API moves, this stops building.

```rust
use async_trait::async_trait;
use horsie_actor::{
    ActorContext, ActorSystem, CommandEffect, EventSourcedActor, InMemoryJournal, PersistenceId,
    Root,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::oneshot;

struct Counter {
    id: String,
}

enum Cmd {
    Inc(i64),
    Get(oneshot::Sender<i64>),
}

#[derive(Serialize, Deserialize, Clone)]
enum Event {
    Incremented(i64),
}

#[derive(Serialize, Deserialize, Default)]
struct State {
    value: i64,
}

#[async_trait]
impl EventSourcedActor for Counter {
    type Command = Cmd;
    type Event = Event;
    type State = State;
    // A top-level actor is a child of the root, which takes no messages.
    type ParentCommand = Root;

    fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new("counter", self.id.clone())
    }

    fn initial_state() -> State {
        State::default()
    }

    // Pure: no I/O, no side effects. Runs identically during replay.
    fn apply_event(mut state: State, event: Event) -> State {
        match event {
            Event::Incremented(n) => state.value += n,
        }
        state
    }

    async fn handle_command(
        &mut self,
        state: &State,
        cmd: Cmd,
        _ctx: &mut ActorContext<Self::Command>,
    ) -> CommandEffect<Event> {
        match cmd {
            Cmd::Inc(n) => CommandEffect::persist(vec![Event::Incremented(n)]),
            Cmd::Get(reply) => {
                let _ = reply.send(state.value);
                CommandEffect::none()
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let journal = Arc::new(InMemoryJournal::new());
    let system = ActorSystem::new(journal.clone());
    // `/c1` — a top-level actor, created by the system under the root. The name
    // is the actor's identity for as long as it exists.
    let counter = system
        .actor_of_persistent("c1", Counter { id: "c1".into() })
        .unwrap();

    counter.tell(Cmd::Inc(3)).await.unwrap();
    counter.tell(Cmd::Inc(4)).await.unwrap();

    let (tx, rx) = oneshot::channel();
    counter.tell(Cmd::Get(tx)).await.unwrap();
    assert_eq!(rx.await.unwrap(), 7);

    // A fresh system over the same journal — a restart, in effect. The second
    // incarnation replays what the first one wrote.
    let restarted = ActorSystem::new(journal);
    let revived = restarted
        .actor_of_persistent("c1", Counter { id: "c1".into() })
        .unwrap();
    let (tx, rx) = oneshot::channel();
    revived.tell(Cmd::Get(tx)).await.unwrap();
    assert_eq!(rx.await.unwrap(), 7);
}
```

## A reference is a name

Every actor has a path. `/` is the root; a top-level actor is created by the system under it, and every other actor is created by its parent, under its parent's path. `/acct-7/session-3/agent-main` names one actor for as long as that actor exists.

An `ActorRef` is that path plus a cached link to whatever it resolves to right now. A send uses the cache; a send that fails drops it, resolves the path once more and retries. So a reference held across a stop, a restart, or a reactivation after an idle offload keeps working, and the holder does nothing and knows nothing:

```rust
let held = system.actor_of("worker", Worker::new())?;
held.tell(Stop).await?;              // the instance goes away
system.actor_of("worker", Worker::new())?;   // a different instance, same name
held.tell(Ping).await?;              // still delivers — to the new one
```

Resolution never *creates*. A path with nothing at it fails the send, so a reference cannot wake an actor that nobody asked for.

`ctx.actor_of(name, actor)` creates a child under the current actor; `ctx.parent()` is an ordinary reference to the parent's path, typed by the actor's `ParentCommand`, so a child reaches upwards without having been handed anything at construction. Both are get-or-create: two callers naming one path get one actor, and the loser's actor value is dropped without ever being started.

## Durability you can wait on

`CommandEffect` composes one persist step with an ordered set of post-persist
actions, rather than offering a variant per combination:

```rust
CommandEffect::persist(events).and_snapshot().and_ack(reply).and_stop()
```

`and_ack` is the interesting one. The reply is sent **after** the durable write
resolves, carrying `Ok(())` or the `JournalError`. Paired with `ActorRef::ask`,
that gives a caller genuine backpressure: when `ask` returns `Ok`, the event is
on disk and already folded into state. When the write fails, the events are
neither folded nor counted, so state never diverges from what was persisted, and
the caller learns to abort instead of proceeding on a history that does not
exist.

`and_snapshot` writes the state and compacts the events it subsumes. Surviving
events keep their original sequence numbers, so a snapshot taken at sequence `n`
and a later `replay(after: n)` line up exactly.

## Bring your own storage

`Journal` is an append-only log of opaque byte blobs plus snapshots, keyed by
`PersistenceId` (an actor kind and an instance id). Serialization is the
runtime's concern, so the trait carries no domain types and no serde bounds —
implement it over Postgres, SQLite, S3, or anything else that can append.

`persist` and `save_snapshot` are conditional: they take the sequence number the
writer believes the log ends at, and append only if it still does. A mismatch is
a `JournalError::Conflict` and the log is left untouched.

That one parameter is the write fence. It needs no notion of ownership and no
claim step — a writer that has fallen behind is caught by being behind — and,
unlike anything that checks before writing, it holds for a process that was
frozen through a failover and woke up still believing it owned the log. The
check and the append must be **one operation**, which is why it is a parameter
and not a wrapper: a decorator cannot join a transaction it does not open, so it
would read and then write, which is the race being closed. A backend that cannot
express the condition atomically must return an error rather than append
anyway.

`InMemoryJournal` ships for tests and single-process runs. With the `test-util`
feature, `testkit::FaultyJournal` wraps any journal and fails writes or truncates
replays on demand, so recovery paths can be tested rather than assumed, and
`testkit::conformance` is the contract as runnable assertions — including the
fence — so a new backend can be held to it.

## Clustering

Several nodes can host one actor tree, addressed the same way from any of them:
`system.singleton_of::<A>(&id)` returns an `ActorRef` whether the instance runs
here or on another node, and `tell` and `ask` both work across the boundary. A reply
handle carries the node that asked and a correlation id, so the answer finds its
way back to a caller still awaiting it.

The requirement that a reply must be encodable sits on `ReplyTo`'s own
`Serialize` rather than on `ask`. So it applies exactly to handles that really do
cross a host — `ClusterActor` already requires its command type to round-trip,
and a command holding a `ReplyTo<R>` only does if `R` does — and never to the
local-only reply types that make up most of an application.

**Which addresses are clustered is configuration.** An entry is a pattern over paths — `/*`, `/acct-7/*` — and a set of settings, and an address takes every matching entry merged *per field*, most specific winning. Declaration order decides nothing. `system.settings_at(&path)` answers what applies and which entry set it, because patterns compose invisibly and config that cannot be explained gets worked around rather than fixed.

Matched on the address, not the actor's type, because resolution happens before the actor exists: a node asked for `/acct-7/session-3` has to decide whether that path is clustered with nothing at the path to ask. The default is local, so a single-node deployment configures none of this.

**An actor lives where its nearest clustered ancestor lives.** Only some addresses are placed by the cluster; the rest are ordinary children that live with their parent. Resolving a path means routing to the host of its deepest clustered prefix and walking the remaining segments there — so clustering stays something you turn on for a few addresses rather than for every actor in the tree.

Config *chooses*; it cannot *grant*. A clustered actor's commands must round-trip, and no setting makes them — so `register_clusterable::<A>()` is where that is proved, and creating an actor at a clustered address without it fails there, naming the path and the type.

Three things, kept separate:

- **Membership** — who is in the cluster — is agreed by Raft, so no node can
  invent its own answer. This is the part that matters: the alternative is a
  node that drops a peer the moment one send fails, and a node cut off from its
  peers then concludes it is the whole cluster.
- **Liveness** — which members are up — is observed by the leader and
  replicated, so every node places instances over the same set.
- **The write fence** — the conditional append above — is what makes a wrong
  answer to either survivable rather than corrupting.

A node that cannot see a quorum stops: it refuses to start instances, stops the
ones it is running, and drops their in-flight work. That bounds how long a
displaced node keeps answering reads, which the fence cannot do because a read
never writes.

`TcpTransport` carries both actor deliveries and consensus over one
length-prefixed, authenticated connection. It authenticates the peer; it does
not encrypt, so run it on a private network or through a TLS tunnel.

## License

Apache-2.0 OR MIT, at your option.
