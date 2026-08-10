# horsie-actor

[![crates.io](https://img.shields.io/crates/v/horsie-actor.svg)](https://crates.io/crates/horsie-actor)
[![docs.rs](https://docs.rs/horsie-actor/badge.svg)](https://docs.rs/horsie-actor)

An event-sourced actor runtime for Tokio. An actor's state is never mutated
directly — it is rebuilt by folding persisted events, so a fresh actor with the
same identity recovers exactly where the previous one left off.

```toml
[dependencies]
horsie-actor = "0.2"
```

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
    ActorContext, CommandEffect, EventSourcedActor, InMemoryJournal, PersistenceId, spawn_root,
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
        _ctx: &mut ActorContext<Self>,
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
    let counter = spawn_root(Counter { id: "c1".into() }, journal.clone());

    counter.tell(Cmd::Inc(3)).await.unwrap();
    counter.tell(Cmd::Inc(4)).await.unwrap();

    let (tx, rx) = oneshot::channel();
    counter.tell(Cmd::Get(tx)).await.unwrap();
    assert_eq!(rx.await.unwrap(), 7);

    // A second incarnation on the same journal recovers the same value.
    let revived = spawn_root(Counter { id: "c1".into() }, journal);
    let (tx, rx) = oneshot::channel();
    revived.tell(Cmd::Get(tx)).await.unwrap();
    assert_eq!(rx.await.unwrap(), 7);
}
```

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

`InMemoryJournal` ships for tests and single-process runs. With the `test-util`
feature, `testkit::FaultyJournal` wraps any journal and fails writes or truncates
replays on demand, so recovery paths can be tested rather than assumed.

## License

Apache-2.0 OR MIT, at your option.
