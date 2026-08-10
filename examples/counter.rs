//! A counter that persists every increment and recovers it after a restart.
//!
//! Run with `cargo run --example counter`. Kept in sync with the README by
//! being the same code: this file is what CI compiles.
#![allow(
    clippy::unwrap_used,
    reason = "an example reads better without error plumbing"
)]

use async_trait::async_trait;
use horsie_actor::{ActorContext, ActorSystem, CommandEffect, EventSourcedActor, PersistenceId};
use serde::{Deserialize, Serialize};
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
    let system = ActorSystem::in_memory();
    let counter = system.spawn_persistent(Counter { id: "c1".into() });

    counter.tell(Cmd::Inc(3)).await.unwrap();
    counter.tell(Cmd::Inc(4)).await.unwrap();

    let (tx, rx) = oneshot::channel();
    counter.tell(Cmd::Get(tx)).await.unwrap();
    assert_eq!(rx.await.unwrap(), 7);

    // A second incarnation on the same journal recovers the same value.
    let revived = system.spawn_persistent(Counter { id: "c1".into() });
    let (tx, rx) = oneshot::channel();
    revived.tell(Cmd::Get(tx)).await.unwrap();
    assert_eq!(rx.await.unwrap(), 7);
}
