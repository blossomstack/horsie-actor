//! A counter that persists every increment and recovers it after a restart.
//!
//! Run with `cargo run --example counter`. Kept in sync with the README by
//! being the same code: this file is what CI compiles.
#![allow(
    clippy::unwrap_used,
    reason = "an example reads better without error plumbing"
)]

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
        .actor_of("c1", system.persistent(Counter { id: "c1".into() }))
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
        .actor_of("c1", restarted.persistent(Counter { id: "c1".into() }))
        .unwrap();
    let (tx, rx) = oneshot::channel();
    revived.tell(Cmd::Get(tx)).await.unwrap();
    assert_eq!(rx.await.unwrap(), 7);
}
