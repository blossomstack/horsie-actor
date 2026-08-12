//! What happens to a caller when the answer does not come.
//!
//! `ask` has no deadline on purpose — an actor may legitimately take a long
//! time — so every way an answer can fail to arrive has to end the wait by
//! itself. These are the local cases; the ones that only appear once a reply
//! handle has crossed a host are in `cluster_e2e.rs`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use async_trait::async_trait;
use horsie_actor::{Actor, ActorContext, ActorSystem, Flow, ReplyTo, Root, TellError};
use std::time::Duration;

enum Ask {
    /// Answered at once.
    Now(ReplyTo<u32>),
    /// Taken and dropped without an answer.
    Never(ReplyTo<u32>),
    /// Kept, so the caller is left genuinely waiting.
    Hold(ReplyTo<u32>),
}

#[derive(Default)]
struct Oracle {
    held: Vec<ReplyTo<u32>>,
}

#[async_trait]
impl Actor for Oracle {
    type Command = Ask;
    type ParentCommand = Root;

    async fn handle(&mut self, cmd: Ask, _ctx: &mut ActorContext<Ask>) -> Flow {
        match cmd {
            Ask::Now(reply) => {
                let _ = reply.send(7);
            }
            Ask::Never(reply) => drop(reply),
            Ask::Hold(reply) => self.held.push(reply),
        }
        Flow::Continue
    }
}

fn oracle() -> horsie_actor::ActorRef<Ask> {
    ActorSystem::in_memory()
        .actor_of("oracle", Oracle::default())
        .unwrap()
}

/// The baseline the rest is measured against: an actor that takes a request and
/// drops the handle fails its caller immediately, with no deadline involved.
#[tokio::test]
async fn a_dropped_handle_fails_the_caller_at_once() {
    let answer = tokio::time::timeout(Duration::from_secs(5), oracle().ask(Ask::Never))
        .await
        .expect("a dropped handle must not leave the caller waiting");
    assert!(answer.is_err());
}

/// A deadline is for the case nothing else covers — an answer that is still
/// coming, but not soon enough to be worth having.
#[tokio::test]
async fn ask_within_gives_up_on_an_answer_that_never_comes() {
    let outcome = oracle()
        .ask_within(Duration::from_millis(50), Ask::Hold)
        .await;
    assert!(matches!(outcome, Err(TellError::NoAnswer)), "{outcome:?}");
}

/// And does nothing at all when the answer is in time, which is the case that
/// would otherwise make a deadline expensive to reach for.
#[tokio::test]
async fn ask_within_returns_an_answer_that_arrives_in_time() {
    let answer = oracle()
        .ask_within(Duration::from_secs(5), Ask::Now)
        .await
        .unwrap();
    assert_eq!(answer, 7);
}
