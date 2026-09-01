use crate::actor::{CommandEffect, EventSourcedActor};
use crate::behaviour::{Actor, Flow, StartError};
use crate::error::JournalError;
use crate::journal::Journal;
use crate::persistence_id::PersistenceId;
use crate::runtime::ActorContext;
use async_trait::async_trait;
use futures_util::StreamExt;
use std::sync::Arc;

/// Adapts an [`EventSourcedActor`] into a plain [`Actor`].
///
/// This is where every persistence concern lives — recovery, the durable write,
/// the fold, snapshotting, the ack, the stop — so the mailbox loop itself knows
/// nothing about journals. An actor that does not want event sourcing simply
/// implements [`Actor`] and never goes through here.
pub struct Persistent<A: EventSourcedActor> {
    inner: A,
    pid: PersistenceId,
    journal: Arc<dyn Journal>,
    state: A::State,
    /// Sequence number of the last event folded into `state`.
    ///
    /// Doubles as the write fence. Every append is conditional on the log still
    /// ending here, so an instance that has been superseded — by another host,
    /// or by its own earlier incarnation waking from a pause — is rejected on
    /// its first write rather than merging its history with the live one.
    seq_nr: u64,
}

impl<A: EventSourcedActor> Persistent<A> {
    pub(crate) fn new(inner: A, journal: Arc<dyn Journal>) -> Self {
        let pid = inner.persistence_id();
        Self {
            inner,
            pid,
            journal,
            // Overwritten by `on_start`. Held as a value rather than an
            // `Option` so `handle` never has to unwrap a state that is always
            // present by the time a command arrives.
            state: A::initial_state(),
            seq_nr: 0,
        }
    }
}

#[async_trait]
impl<A: EventSourcedActor> Actor for Persistent<A> {
    type Command = A::Command;
    // Carried through verbatim, which is what keeps the context type identical
    // for the adapter and the actor it wraps — the reason the context is
    // parameterized by command types rather than by actor types.

    async fn on_start(&mut self, ctx: &mut ActorContext<Self::Command>) -> Result<(), StartError> {
        let (state, seq_nr) = recover::<A>(&self.pid, &self.journal).await?;
        self.state = state;
        self.seq_nr = seq_nr;
        self.inner.on_recovery_complete(&self.state, ctx).await;
        Ok(())
    }

    async fn handle(&mut self, cmd: Self::Command, ctx: &mut ActorContext<Self::Command>) -> Flow {
        let effect = self.inner.handle_command(&self.state, cmd, ctx).await;
        let CommandEffect {
            events,
            snapshot,
            ack,
            confirmed,
            stop,
        } = effect;

        // One persist step, then the post-persist actions in a fixed order:
        // write -> publish -> snapshot -> confirm -> ack -> stop. The write
        // outcome is folded only on success, so a failed write leaves state
        // consistent with what is actually durable and the ack reports the
        // failure.
        //
        // An effect with nothing to write skips the journal entirely — a plain
        // read costs no round trip — *unless* it asked to be confirmed, in
        // which case the empty conditional append runs anyway: it appends
        // nothing and checks everything, which is the checked read.
        let (persisted, result) = if events.is_empty() && confirmed.is_none() {
            (Vec::new(), Ok(()))
        } else {
            persist_events::<A>(
                &self.pid,
                &self.journal,
                events,
                &mut self.state,
                &mut self.seq_nr,
            )
            .await
        };

        // Publish what just became durable. Before the ack, so an `ask` caller
        // cannot observe the write landing ahead of the frames it produced; and
        // before `stop`, so a final batch is still announced.
        if result.is_ok() && !persisted.is_empty() {
            self.inner
                .on_events_persisted(&persisted, &self.state)
                .await;
        }

        // Only after a successful write: snapshotting state that diverged from
        // the journal would be unsound. Skipped when stopping — the state is
        // discarded next anyway.
        if snapshot && result.is_ok() && !stop {
            snapshot_state::<A>(&self.pid, &self.journal, &self.state, self.seq_nr).await;
        }

        // A conflict is terminal, not a retryable hiccup: somebody else has
        // written to this log, so this instance's state is a dead branch and
        // every future write from it would be rejected too. Carrying on would
        // leave a zombie that accepts commands and fails every write until an
        // operator noticed — so stop, which closes the mailbox and makes callers
        // fail fast and re-resolve to whoever is live now.
        let conflicted = matches!(result, Err(JournalError::Conflict { .. }));
        if conflicted {
            tracing::warn!(
                pid = %self.pid,
                "the log has moved past this instance; stopping rather than serving stale"
            );
        }

        // The checked read's answer: only now does the handler's deferred
        // reply learn whether the log still ends where this instance believes.
        if let Some(confirm) = confirmed {
            confirm(&result);
        }

        // Reply only now, so an `ask` caller returns the journaled guarantee
        // (`Ok`) or the failure (`Err`) and can decide whether to proceed.
        if let Some(ack) = ack {
            let _ = ack.send(result);
        }

        if stop || conflicted {
            Flow::Stop
        } else {
            Flow::Continue
        }
    }
}

/// Rebuild an actor's state from its latest snapshot plus subsequent events.
/// Returns the recovered state and the sequence number of the last applied event.
async fn recover<A: EventSourcedActor>(
    pid: &PersistenceId,
    journal: &Arc<dyn Journal>,
) -> Result<(A::State, u64), JournalError> {
    let (mut state, mut seq_nr) = match journal.latest_snapshot(pid).await? {
        Some((bytes, seq)) => {
            let state = serde_json::from_slice::<A::State>(&bytes)
                .map_err(|e| JournalError::Serialization(e.to_string()))?;
            (state, seq)
        }
        None => (A::initial_state(), 0),
    };

    let mut stream = journal.replay(pid, seq_nr).await;
    while let Some(item) = stream.next().await {
        // Take the journal's number rather than counting: after compaction the
        // survivors keep their original numbers, and this is the number a later
        // snapshot is recorded at.
        let (seq, bytes) = item?;
        let event = serde_json::from_slice::<A::Event>(&bytes)
            .map_err(|e| JournalError::Serialization(e.to_string()))?;
        state = A::apply_event(state, event);
        seq_nr = seq;
    }
    Ok((state, seq_nr))
}

/// Persist `events`, then fold them into `state`, advancing `seq_nr`. On failure
/// the events are neither applied nor counted, keeping state consistent with what
/// was durably written; the error is logged here and also returned. The batch
/// comes back so the caller can hand it to
/// [`EventSourcedActor::on_events_persisted`] without re-deriving it.
async fn persist_events<A: EventSourcedActor>(
    pid: &PersistenceId,
    journal: &Arc<dyn Journal>,
    events: Vec<A::Event>,
    state: &mut A::State,
    seq_nr: &mut u64,
) -> (Vec<A::Event>, Result<(), JournalError>) {
    let mut encoded = Vec::with_capacity(events.len());
    for event in &events {
        match serde_json::to_vec(event) {
            Ok(bytes) => encoded.push(bytes),
            Err(e) => {
                tracing::error!(%pid, error = %e, "failed to serialize event; skipping persist");
                return (events, Err(JournalError::Serialization(e.to_string())));
            }
        }
    }
    if let Err(e) = journal.persist(pid, &encoded, *seq_nr).await {
        tracing::error!(%pid, error = %e, "failed to persist events; state left unchanged");
        return (events, Err(e));
    }
    for event in &events {
        // `apply_event` takes the state by value, so swap a placeholder in while
        // folding. `initial_state()` is cheap and immediately overwritten.
        let current = std::mem::replace(state, A::initial_state());
        *state = A::apply_event(current, event.clone());
        *seq_nr += 1;
    }
    (events, Ok(()))
}

/// Snapshot `state` at `seq_nr` and compact the now-redundant event log.
///
/// The order is load-bearing. Compaction is unconditional, so it is the
/// snapshot's rejection that stops a stale writer deleting events it never saw
/// — which means the early return below is a safety property, not tidiness.
async fn snapshot_state<A: EventSourcedActor>(
    pid: &PersistenceId,
    journal: &Arc<dyn Journal>,
    state: &A::State,
    seq_nr: u64,
) {
    let bytes = match serde_json::to_vec(state) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!(%pid, error = %e, "failed to serialize snapshot; skipping");
            return;
        }
    };
    if let Err(e) = journal.save_snapshot(pid, bytes, seq_nr).await {
        tracing::error!(%pid, error = %e, "failed to save snapshot");
        return;
    }
    if let Err(e) = journal.delete_events_before(pid, seq_nr).await {
        tracing::error!(%pid, error = %e, "failed to compact event log after snapshot");
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
    use crate::journal::InMemoryJournal;
    use crate::reply::ReplyTo;
    use crate::runtime::ActorRef;
    use crate::system::ActorSystem;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;
    use tokio::sync::oneshot;

    // A counter that persists every increment and snapshots on demand.
    struct Counter {
        id: String,
        // Lets a test observe the recovered value at startup.
        report: Option<oneshot::Sender<i64>>,
        // Records the state value seen by each `on_events_persisted` call.
        persisted: Arc<std::sync::Mutex<Vec<i64>>>,
    }

    impl Counter {
        fn new(id: &str) -> Self {
            Self {
                id: id.into(),
                report: None,
                persisted: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn reporting(id: &str, report: oneshot::Sender<i64>) -> Self {
            Self {
                report: Some(report),
                ..Self::new(id)
            }
        }
    }

    enum CounterCmd {
        Inc(i64),
        /// Increment, replying with the durable-write outcome.
        IncAck(i64, ReplyTo<Result<(), JournalError>>),
        Snapshot,
        Get(ReplyTo<i64>),
        /// The checked read: answer only once the journal confirms the log has
        /// not moved past this instance.
        GetChecked(ReplyTo<i64>),
        Stop,
    }

    /// What the parent in `spawned_child_recovers_independently` accepts.
    /// Declared here because `Counter` names it as its parent's command type.
    enum ParentCmd {
        Start,
        ChildValue(ReplyTo<i64>),
    }

    #[derive(Serialize, Deserialize, Clone)]
    enum CounterEvent {
        Incremented(i64),
    }

    #[derive(Serialize, Deserialize, Default, Clone)]
    struct CounterState {
        value: i64,
    }

    #[async_trait]
    impl EventSourcedActor for Counter {
        type Command = CounterCmd;
        type Event = CounterEvent;
        type State = CounterState;
        // Counter is created both at the top of the tree and as a child of the
        // parent below, so it names what its parent accepts, not who it is.

        fn persistence_id(&self) -> PersistenceId {
            PersistenceId::new("counter", self.id.clone())
        }

        fn initial_state() -> CounterState {
            CounterState::default()
        }

        fn apply_event(mut state: CounterState, event: CounterEvent) -> CounterState {
            match event {
                CounterEvent::Incremented(n) => state.value += n,
            }
            state
        }

        async fn handle_command(
            &mut self,
            state: &CounterState,
            cmd: CounterCmd,
            _ctx: &mut ActorContext<CounterCmd>,
        ) -> CommandEffect<CounterEvent> {
            match cmd {
                CounterCmd::Inc(n) => CommandEffect::persist(vec![CounterEvent::Incremented(n)]),
                CounterCmd::IncAck(n, ack) => {
                    CommandEffect::persist(vec![CounterEvent::Incremented(n)]).and_ack(ack)
                }
                CounterCmd::Snapshot => CommandEffect::snapshot(),
                CounterCmd::Get(reply) => {
                    let _ = reply.send(state.value);
                    CommandEffect::none()
                }
                CounterCmd::GetChecked(reply) => {
                    let value = state.value;
                    CommandEffect::none().and_confirm(move |check| {
                        if check.is_ok() {
                            let _ = reply.send(value);
                        }
                        // Dropped otherwise: the caller fails now rather than
                        // reading a history that has moved on.
                    })
                }
                CounterCmd::Stop => CommandEffect::stop(),
            }
        }

        async fn on_recovery_complete(
            &mut self,
            state: &CounterState,
            _ctx: &mut ActorContext<CounterCmd>,
        ) {
            if let Some(tx) = self.report.take() {
                let _ = tx.send(state.value);
            }
        }

        async fn on_events_persisted(&mut self, _events: &[CounterEvent], state: &CounterState) {
            self.persisted
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(state.value);
        }
    }

    async fn current_value(actor: &ActorRef<CounterCmd>) -> i64 {
        actor.ask(CounterCmd::Get).await.unwrap()
    }

    #[tokio::test]
    async fn persists_and_applies_events() {
        let system = ActorSystem::in_memory();
        let actor = system
            .actor_of("c1", system.persistent(Counter::new("c1")))
            .unwrap();
        actor.tell(CounterCmd::Inc(3)).await.unwrap();
        actor.tell(CounterCmd::Inc(4)).await.unwrap();
        assert_eq!(current_value(&actor).await, 7);
    }

    #[tokio::test]
    async fn ask_with_persist_and_ack_returns_after_durable_write() {
        let system = ActorSystem::in_memory();
        let actor = system
            .actor_of("ack", system.persistent(Counter::new("ack")))
            .unwrap();
        // `ask` resolves only when the actor replies, and `and_ack` replies
        // *after* the event is persisted and folded — so the new value is already
        // observable the instant `ask` returns, and the reply reports success. This
        // is the backpressure + durability guarantee callers rely on.
        let durable = actor.ask(|ack| CounterCmd::IncAck(5, ack)).await.unwrap();
        assert!(durable.is_ok(), "in-memory journal write should succeed");
        assert_eq!(current_value(&actor).await, 5);
    }

    #[tokio::test]
    async fn ask_with_persist_and_ack_reports_journal_failure() {
        // A journal whose `persist` always fails, to prove the ack surfaces the
        // durable-write failure to the asker rather than acking success on a
        // write that never landed.
        let journal = Arc::new(
            crate::testkit::FaultyJournal::wrapping(InMemoryJournal::new()).fail_persist_after(0),
        );
        let system = ActorSystem::new(journal);
        let actor = system
            .actor_of("fail", system.persistent(Counter::new("fail")))
            .unwrap();
        let durable = actor.ask(|ack| CounterCmd::IncAck(5, ack)).await.unwrap();
        assert!(durable.is_err(), "failed journal write must report Err");
        // State was left unchanged because the events were never folded.
        assert_eq!(current_value(&actor).await, 0);
    }

    #[tokio::test]
    async fn on_events_persisted_runs_after_the_fold() {
        let system = ActorSystem::in_memory();
        let counter = Counter::new("hook");
        let seen = counter.persisted.clone();
        let actor = system.actor_of("hook", system.persistent(counter)).unwrap();
        actor.tell(CounterCmd::Inc(3)).await.unwrap();
        actor.tell(CounterCmd::Inc(4)).await.unwrap();
        // Forces both commands through the mailbox before we assert.
        assert_eq!(current_value(&actor).await, 7);
        // The hook sees state AFTER the fold, so 3 then 7 — never the pre-fold 0.
        // That ordering is what lets an observer publish durable facts.
        assert_eq!(*seen.lock().unwrap(), vec![3, 7]);
    }

    #[tokio::test]
    async fn on_events_persisted_is_skipped_when_the_write_fails() {
        let journal = Arc::new(
            crate::testkit::FaultyJournal::wrapping(InMemoryJournal::new()).fail_persist_after(0),
        );
        let system = ActorSystem::new(journal);
        let counter = Counter::new("hookfail");
        let seen = counter.persisted.clone();
        let actor = system
            .actor_of("hookfail", system.persistent(counter))
            .unwrap();
        let durable = actor.ask(|ack| CounterCmd::IncAck(5, ack)).await.unwrap();
        assert!(durable.is_err(), "the faulty journal must reject the write");
        // Nothing was journaled, so nothing may be published — otherwise an
        // observer would announce history that does not exist.
        assert!(seen.lock().unwrap().is_empty());
    }

    /// #37, both halves of the trade. A plain read is answered from memory —
    /// free, journal untouched, and stale by at most the liveness window when
    /// the instance has been displaced. A checked read pays one journal round
    /// trip and refuses instead.
    #[tokio::test]
    async fn a_checked_read_refuses_what_a_plain_read_answers_stale() {
        let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
        let system = ActorSystem::new(journal.clone());
        let actor = system
            .actor_of("reader", system.persistent(Counter::new("reader")))
            .unwrap();
        actor.tell(CounterCmd::Inc(5)).await.unwrap();
        assert_eq!(current_value(&actor).await, 5);

        // Somebody else appends — the instance is now a displaced reader, and
        // nothing tells it.
        let rival = serde_json::to_vec(&CounterEvent::Incremented(10)).unwrap();
        journal
            .persist(&PersistenceId::new("counter", "reader"), &[rival], 1)
            .await
            .unwrap();

        // Plain reads keep answering from memory, and keep the instance alive:
        // a read touches no journal, so the fence has nothing to catch. Twice,
        // to prove the first one did not quietly stop the actor either.
        assert_eq!(current_value(&actor).await, 5);
        assert_eq!(current_value(&actor).await, 5);

        // The checked read is where the displacement is caught: the empty
        // conditional append conflicts, the reply handle is dropped, and the
        // caller fails now instead of acting on history.
        assert!(
            actor.ask(CounterCmd::GetChecked).await.is_err(),
            "a checked read on a displaced instance answered anyway"
        );
    }

    /// A checked read on an instance nobody displaced answers normally — the
    /// check is a fence, not a tax on being right.
    #[tokio::test]
    async fn a_checked_read_answers_when_the_log_has_not_moved() {
        let system = ActorSystem::in_memory();
        let actor = system
            .actor_of("fine", system.persistent(Counter::new("fine")))
            .unwrap();
        actor.tell(CounterCmd::Inc(3)).await.unwrap();
        assert_eq!(actor.ask(CounterCmd::GetChecked).await.unwrap(), 3);
    }

    /// Counts conditional appends, delegating everything to the journal it
    /// wraps — what makes "a read costs no journal round trip" assertable.
    struct CountingJournal {
        inner: InMemoryJournal,
        persists: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl Journal for CountingJournal {
        async fn persist(
            &self,
            pid: &PersistenceId,
            events: &[Vec<u8>],
            expected_last_seq: u64,
        ) -> crate::journal::JournalResult<()> {
            self.persists
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.persist(pid, events, expected_last_seq).await
        }
        async fn replay(
            &self,
            pid: &PersistenceId,
            after_seq: u64,
        ) -> futures_util::stream::BoxStream<'_, crate::journal::JournalResult<(u64, Vec<u8>)>>
        {
            self.inner.replay(pid, after_seq).await
        }
        async fn save_snapshot(
            &self,
            pid: &PersistenceId,
            state: Vec<u8>,
            seq_nr: u64,
        ) -> crate::journal::JournalResult<()> {
            self.inner.save_snapshot(pid, state, seq_nr).await
        }
        async fn latest_snapshot(
            &self,
            pid: &PersistenceId,
        ) -> crate::journal::JournalResult<Option<(Vec<u8>, u64)>> {
            self.inner.latest_snapshot(pid).await
        }
        async fn delete_events_before(
            &self,
            pid: &PersistenceId,
            seq_nr: u64,
        ) -> crate::journal::JournalResult<()> {
            self.inner.delete_events_before(pid, seq_nr).await
        }
        async fn copy_snapshot(
            &self,
            from: &PersistenceId,
            to: &PersistenceId,
        ) -> crate::journal::JournalResult<()> {
            self.inner.copy_snapshot(from, to).await
        }
        async fn last_seq(&self, pid: &PersistenceId) -> crate::journal::JournalResult<u64> {
            self.inner.last_seq(pid).await
        }
        async fn clear(&self, pid: &PersistenceId) -> crate::journal::JournalResult<()> {
            self.inner.clear(pid).await
        }
    }

    /// The cost model, pinned: a plain read makes zero journal calls, and a
    /// checked read makes exactly the one it opted into.
    #[tokio::test]
    async fn a_plain_read_touches_no_journal_and_a_checked_read_touches_it_once() {
        let journal = Arc::new(CountingJournal {
            inner: InMemoryJournal::new(),
            persists: std::sync::atomic::AtomicUsize::new(0),
        });
        let counting = journal.clone();
        let persists = move || counting.persists.load(std::sync::atomic::Ordering::Relaxed);

        let system = ActorSystem::new(journal);
        let actor = system
            .actor_of("free", system.persistent(Counter::new("free")))
            .unwrap();

        assert_eq!(current_value(&actor).await, 0);
        assert_eq!(current_value(&actor).await, 0);
        assert_eq!(persists(), 0, "a plain read paid a journal round trip");

        assert_eq!(actor.ask(CounterCmd::GetChecked).await.unwrap(), 0);
        assert_eq!(persists(), 1, "a checked read is exactly one round trip");
    }

    #[tokio::test]
    async fn recovers_state_from_event_log_after_restart() {
        let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
        let system = ActorSystem::new(journal.clone());

        // First incarnation persists some events, then stops.
        let a1 = system
            .actor_of("c2", system.persistent(Counter::new("c2")))
            .unwrap();
        a1.tell(CounterCmd::Inc(5)).await.unwrap();
        a1.tell(CounterCmd::Inc(10)).await.unwrap();
        // Ensure the increments are processed before we drop and "crash".
        assert_eq!(current_value(&a1).await, 15);
        a1.tell(CounterCmd::Stop).await.unwrap();

        // Second incarnation reuses the same persistence_id and journal.
        let (report_tx, report_rx) = oneshot::channel();
        let revived = ActorSystem::new(journal);
        let _a2 = revived
            .actor_of(
                "c2",
                revived.persistent(Counter::reporting("c2", report_tx)),
            )
            .unwrap();
        // Recovery folds the two events back to 15.
        assert_eq!(report_rx.await.unwrap(), 15);
    }

    #[tokio::test]
    async fn recovers_from_snapshot_after_compaction() {
        let journal: Arc<dyn Journal> = Arc::new(InMemoryJournal::new());
        let system = ActorSystem::new(journal.clone());

        let a1 = system
            .actor_of("c3", system.persistent(Counter::new("c3")))
            .unwrap();
        a1.tell(CounterCmd::Inc(2)).await.unwrap();
        a1.tell(CounterCmd::Inc(2)).await.unwrap();
        a1.tell(CounterCmd::Snapshot).await.unwrap();
        a1.tell(CounterCmd::Inc(1)).await.unwrap();
        assert_eq!(current_value(&a1).await, 5);
        a1.tell(CounterCmd::Stop).await.unwrap();

        // Confirm the snapshot compacted the pre-snapshot events.
        let count = {
            let mut remaining = journal
                .replay(&PersistenceId::new("counter", "c3"), 0)
                .await;
            let mut count = 0;
            while let Some(item) = remaining.next().await {
                item.unwrap();
                count += 1;
            }
            count
        };
        // Only the single post-snapshot increment should remain in the log.
        assert_eq!(count, 1);

        let (report_tx, report_rx) = oneshot::channel();
        let revived = ActorSystem::new(journal);
        let _a2 = revived
            .actor_of(
                "c3",
                revived.persistent(Counter::reporting("c3", report_tx)),
            )
            .unwrap();
        // snapshot (4) + replayed post-snapshot event (1) == 5.
        assert_eq!(report_rx.await.unwrap(), 5);
    }

    #[tokio::test]
    async fn spawned_child_recovers_independently() {
        // A parent that spawns a child counter and forwards a value to it.
        struct Parent {
            child: Option<ActorRef<CounterCmd>>,
        }
        #[derive(Serialize, Deserialize, Default)]
        struct Empty {}

        #[async_trait]
        impl EventSourcedActor for Parent {
            type Command = ParentCmd;
            type Event = ();
            type State = Empty;
            fn persistence_id(&self) -> PersistenceId {
                PersistenceId::new("parent", "parent")
            }
            fn initial_state() -> Empty {
                Empty::default()
            }
            fn apply_event(state: Empty, _e: ()) -> Empty {
                state
            }
            async fn handle_command(
                &mut self,
                _state: &Empty,
                cmd: ParentCmd,
                ctx: &mut ActorContext<ParentCmd>,
            ) -> CommandEffect<()> {
                match cmd {
                    ParentCmd::Start => {
                        let child = ctx
                            .actor_of("child", ctx.persistent(Counter::new("child")))
                            .unwrap();
                        child.tell(CounterCmd::Inc(42)).await.unwrap();
                        self.child = Some(child);
                        CommandEffect::none()
                    }
                    ParentCmd::ChildValue(reply) => {
                        if let Some(child) = &self.child {
                            let v = current_value(child).await;
                            let _ = reply.send(v);
                        }
                        CommandEffect::none()
                    }
                }
            }
        }

        let system = ActorSystem::in_memory();
        let parent = system
            .actor_of("parent", system.persistent(Parent { child: None }))
            .unwrap();
        parent.tell(ParentCmd::Start).await.unwrap();
        // Give the child a moment to process the increment.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let value = parent.ask(ParentCmd::ChildValue).await.unwrap();
        assert_eq!(value, 42);
    }
}
