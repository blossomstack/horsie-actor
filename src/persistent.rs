use crate::actor::{CommandEffect, EventSourcedActor};
use crate::behaviour::{Actor, Delivery, Flow, StartError};
use crate::cluster::Dedup;
use crate::error::JournalError;
use crate::journal::Journal;
use crate::persistence_id::PersistenceId;
use crate::runtime::ActorContext;
use async_trait::async_trait;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// How many applied message ids one actor remembers durably.
///
/// Per instance, not per node, so it can be far smaller than the node window:
/// it only has to outlast the retries aimed at this one actor. It rides in
/// every snapshot and is rebuilt on every recovery, so the bound is what keeps
/// both from growing with history. The trade is the same one every window
/// makes: a redelivery older than the last `DURABLE_DEDUP_WINDOW` applied
/// commands reads as new — durably bounded, not durably infinite.
pub(crate) const DURABLE_DEDUP_WINDOW: usize = 256;

/// One journaled event as [`Persistent`] encodes it.
///
/// The message id of the command that produced the event travels in the same
/// conditional append as the event itself — that atomicity is the whole point,
/// since an id recorded in a second write could land without its events or the
/// other way round. Recovery folds the event and re-learns the id in one pass.
#[derive(Serialize, Deserialize)]
struct EventRecord<E> {
    /// The producing command's dedup key, for a command that arrived from
    /// another node. Repeated on every event of the batch so recovery need not
    /// care where batches began.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message_id: Option<u128>,
    event: E,
}

/// A snapshot as [`Persistent`] encodes it: the folded state, plus the dedup
/// window at the moment it was taken.
///
/// The window has to ride here because snapshotting compacts the events —
/// and with them the ids they carried. A snapshot without the window would
/// make every compaction an amnesty for old retries.
#[derive(Serialize, Deserialize)]
struct SnapshotRecord<S> {
    state: S,
    /// Applied message ids, oldest first — the order that rebuilds the window
    /// exactly.
    seen: Vec<u128>,
}

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
    /// Message ids this instance has applied, recovered with everything else.
    ///
    /// The durable half of deduplication: the node window in dispatch cannot
    /// recognise a repeat that crossed a restart or a move, and this can,
    /// because it travels with the instance rather than with the process. An
    /// id joins it only once its events are durably written — recording on
    /// sight would make a failed write look applied to the retry that could
    /// have repaired it. Ids of commands that persisted nothing are held in
    /// memory only, which matches what there is to protect: re-applying a
    /// command with no events re-reads, it does not re-write.
    seen: Dedup,
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
            seen: Dedup::with_capacity(DURABLE_DEDUP_WINDOW),
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
        let (state, seq_nr, seen) = recover::<A>(&self.pid, &self.journal).await?;
        self.state = state;
        self.seq_nr = seq_nr;
        self.seen = seen;
        self.inner.on_recovery_complete(&self.state, ctx).await;
        Ok(())
    }

    async fn handle(&mut self, cmd: Self::Command, ctx: &mut ActorContext<Self::Command>) -> Flow {
        self.process(None, cmd, ctx).await
    }

    /// The durable dedup check, in front of the handler.
    ///
    /// A command whose id this instance's recovered state already holds has
    /// been applied — perhaps by an earlier incarnation on another host — so
    /// it is dropped here. Dropping it also drops any reply handle inside,
    /// which tells a still-waiting caller that no fresh answer is coming: the
    /// first application's answer either arrived or is not reconstructible,
    /// and pretending otherwise would mean applying the command again.
    async fn deliver(
        &mut self,
        delivery: Delivery<Self::Command>,
        ctx: &mut ActorContext<Self::Command>,
    ) -> Flow {
        if let Some(id) = delivery.message_id
            && self.seen.contains(id)
        {
            tracing::debug!(
                pid = %self.pid,
                message_id = id,
                "dropped a redelivery this instance has already applied"
            );
            return Flow::Continue;
        }
        self.process(delivery.message_id, delivery.cmd, ctx).await
    }
}

impl<A: EventSourcedActor> Persistent<A> {
    /// Handle one command, carrying the message id it arrived under so a
    /// successful persist can record it durably.
    async fn process(
        &mut self,
        message_id: Option<u128>,
        cmd: A::Command,
        ctx: &mut ActorContext<A::Command>,
    ) -> Flow {
        let effect = self.inner.handle_command(&self.state, cmd, ctx).await;
        let CommandEffect {
            events,
            snapshot,
            ack,
            stop,
        } = effect;

        // One persist step, then the post-persist actions in a fixed order:
        // write -> publish -> snapshot -> ack -> stop. The write outcome is
        // folded only on success, so a failed write leaves state consistent with
        // what is actually durable and the ack reports the failure.
        let (persisted, result) = persist_events::<A>(
            &self.pid,
            &self.journal,
            events,
            message_id,
            &mut self.state,
            &mut self.seq_nr,
        )
        .await;

        // The id becomes "seen" only now, on success. For a batch that wrote,
        // the events carry it durably; for a command that persisted nothing,
        // this in-memory record is all there is — and all there needs to be,
        // since re-applying it would re-read rather than re-write. A failed
        // write records nothing, so the retry it provokes is let through.
        if result.is_ok()
            && let Some(id) = message_id
        {
            self.seen.accept_id(id);
        }

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
            snapshot_state::<A>(
                &self.pid,
                &self.journal,
                &self.state,
                &self.seen,
                self.seq_nr,
            )
            .await;
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
/// Returns the recovered state, the sequence number of the last applied event,
/// and the dedup window as history left it — snapshot first, then the ids off
/// the replayed events, which is the same order they were applied in.
async fn recover<A: EventSourcedActor>(
    pid: &PersistenceId,
    journal: &Arc<dyn Journal>,
) -> Result<(A::State, u64, Dedup), JournalError> {
    let mut seen = Dedup::with_capacity(DURABLE_DEDUP_WINDOW);
    let (mut state, mut seq_nr) = match journal.latest_snapshot(pid).await? {
        Some((bytes, seq)) => {
            let snapshot = serde_json::from_slice::<SnapshotRecord<A::State>>(&bytes)
                .map_err(|e| JournalError::Serialization(e.to_string()))?;
            for id in snapshot.seen {
                seen.accept_id(id);
            }
            (snapshot.state, seq)
        }
        None => (A::initial_state(), 0),
    };

    let mut stream = journal.replay(pid, seq_nr).await;
    while let Some(item) = stream.next().await {
        // Take the journal's number rather than counting: after compaction the
        // survivors keep their original numbers, and this is the number a later
        // snapshot is recorded at.
        let (seq, bytes) = item?;
        let record = serde_json::from_slice::<EventRecord<A::Event>>(&bytes)
            .map_err(|e| JournalError::Serialization(e.to_string()))?;
        if let Some(id) = record.message_id {
            // A batch repeats its id on every event; `accept_id` records the
            // first and shrugs at the rest.
            seen.accept_id(id);
        }
        state = A::apply_event(state, record.event);
        seq_nr = seq;
    }
    Ok((state, seq_nr, seen))
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
    message_id: Option<u128>,
    state: &mut A::State,
    seq_nr: &mut u64,
) -> (Vec<A::Event>, Result<(), JournalError>) {
    let mut encoded = Vec::with_capacity(events.len());
    for event in &events {
        // The producing command's id rides in the same append as the event, so
        // the two are durable together or not at all.
        let record = EventRecord { message_id, event };
        match serde_json::to_vec(&record) {
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
    seen: &Dedup,
    seq_nr: u64,
) {
    // The window rides in the snapshot because compaction is about to delete
    // the events that carried its ids.
    let record = SnapshotRecord {
        state,
        seen: seen.ids().collect(),
    };
    let bytes = match serde_json::to_vec(&record) {
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
