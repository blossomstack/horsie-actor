use crate::error::JournalError;
use crate::persistence_id::PersistenceId;
use async_trait::async_trait;
use futures_util::stream::{self, BoxStream, StreamExt};
use parking_lot::Mutex;
use std::collections::HashMap;

/// Result alias for journal operations.
pub type JournalResult<T> = Result<T, JournalError>;

/// Append-only event log with snapshot support.
///
/// Events and snapshots are opaque byte blobs — serialization is the caller's
/// concern, keeping the journal free of any domain types. Each log is identified by
/// a [`PersistenceId`] (actor kind + instance id). Sequence numbers are 1-based and
/// monotonic per `PersistenceId`; an event's sequence number is stable for the life
/// of the log even after older events are compacted away.
#[async_trait]
pub trait Journal: Send + Sync + 'static {
    /// Append `events` to `pid`'s log, assigning each the next sequence number —
    /// but only if the log currently ends at `expected_last_seq`.
    ///
    /// The condition and the append are **one operation**. A backend must not
    /// read the current sequence number and then write; that gap is precisely
    /// the race this exists to close, and it is why this is a parameter rather
    /// than a wrapper — a decorator cannot join a transaction it does not open.
    ///
    /// A mismatch returns [`JournalError::Conflict`] and leaves the log
    /// untouched. It means the writer's state is stale: some other writer has
    /// appended since. That is the entire fence, and it needs no notion of
    /// ownership, no claim step, and no coordination — a writer that is behind
    /// is caught by being behind. Crucially it holds even for a writer that was
    /// frozen through a failover and woke up still believing it owned the log,
    /// which no amount of checking-before-writing can achieve.
    ///
    /// `0` is the expectation for an empty log, so a fresh actor's first write
    /// passes only if nobody else got there first.
    ///
    /// A backend that cannot express the condition atomically must return an
    /// error rather than append unconditionally. Presenting a fence that does
    /// not fence is worse than having none.
    async fn persist(
        &self,
        pid: &PersistenceId,
        events: &[Vec<u8>],
        expected_last_seq: u64,
    ) -> JournalResult<()>;

    /// Stream every event for `pid` whose sequence number is strictly greater than
    /// `after_seq`, in ascending sequence order, as `(seq_nr, bytes)`.
    ///
    /// The sequence number is the journal's, not a count of what it yielded: a
    /// caller must never re-derive it by counting, because compaction leaves the
    /// survivors' numbers untouched and a snapshot recorded at a counted number
    /// would make the next `replay(after_seq)` skip or repeat events.
    async fn replay(
        &self,
        pid: &PersistenceId,
        after_seq: u64,
    ) -> BoxStream<'_, JournalResult<(u64, Vec<u8>)>>;

    /// Store `state` as the snapshot for `pid`, taken at sequence `seq_nr` (the
    /// sequence number of the last event folded into it). Replaces any prior snapshot.
    ///
    /// Conditional in the same way as [`persist`](Journal::persist), and on the
    /// same value: a snapshot is a claim about what the log contains, so a
    /// writer whose log has moved on is claiming something false. `seq_nr` is
    /// both the snapshot's sequence number and the expectation.
    async fn save_snapshot(
        &self,
        pid: &PersistenceId,
        state: Vec<u8>,
        seq_nr: u64,
    ) -> JournalResult<()>;

    /// Return the latest snapshot for `pid` as `(state, seq_nr)`, if any.
    async fn latest_snapshot(&self, pid: &PersistenceId) -> JournalResult<Option<(Vec<u8>, u64)>>;

    /// Drop all events for `pid` with sequence number less than or equal to `seq_nr`.
    ///
    /// Unconditional, and safe only because it runs strictly after a successful
    /// [`save_snapshot`](Journal::save_snapshot) at the same `seq_nr`. A stale
    /// writer's snapshot is rejected, so it never reaches this call — which is
    /// what stops it deleting events it has never seen. That ordering is
    /// load-bearing; see `snapshot_state` in `persistent.rs`.
    async fn delete_events_before(&self, pid: &PersistenceId, seq_nr: u64) -> JournalResult<()>;

    /// Copy the snapshot from `from` onto `to`. `to` keeps the source's snapshot
    /// sequence number and starts with an empty event log, so a fresh actor recovers
    /// the copied state and continues numbering from there.
    async fn copy_snapshot(&self, from: &PersistenceId, to: &PersistenceId) -> JournalResult<()>;

    /// The sequence number `pid`'s log currently ends at, or `0` if it is empty.
    ///
    /// A read, with no bearing on safety — a writer never needs to ask, because
    /// it already knows what it has written and the conditional append catches
    /// it when that belief is wrong. This is for observers.
    async fn last_seq(&self, pid: &PersistenceId) -> JournalResult<u64>;

    /// Remove all persisted state for `pid`. Primarily a test helper.
    async fn clear(&self, pid: &PersistenceId) -> JournalResult<()>;
}

#[derive(Default)]
struct Entry {
    /// `(seq_nr, bytes)` in ascending sequence order.
    events: Vec<(u64, Vec<u8>)>,
    /// Sequence number of the most recently assigned event (0 = none yet).
    last_seq: u64,
    snapshot: Option<(Vec<u8>, u64)>,
}

/// Reject the write unless the log still ends where the writer believes.
///
/// Returns `Err` without touching anything, so a stale write leaves the log
/// exactly as it was.
fn admit(entry: &Entry, pid: &PersistenceId, expected_last_seq: u64) -> JournalResult<()> {
    if entry.last_seq == expected_last_seq {
        return Ok(());
    }
    Err(JournalError::Conflict {
        pid: pid.to_string(),
        expected: expected_last_seq,
        actual: entry.last_seq,
    })
}

/// In-memory [`Journal`] for tests and single-process runs.
#[derive(Default)]
pub struct InMemoryJournal {
    inner: Mutex<HashMap<PersistenceId, Entry>>,
}

impl InMemoryJournal {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Journal for InMemoryJournal {
    async fn persist(
        &self,
        pid: &PersistenceId,
        events: &[Vec<u8>],
        expected_last_seq: u64,
    ) -> JournalResult<()> {
        let mut map = self.inner.lock();
        let entry = map.entry(pid.clone()).or_default();
        admit(entry, pid, expected_last_seq)?;
        for bytes in events {
            entry.last_seq += 1;
            entry.events.push((entry.last_seq, bytes.clone()));
        }
        Ok(())
    }

    async fn replay(
        &self,
        pid: &PersistenceId,
        after_seq: u64,
    ) -> BoxStream<'_, JournalResult<(u64, Vec<u8>)>> {
        let items: Vec<JournalResult<(u64, Vec<u8>)>> = {
            let map = self.inner.lock();
            map.get(pid)
                .map(|e| {
                    e.events
                        .iter()
                        .filter(|(seq, _)| *seq > after_seq)
                        .map(|(seq, bytes)| Ok((*seq, bytes.clone())))
                        .collect()
                })
                .unwrap_or_default()
        };
        stream::iter(items).boxed()
    }

    async fn save_snapshot(
        &self,
        pid: &PersistenceId,
        state: Vec<u8>,
        seq_nr: u64,
    ) -> JournalResult<()> {
        let mut map = self.inner.lock();
        let entry = map.entry(pid.clone()).or_default();
        admit(entry, pid, seq_nr)?;
        entry.snapshot = Some((state, seq_nr));
        Ok(())
    }

    async fn latest_snapshot(&self, pid: &PersistenceId) -> JournalResult<Option<(Vec<u8>, u64)>> {
        Ok(self.inner.lock().get(pid).and_then(|e| e.snapshot.clone()))
    }

    async fn delete_events_before(&self, pid: &PersistenceId, seq_nr: u64) -> JournalResult<()> {
        let mut map = self.inner.lock();
        if let Some(entry) = map.get_mut(pid) {
            entry.events.retain(|(seq, _)| *seq > seq_nr);
        }
        Ok(())
    }

    async fn copy_snapshot(&self, from: &PersistenceId, to: &PersistenceId) -> JournalResult<()> {
        let mut map = self.inner.lock();
        let snapshot = map
            .get(from)
            .and_then(|e| e.snapshot.clone())
            .ok_or_else(|| JournalError::Backend(format!("no snapshot for '{from}'")))?;
        let seq = snapshot.1;
        map.insert(
            to.clone(),
            Entry {
                events: Vec::new(),
                // The destination inherits the source's numbering, so its first
                // writer must expect this sequence rather than an empty log.
                last_seq: seq,
                snapshot: Some(snapshot),
            },
        );
        Ok(())
    }

    async fn last_seq(&self, pid: &PersistenceId) -> JournalResult<u64> {
        Ok(self.inner.lock().get(pid).map_or(0, |e| e.last_seq))
    }

    async fn clear(&self, pid: &PersistenceId) -> JournalResult<()> {
        self.inner.lock().remove(pid);
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

    fn pid(id: &str) -> PersistenceId {
        PersistenceId::new("t", id)
    }

    async fn drain(j: &InMemoryJournal, id: &str, after: u64) -> Vec<Vec<u8>> {
        let mut s = j.replay(&pid(id), after).await;
        let mut out = Vec::new();
        while let Some(item) = s.next().await {
            out.push(item.unwrap().1);
        }
        out
    }

    #[tokio::test]
    async fn persist_then_replay_returns_events_in_order() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1], vec![2], vec![3]], 0)
            .await
            .unwrap();
        assert_eq!(drain(&j, "a", 0).await, vec![vec![1], vec![2], vec![3]]);
    }

    #[tokio::test]
    async fn logs_are_namespaced_by_kind() {
        let j = InMemoryJournal::new();
        j.persist(&PersistenceId::new("workflow", "x"), &[vec![1]], 0)
            .await
            .unwrap();
        j.persist(&PersistenceId::new("agent", "x"), &[vec![2]], 0)
            .await
            .unwrap();
        // Same id, different kind → separate logs, so each expects its own
        // empty-log sequence of 0 rather than seeing the other's writes.
        let mut wf = j.replay(&PersistenceId::new("workflow", "x"), 0).await;
        let mut ag = j.replay(&PersistenceId::new("agent", "x"), 0).await;
        assert_eq!(wf.next().await.unwrap().unwrap(), (1, vec![1]));
        assert_eq!(ag.next().await.unwrap().unwrap(), (1, vec![2]));
    }

    #[tokio::test]
    async fn replay_skips_events_at_or_before_after_seq() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1], vec![2], vec![3]], 0)
            .await
            .unwrap();
        assert_eq!(drain(&j, "a", 1).await, vec![vec![2], vec![3]]);
    }

    #[tokio::test]
    async fn snapshot_roundtrips_with_seq() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1], vec![2]], 0).await.unwrap();
        j.save_snapshot(&pid("a"), vec![9, 9], 2).await.unwrap();
        assert_eq!(
            j.latest_snapshot(&pid("a")).await.unwrap(),
            Some((vec![9, 9], 2))
        );
    }

    #[tokio::test]
    async fn delete_events_before_compacts() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1], vec![2], vec![3]], 0)
            .await
            .unwrap();
        j.delete_events_before(&pid("a"), 2).await.unwrap();
        assert_eq!(drain(&j, "a", 0).await, vec![vec![3]]);
    }

    /// Compaction removes events but not their numbering, so the next write
    /// still expects the pre-compaction sequence. A journal that reset here
    /// would make every post-snapshot write look like a conflict.
    #[tokio::test]
    async fn persist_continues_numbering_after_compaction() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1], vec![2]], 0).await.unwrap();
        j.delete_events_before(&pid("a"), 2).await.unwrap();
        j.persist(&pid("a"), &[vec![3]], 2).await.unwrap();
        assert_eq!(drain(&j, "a", 2).await, vec![vec![3]]);
    }

    #[tokio::test]
    async fn copy_snapshot_seeds_new_id() {
        let j = InMemoryJournal::new();
        j.persist(&pid("src"), &[vec![1], vec![2]], 0)
            .await
            .unwrap();
        j.save_snapshot(&pid("src"), vec![7], 2).await.unwrap();
        j.copy_snapshot(&pid("src"), &pid("dst")).await.unwrap();
        assert_eq!(
            j.latest_snapshot(&pid("dst")).await.unwrap(),
            Some((vec![7], 2))
        );
        assert!(drain(&j, "dst", 2).await.is_empty());
        // The copy inherits the source's numbering, so the destination's first
        // writer expects 2 — not the 0 an empty log would take.
        j.persist(&pid("dst"), &[vec![8]], 2).await.unwrap();
        assert_eq!(drain(&j, "dst", 2).await, vec![vec![8]]);
    }

    #[tokio::test]
    async fn copy_snapshot_without_source_errors() {
        let j = InMemoryJournal::new();
        let err = j
            .copy_snapshot(&pid("missing"), &pid("dst"))
            .await
            .unwrap_err();
        assert!(matches!(err, JournalError::Backend(_)));
    }

    #[tokio::test]
    async fn last_seq_reports_where_the_log_ends() {
        let j = InMemoryJournal::new();
        assert_eq!(j.last_seq(&pid("a")).await.unwrap(), 0);
        j.persist(&pid("a"), &[vec![1], vec![2]], 0).await.unwrap();
        assert_eq!(j.last_seq(&pid("a")).await.unwrap(), 2);
    }
}

/// The write fence.
///
/// Every test here is about one guarantee: a writer whose picture of the log is
/// out of date cannot write. It is the only thing standing between two hosts
/// that both believe they own an instance and a single history spliced out of
/// two divergent ones, so each of these was confirmed to fail with the check
/// removed.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod conflict_tests {
    use super::*;

    fn pid(id: &str) -> PersistenceId {
        PersistenceId::new("t", id)
    }

    async fn drain(j: &InMemoryJournal, id: &str) -> Vec<Vec<u8>> {
        let mut s = j.replay(&pid(id), 0).await;
        let mut out = Vec::new();
        while let Some(item) = s.next().await {
            out.push(item.unwrap().1);
        }
        out
    }

    /// The case the fence exists for: two writers recovered at the same point,
    /// one wrote, and the other tries to write from the state it had before.
    #[tokio::test]
    async fn a_write_from_a_stale_writer_is_rejected() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1]], 0).await.unwrap();

        let err = j.persist(&pid("a"), &[vec![2]], 0).await.unwrap_err();
        assert!(
            matches!(
                err,
                JournalError::Conflict {
                    expected: 0,
                    actual: 1,
                    ..
                }
            ),
            "got {err:?}"
        );

        // Nothing from the rejected write landed.
        assert_eq!(drain(&j, "a").await, vec![vec![1]]);
    }

    /// A writer that keeps up keeps working: each write moves the expectation
    /// forward by exactly the number of events it appended.
    #[tokio::test]
    async fn consecutive_writes_advance_the_expectation() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1], vec![2]], 0).await.unwrap();
        j.persist(&pid("a"), &[vec![3]], 2).await.unwrap();
        assert_eq!(drain(&j, "a").await, vec![vec![1], vec![2], vec![3]]);
    }

    /// A writer ahead of the log is as wrong as one behind it, and for a worse
    /// reason — it is claiming events nobody has. Admitting it would leave a
    /// gap in the sequence that recovery could not detect.
    #[tokio::test]
    async fn a_write_ahead_of_the_log_is_rejected() {
        let j = InMemoryJournal::new();
        let err = j.persist(&pid("a"), &[vec![1]], 5).await.unwrap_err();
        assert!(matches!(err, JournalError::Conflict { .. }), "got {err:?}");
        assert!(drain(&j, "a").await.is_empty());
    }

    /// Snapshots carry the same condition. A stale writer that only ever
    /// snapshots would otherwise overwrite the state of a log that has moved
    /// past it — and that state is what the next recovery starts from, so it is
    /// the more destructive of the two writes, not the lesser one.
    #[tokio::test]
    async fn a_snapshot_from_a_stale_writer_is_rejected() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1], vec![2]], 0).await.unwrap();

        let err = j.save_snapshot(&pid("a"), vec![9], 1).await.unwrap_err();
        assert!(matches!(err, JournalError::Conflict { .. }), "got {err:?}");
        assert!(j.latest_snapshot(&pid("a")).await.unwrap().is_none());
    }

    /// The rejection is what stops a stale writer compacting: `persistent.rs`
    /// only compacts after a snapshot succeeds, so a rejected snapshot must
    /// leave the events it would have deleted in place.
    #[tokio::test]
    async fn a_rejected_snapshot_leaves_the_events_it_would_have_compacted() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1], vec![2]], 0).await.unwrap();
        assert!(j.save_snapshot(&pid("a"), vec![9], 1).await.is_err());
        assert_eq!(drain(&j, "a").await, vec![vec![1], vec![2]]);
    }

    /// A fresh log expects 0, so two writers racing to create the same instance
    /// do not both succeed.
    #[tokio::test]
    async fn only_one_writer_can_start_a_log() {
        let j = InMemoryJournal::new();
        j.persist(&pid("new"), &[vec![1]], 0).await.unwrap();
        assert!(j.persist(&pid("new"), &[vec![2]], 0).await.is_err());
        assert_eq!(drain(&j, "new").await, vec![vec![1]]);
    }
}
