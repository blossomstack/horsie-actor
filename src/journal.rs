use crate::envelope::Epoch;
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
    /// Append `events` to `pid`'s log, assigning each the next sequence number.
    ///
    /// `fence` is the writer's claim on this instance. `None` means nothing is
    /// arbitrating ownership — a single-process deployment — and the write
    /// always proceeds. `Some(epoch)` obliges the backend to record the highest
    /// epoch it has seen for `pid` and reject anything lower with
    /// [`JournalError::Fenced`], **in the same transaction as the append**.
    ///
    /// That last clause is the whole point, and it is why this is a parameter
    /// rather than a wrapper: a decorator cannot join a transaction it does not
    /// open, so it would check ownership and append in two steps, which is
    /// exactly the race the fence exists to close.
    ///
    /// A backend that cannot enforce a fence must return an error for
    /// `Some(_)`. Ignoring it would present a fence that does not fence, which
    /// is worse than having none.
    async fn persist(
        &self,
        pid: &PersistenceId,
        events: &[Vec<u8>],
        fence: Option<Epoch>,
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
    async fn save_snapshot(
        &self,
        pid: &PersistenceId,
        state: Vec<u8>,
        seq_nr: u64,
        fence: Option<Epoch>,
    ) -> JournalResult<()>;

    /// Return the latest snapshot for `pid` as `(state, seq_nr)`, if any.
    async fn latest_snapshot(&self, pid: &PersistenceId) -> JournalResult<Option<(Vec<u8>, u64)>>;

    /// Drop all events for `pid` with sequence number less than or equal to `seq_nr`.
    async fn delete_events_before(&self, pid: &PersistenceId, seq_nr: u64) -> JournalResult<()>;

    /// Copy the snapshot from `from` onto `to`. `to` keeps the source's snapshot
    /// sequence number and starts with an empty event log, so a fresh actor recovers
    /// the copied state and continues numbering from there.
    async fn copy_snapshot(&self, from: &PersistenceId, to: &PersistenceId) -> JournalResult<()>;

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
    /// Highest ownership epoch this log has accepted a write at.
    epoch: Epoch,
}

/// Check `fence` against `entry`, and adopt it when it is current.
///
/// Returns `Err` without touching anything, so a fenced write leaves the log
/// exactly as it was.
fn admit(entry: &mut Entry, pid: &PersistenceId, fence: Option<Epoch>) -> JournalResult<()> {
    let Some(attempted) = fence else {
        return Ok(());
    };
    if attempted < entry.epoch {
        return Err(JournalError::Fenced {
            pid: pid.to_string(),
            current: entry.epoch,
            attempted,
        });
    }
    entry.epoch = attempted;
    Ok(())
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
        fence: Option<Epoch>,
    ) -> JournalResult<()> {
        let mut map = self.inner.lock();
        let entry = map.entry(pid.clone()).or_default();
        admit(entry, pid, fence)?;
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
        fence: Option<Epoch>,
    ) -> JournalResult<()> {
        let mut map = self.inner.lock();
        let entry = map.entry(pid.clone()).or_default();
        admit(entry, pid, fence)?;
        entry.last_seq = entry.last_seq.max(seq_nr);
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
                last_seq: seq,
                snapshot: Some(snapshot),
                // A copy starts unowned. The destination is a fresh instance
                // and whoever hosts it claims it; inheriting the source's epoch
                // would fence out its first legitimate writer.
                epoch: Epoch::default(),
            },
        );
        Ok(())
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
        j.persist(&pid("a"), &[vec![1], vec![2], vec![3]], None)
            .await
            .unwrap();
        assert_eq!(drain(&j, "a", 0).await, vec![vec![1], vec![2], vec![3]]);
    }

    #[tokio::test]
    async fn logs_are_namespaced_by_kind() {
        let j = InMemoryJournal::new();
        j.persist(&PersistenceId::new("workflow", "x"), &[vec![1]], None)
            .await
            .unwrap();
        j.persist(&PersistenceId::new("agent", "x"), &[vec![2]], None)
            .await
            .unwrap();
        // Same id, different kind → separate logs.
        let mut wf = j.replay(&PersistenceId::new("workflow", "x"), 0).await;
        let mut ag = j.replay(&PersistenceId::new("agent", "x"), 0).await;
        assert_eq!(wf.next().await.unwrap().unwrap(), (1, vec![1]));
        assert_eq!(ag.next().await.unwrap().unwrap(), (1, vec![2]));
    }

    #[tokio::test]
    async fn replay_skips_events_at_or_before_after_seq() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1], vec![2], vec![3]], None)
            .await
            .unwrap();
        assert_eq!(drain(&j, "a", 1).await, vec![vec![2], vec![3]]);
    }

    #[tokio::test]
    async fn snapshot_roundtrips_with_seq() {
        let j = InMemoryJournal::new();
        j.save_snapshot(&pid("a"), vec![9, 9], 5, None)
            .await
            .unwrap();
        assert_eq!(
            j.latest_snapshot(&pid("a")).await.unwrap(),
            Some((vec![9, 9], 5))
        );
    }

    #[tokio::test]
    async fn delete_events_before_compacts() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1], vec![2], vec![3]], None)
            .await
            .unwrap();
        j.delete_events_before(&pid("a"), 2).await.unwrap();
        assert_eq!(drain(&j, "a", 0).await, vec![vec![3]]);
    }

    #[tokio::test]
    async fn persist_continues_numbering_after_compaction() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1], vec![2]], None)
            .await
            .unwrap();
        j.delete_events_before(&pid("a"), 2).await.unwrap();
        j.persist(&pid("a"), &[vec![3]], None).await.unwrap();
        assert_eq!(drain(&j, "a", 2).await, vec![vec![3]]);
    }

    #[tokio::test]
    async fn copy_snapshot_seeds_new_id() {
        let j = InMemoryJournal::new();
        j.persist(&pid("src"), &[vec![1], vec![2]], None)
            .await
            .unwrap();
        j.save_snapshot(&pid("src"), vec![7], 2, None)
            .await
            .unwrap();
        j.copy_snapshot(&pid("src"), &pid("dst")).await.unwrap();
        assert_eq!(
            j.latest_snapshot(&pid("dst")).await.unwrap(),
            Some((vec![7], 2))
        );
        assert!(drain(&j, "dst", 2).await.is_empty());
        j.persist(&pid("dst"), &[vec![8]], None).await.unwrap();
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
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod fence_tests {
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

    /// A stale owner's write is rejected, not merged.
    ///
    /// This is the whole defence against two hosts believing they own one
    /// instance: deciding the owner can be briefly wrong, the fence cannot.
    #[tokio::test]
    async fn a_write_below_the_current_epoch_is_rejected() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1]], Some(Epoch(7)))
            .await
            .unwrap();

        let err = j
            .persist(&pid("a"), &[vec![2]], Some(Epoch(6)))
            .await
            .unwrap_err();
        assert!(matches!(err, JournalError::Fenced { .. }), "got {err:?}");

        // Nothing from the rejected write landed.
        assert_eq!(drain(&j, "a").await, vec![vec![1]]);
    }

    /// The same epoch keeps working — an owner writes many times per assignment,
    /// so equality has to be admitted or the second write of every generation
    /// would fail.
    #[tokio::test]
    async fn a_write_at_the_current_epoch_is_admitted() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1]], Some(Epoch(3)))
            .await
            .unwrap();
        j.persist(&pid("a"), &[vec![2]], Some(Epoch(3)))
            .await
            .unwrap();
        assert_eq!(drain(&j, "a").await, vec![vec![1], vec![2]]);
    }

    /// A newer epoch takes over and moves the floor up, so the host it replaced
    /// is locked out from that moment on.
    #[tokio::test]
    async fn a_newer_epoch_takes_ownership_and_locks_out_the_old_one() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1]], Some(Epoch(1)))
            .await
            .unwrap();
        j.persist(&pid("a"), &[vec![2]], Some(Epoch(2)))
            .await
            .unwrap();
        let err = j
            .persist(&pid("a"), &[vec![3]], Some(Epoch(1)))
            .await
            .unwrap_err();
        assert!(matches!(err, JournalError::Fenced { .. }));
        assert_eq!(drain(&j, "a").await, vec![vec![1], vec![2]]);
    }

    /// Snapshots are fenced too. A stale owner that only ever snapshots would
    /// otherwise overwrite the state of the log it no longer owns.
    #[tokio::test]
    async fn snapshots_are_fenced_as_well_as_appends() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1]], Some(Epoch(5)))
            .await
            .unwrap();
        let err = j
            .save_snapshot(&pid("a"), vec![9], 1, Some(Epoch(4)))
            .await
            .unwrap_err();
        assert!(matches!(err, JournalError::Fenced { .. }));
        assert!(j.latest_snapshot(&pid("a")).await.unwrap().is_none());
    }

    /// No fence means no arbitration — a single-process deployment behaves
    /// exactly as it did before this existed.
    #[tokio::test]
    async fn no_fence_never_rejects() {
        let j = InMemoryJournal::new();
        j.persist(&pid("a"), &[vec![1]], Some(Epoch(9)))
            .await
            .unwrap();
        j.persist(&pid("a"), &[vec![2]], None).await.unwrap();
        assert_eq!(drain(&j, "a").await, vec![vec![1], vec![2]]);
    }
}
