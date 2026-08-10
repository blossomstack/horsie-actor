//! Fault-injecting [`Journal`] wrappers and on-disk fixtures.
//!
//! Gated behind `cfg(any(test, feature = "test-util"))`: available to the actor
//! crate's own tests unconditionally, and to `server` / `workflow` when they
//! enable `horsie-actor/test-util`.

use crate::error::JournalError;
use crate::journal::{Journal, JournalResult};
use crate::persistence_id::PersistenceId;
use async_trait::async_trait;
use futures_util::stream::{self, BoxStream, StreamExt};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Wraps any [`Journal`], failing selected operations on demand.
pub struct FaultyJournal<J> {
    inner: J,
    /// Number of `persist` calls to allow before failing; `None` = never fail.
    persist_budget: Option<usize>,
    persists: AtomicUsize,
    fail_snapshot: bool,
    /// Sequence number at which `replay` yields an error instead of the event.
    replay_fails_at: Option<u64>,
}

impl<J> FaultyJournal<J> {
    /// A healthy wrapper — every call delegates until a fault is configured.
    pub fn wrapping(inner: J) -> Self {
        Self {
            inner,
            persist_budget: None,
            persists: AtomicUsize::new(0),
            fail_snapshot: false,
            replay_fails_at: None,
        }
    }

    /// Allow `n` successful persists, then fail every one after.
    #[must_use]
    pub fn fail_persist_after(mut self, n: usize) -> Self {
        self.persist_budget = Some(n);
        self
    }

    /// Fail every `save_snapshot`.
    #[must_use]
    pub fn fail_snapshot(mut self) -> Self {
        self.fail_snapshot = true;
        self
    }

    /// Yield an error in place of the event at `seq`, ending the replay there.
    #[must_use]
    pub fn fail_replay_at(mut self, seq: u64) -> Self {
        self.replay_fails_at = Some(seq);
        self
    }
}

#[async_trait]
impl<J: Journal> Journal for FaultyJournal<J> {
    async fn persist(
        &self,
        pid: &PersistenceId,
        events: &[Vec<u8>],
        expected_last_seq: u64,
    ) -> JournalResult<()> {
        if let Some(budget) = self.persist_budget
            && self.persists.fetch_add(1, Ordering::Relaxed) >= budget
        {
            return Err(JournalError::Backend("injected persist failure".into()));
        }
        self.inner.persist(pid, events, expected_last_seq).await
    }

    async fn replay(
        &self,
        pid: &PersistenceId,
        after_seq: u64,
    ) -> BoxStream<'_, JournalResult<(u64, Vec<u8>)>> {
        let Some(fail_at) = self.replay_fails_at else {
            return self.inner.replay(pid, after_seq).await;
        };
        let mut out: Vec<JournalResult<(u64, Vec<u8>)>> = Vec::new();
        let mut inner = self.inner.replay(pid, after_seq).await;
        while let Some(item) = inner.next().await {
            // Fail at the journal's own numbering, so an injected failure lands
            // at the same event whether or not the log has been compacted.
            if item.as_ref().is_ok_and(|(seq, _)| *seq >= fail_at) {
                out.push(Err(JournalError::Backend("injected replay failure".into())));
                break;
            }
            out.push(item);
        }
        stream::iter(out).boxed()
    }

    async fn save_snapshot(
        &self,
        pid: &PersistenceId,
        state: Vec<u8>,
        seq_nr: u64,
    ) -> JournalResult<()> {
        if self.fail_snapshot {
            return Err(JournalError::Backend("injected snapshot failure".into()));
        }
        self.inner.save_snapshot(pid, state, seq_nr).await
    }

    async fn latest_snapshot(&self, pid: &PersistenceId) -> JournalResult<Option<(Vec<u8>, u64)>> {
        self.inner.latest_snapshot(pid).await
    }

    async fn delete_events_before(&self, pid: &PersistenceId, seq_nr: u64) -> JournalResult<()> {
        self.inner.delete_events_before(pid, seq_nr).await
    }

    async fn copy_snapshot(&self, from: &PersistenceId, to: &PersistenceId) -> JournalResult<()> {
        self.inner.copy_snapshot(from, to).await
    }

    async fn last_seq(&self, pid: &PersistenceId) -> JournalResult<u64> {
        self.inner.last_seq(pid).await
    }

    async fn clear(&self, pid: &PersistenceId) -> JournalResult<()> {
        self.inner.clear(pid).await
    }
}

/// The `Journal` contract, as executable assertions.
///
/// Lives here rather than in a test file so every backend can be held to it —
/// including `SqlJournal`, which lives in the server crate and so cannot be
/// reached from `actor/tests/`. The assertions come from the trait's own doc
/// comments, which are the real spec: they are behavioural, never about storage
/// layout, which is what makes them portable.
///
/// Each takes a fresh, empty journal.
///
/// Assertions panic on failure, which is the point — this module is test support
/// and only exists under the `test-util` feature, so the workspace's ban on
/// panic-prone constructs does not apply to it.
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::wildcard_enum_match_arm
)]
pub mod conformance {
    use crate::{Journal, PersistenceId};
    use futures_util::StreamExt;

    fn pid(id: &str) -> PersistenceId {
        PersistenceId::new("conformance", id)
    }

    async fn drain(j: &dyn Journal, id: &str, after: u64) -> Vec<Vec<u8>> {
        let mut s = j.replay(&pid(id), after).await;
        let mut out = Vec::new();
        while let Some(item) = s.next().await {
            out.push(item.unwrap().1);
        }
        out
    }

    // ── the contract ─────────────────────────────────────────────────────────────

    pub async fn persist_then_replay_returns_events_in_order(j: &dyn Journal) {
        j.persist(&pid("order"), &[vec![1], vec![2], vec![3]], 0)
            .await
            .unwrap();
        assert_eq!(
            drain(j, "order", 0).await,
            vec![vec![1], vec![2], vec![3]],
            "replay must return events in ascending sequence order"
        );
    }

    pub async fn replay_skips_events_at_or_before_after_seq(j: &dyn Journal) {
        j.persist(&pid("skip"), &[vec![1], vec![2], vec![3]], 0)
            .await
            .unwrap();
        assert_eq!(
            drain(j, "skip", 1).await,
            vec![vec![2], vec![3]],
            "replay(after_seq) must yield strictly-greater sequence numbers only"
        );
    }

    pub async fn logs_are_namespaced_by_kind(j: &dyn Journal) {
        j.persist(&PersistenceId::new("workflow", "shared"), &[vec![1]], 0)
            .await
            .unwrap();
        // Same id under a different kind is a different log, so this write also
        // starts from an empty one — a backend that keyed on the id alone would
        // reject it as a conflict.
        j.persist(&PersistenceId::new("agent", "shared"), &[vec![2]], 0)
            .await
            .unwrap();
        let mut wf = j.replay(&PersistenceId::new("workflow", "shared"), 0).await;
        let mut ag = j.replay(&PersistenceId::new("agent", "shared"), 0).await;
        assert_eq!(wf.next().await.unwrap().unwrap(), (1, vec![1]));
        assert_eq!(ag.next().await.unwrap().unwrap(), (1, vec![2]));
    }

    pub async fn clear_removes_all_state(j: &dyn Journal) {
        j.persist(&pid("cleared"), &[vec![1]], 0).await.unwrap();
        j.clear(&pid("cleared")).await.unwrap();
        assert!(drain(j, "cleared", 0).await.is_empty());
        assert_eq!(
            j.last_seq(&pid("cleared")).await.unwrap(),
            0,
            "a cleared log must look empty to the next writer, numbering included"
        );
    }

    pub async fn persist_continues_numbering_after_compaction(j: &dyn Journal) {
        j.persist(&pid("numbering"), &[vec![1], vec![2]], 0)
            .await
            .unwrap();
        j.delete_events_before(&pid("numbering"), 2).await.unwrap();
        // Compaction removes events, not numbering: the next write still expects
        // 2. A backend that derived the sequence from the surviving rows would
        // reject this, and every write after a snapshot with it.
        j.persist(&pid("numbering"), &[vec![3]], 2).await.unwrap();
        assert_eq!(
            drain(j, "numbering", 2).await,
            vec![vec![3]],
            "an event's sequence number must be stable across compaction"
        );
    }

    pub async fn snapshot_roundtrips_with_seq(j: &dyn Journal) {
        j.persist(&pid("snap"), &[vec![1], vec![2]], 0)
            .await
            .unwrap();
        j.save_snapshot(&pid("snap"), vec![9, 9], 2).await.unwrap();
        assert_eq!(
            j.latest_snapshot(&pid("snap")).await.unwrap(),
            Some((vec![9, 9], 2)),
            "a saved snapshot must be readable back with its sequence number"
        );
    }

    pub async fn delete_events_before_compacts(j: &dyn Journal) {
        j.persist(&pid("compact"), &[vec![1], vec![2], vec![3]], 0)
            .await
            .unwrap();
        j.delete_events_before(&pid("compact"), 2).await.unwrap();
        assert_eq!(
            drain(j, "compact", 0).await,
            vec![vec![3]],
            "compaction must drop events at or before the given sequence"
        );
    }

    pub async fn copy_snapshot_seeds_new_id(j: &dyn Journal) {
        j.persist(&pid("src"), &[vec![1], vec![2]], 0)
            .await
            .unwrap();
        j.save_snapshot(&pid("src"), vec![7], 2).await.unwrap();
        j.copy_snapshot(&pid("src"), &pid("dst")).await.unwrap();
        assert_eq!(
            j.latest_snapshot(&pid("dst")).await.unwrap(),
            Some((vec![7], 2)),
            "copy_snapshot must seed the destination with the source snapshot"
        );
        assert!(
            drain(j, "dst", 2).await.is_empty(),
            "the destination must start with an empty event log"
        );
        assert_eq!(
            j.last_seq(&pid("dst")).await.unwrap(),
            2,
            "the destination inherits the source's numbering, so its first writer expects it"
        );
        j.persist(&pid("dst"), &[vec![8]], 2).await.unwrap();
    }

    pub async fn copy_snapshot_without_source_errors(j: &dyn Journal) {
        assert!(
            j.copy_snapshot(&pid("missing"), &pid("dst2"))
                .await
                .is_err(),
            "copying a snapshot that does not exist must fail, not silently succeed"
        );
    }

    /// Asserts both that recovery starts from the snapshot and that the log was
    /// compacted. Asserting the recovered state alone would not do: a journal
    /// that never compacts recovers the correct *value* by replaying from event
    /// 0, so the state assertion passes while the bug stands.
    pub async fn snapshot_then_compact_leaves_only_later_events(j: &dyn Journal) {
        j.persist(&pid("e2e"), &[vec![1], vec![2]], 0)
            .await
            .unwrap();
        j.save_snapshot(&pid("e2e"), vec![42], 2).await.unwrap();
        j.delete_events_before(&pid("e2e"), 2).await.unwrap();
        j.persist(&pid("e2e"), &[vec![3]], 2).await.unwrap();

        assert_eq!(
            j.latest_snapshot(&pid("e2e")).await.unwrap(),
            Some((vec![42], 2)),
            "recovery must start from the snapshot"
        );
        assert_eq!(
            drain(j, "e2e", 0).await,
            vec![vec![3]],
            "only post-snapshot events should remain in the log"
        );
    }

    pub async fn last_seq_reports_where_the_log_ends(j: &dyn Journal) {
        assert_eq!(
            j.last_seq(&pid("tip")).await.unwrap(),
            0,
            "a log that does not exist yet must report 0, not fail"
        );
        j.persist(&pid("tip"), &[vec![1], vec![2]], 0)
            .await
            .unwrap();
        assert_eq!(j.last_seq(&pid("tip")).await.unwrap(), 2);
    }

    // ── the write fence ──────────────────────────────────────────────────────────
    //
    // Everything below is one guarantee: a writer whose picture of the log is out
    // of date cannot write. It is the only thing between two hosts that both
    // believe they own an instance and a history spliced together out of two
    // divergent ones, and — unlike anything that checks before writing — it holds
    // for a process that was frozen through a failover.
    //
    // A backend that cannot enforce this must fail these tests rather than skip
    // them. A fence that does not fence is worse than none, because everything
    // above it is written believing it holds.

    pub async fn persist_rejects_a_stale_writer(j: &dyn Journal) {
        j.persist(&pid("stale"), &[vec![1]], 0).await.unwrap();
        assert!(
            j.persist(&pid("stale"), &[vec![2]], 0).await.is_err(),
            "a second writer at the same sequence must be rejected, not appended"
        );
        assert_eq!(
            drain(j, "stale", 0).await,
            vec![vec![1]],
            "a rejected write must leave the log exactly as it was"
        );
    }

    pub async fn persist_rejects_a_writer_ahead_of_the_log(j: &dyn Journal) {
        assert!(
            j.persist(&pid("ahead"), &[vec![1]], 5).await.is_err(),
            "a writer claiming events the log does not have must be rejected"
        );
        assert!(drain(j, "ahead", 0).await.is_empty());
    }

    /// The more destructive of the two writes, and the easier one to leave
    /// unguarded: a snapshot is what the next recovery starts from, so a stale
    /// one silently rewrites history rather than adding to it.
    pub async fn save_snapshot_rejects_a_stale_writer(j: &dyn Journal) {
        j.persist(&pid("snapfence"), &[vec![1], vec![2]], 0)
            .await
            .unwrap();
        assert!(
            j.save_snapshot(&pid("snapfence"), vec![9], 1)
                .await
                .is_err(),
            "a snapshot from behind the log's end must be rejected"
        );
        assert!(
            j.latest_snapshot(&pid("snapfence"))
                .await
                .unwrap()
                .is_none(),
            "a rejected snapshot must not be stored"
        );
        assert_eq!(
            drain(j, "snapfence", 0).await,
            vec![vec![1], vec![2]],
            "a rejected snapshot must leave the events a later compaction would have dropped"
        );
    }

    /// Two hosts racing to create the same instance both expect an empty log.
    /// Exactly one may win, or they would each start a history under one id.
    pub async fn only_one_writer_can_start_a_log(j: &dyn Journal) {
        j.persist(&pid("race"), &[vec![1]], 0).await.unwrap();
        assert!(j.persist(&pid("race"), &[vec![2]], 0).await.is_err());
        assert_eq!(drain(j, "race", 0).await, vec![vec![1]]);
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

    fn pid() -> PersistenceId {
        PersistenceId::new("t", "a")
    }

    #[tokio::test]
    async fn fail_persist_after_lets_the_first_n_through() {
        let j = FaultyJournal::wrapping(InMemoryJournal::new()).fail_persist_after(1);
        assert!(j.persist(&pid(), &[vec![1]], 0).await.is_ok());
        assert!(j.persist(&pid(), &[vec![2]], 1).await.is_err());
        assert!(j.persist(&pid(), &[vec![3]], 1).await.is_err());
    }

    #[tokio::test]
    async fn fail_persist_after_zero_fails_immediately() {
        let j = FaultyJournal::wrapping(InMemoryJournal::new()).fail_persist_after(0);
        assert!(j.persist(&pid(), &[vec![1]], 0).await.is_err());
    }

    #[tokio::test]
    async fn healthy_by_default_delegates_to_inner() {
        let j = FaultyJournal::wrapping(InMemoryJournal::new());
        j.persist(&pid(), &[vec![7]], 0).await.unwrap();
        let mut s = j.replay(&pid(), 0).await;
        assert_eq!(s.next().await.unwrap().unwrap(), (1, vec![7]));
    }

    #[tokio::test]
    async fn fail_snapshot_rejects_saves_but_not_persists() {
        let j = FaultyJournal::wrapping(InMemoryJournal::new()).fail_snapshot();
        assert!(j.persist(&pid(), &[vec![1]], 0).await.is_ok());
        assert!(j.save_snapshot(&pid(), vec![9], 1).await.is_err());
    }

    #[tokio::test]
    async fn fail_replay_at_truncates_the_stream_with_an_error() {
        let j = FaultyJournal::wrapping(InMemoryJournal::new()).fail_replay_at(2);
        j.persist(&pid(), &[vec![1], vec![2], vec![3]], 0)
            .await
            .unwrap();
        let mut s = j.replay(&pid(), 0).await;
        assert!(s.next().await.unwrap().is_ok()); // seq 1
        assert!(s.next().await.unwrap().is_err()); // seq 2 → injected failure
    }

    /// The wrapper must pass the writer's expectation through untouched.
    /// Swallowing it — passing 0, say — would make every test that runs through
    /// this journal exercise an unfenced backend.
    #[tokio::test]
    async fn the_wrapper_forwards_the_write_condition() {
        let j = FaultyJournal::wrapping(InMemoryJournal::new());
        j.persist(&pid(), &[vec![1]], 0).await.unwrap();
        assert!(j.persist(&pid(), &[vec![2]], 0).await.is_err());
    }
}
