//! Durable Raft state: the vote, the log, and the applied membership.
//!
//! Durability here is not an optimisation. A node that restarts having forgotten
//! its vote can vote twice in one term, and two leaders in one term is the exact
//! condition membership consensus exists to prevent — so this file is on the
//! safety path even though the state it holds is tiny.
//!
//! Tiny is the other half of the design. The only thing replicated is the member
//! set, so the log grows by one entry per membership change plus one blank entry
//! per election. Rewriting the whole file on each mutation is therefore cheap,
//! and it buys an atomic write with no journalling scheme of its own: serialise,
//! write a sibling, fsync, rename. A crash leaves either the old file or the new
//! one, never a half-written log.

use crate::cluster::types::{LiveSet, Membership, NodeIdx};
use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, LogState, RaftLogStorage, RaftStateMachine};
use openraft::storage::{RaftLogReader, RaftSnapshotBuilder};
use openraft::type_config::alias::{
    EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf,
};
use openraft::{EntryPayload, StoredMembership};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Everything that must survive a restart.
#[derive(Default, Serialize, Deserialize)]
struct Persisted {
    vote: Option<VoteOf<Membership>>,
    /// Log entries by index. A map rather than a vec because purging removes a
    /// prefix and truncation removes a suffix, and neither should renumber.
    entries: BTreeMap<u64, EntryOf<Membership>>,
    purged: Option<LogIdOf<Membership>>,
    committed: Option<LogIdOf<Membership>>,
    applied: Option<LogIdOf<Membership>>,
    membership: StoredMembershipOf<Membership>,
    /// The leader's last agreed view of who is up. Placement reads this.
    live: LiveSet,
    /// The last snapshot, kept whole. It is a few hundred bytes: a log id and a
    /// member set.
    snapshot: Option<(SnapshotMetaOf<Membership>, Vec<u8>)>,
}

/// The Raft log and state machine for one node, sharing one file.
///
/// Cloneable, and every clone is the same store — openraft wants the log
/// storage and the state machine as two objects, and here they are two views of
/// one thing. Keeping them genuinely separate would mean two files that must
/// agree with each other about the applied log id, which is a consistency
/// problem invented for no reason.
#[derive(Clone)]
pub struct RaftStore {
    path: Arc<PathBuf>,
    inner: Arc<Mutex<Persisted>>,
}

impl RaftStore {
    /// Open the store at `path`, reading what is there or starting empty.
    ///
    /// # Errors
    /// If the file exists but cannot be read or decoded. A corrupt store is
    /// reported rather than silently replaced: starting fresh would discard a
    /// vote, which is how a node votes twice in one term.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let inner = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Persisted::default(),
            Err(e) => return Err(e),
        };
        Ok(Self {
            path: Arc::new(path),
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// A store that never touches a disk — for tests, and only for tests.
    ///
    /// Named for what it costs rather than what it is. A node running on this
    /// forgets its vote when it restarts, which is unsafe in exactly the way
    /// this module exists to prevent.
    #[must_use]
    pub fn in_memory_unsafe() -> Self {
        Self {
            path: Arc::new(PathBuf::new()),
            inner: Arc::new(Mutex::new(Persisted::default())),
        }
    }

    /// Write the whole state out atomically.
    fn flush(&self, state: &Persisted) -> io::Result<()> {
        if self.path.as_os_str().is_empty() {
            return Ok(());
        }
        let bytes = serde_json::to_vec(state)?;
        let tmp = self.path.with_extension("tmp");
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&bytes)?;
            // Rename is atomic, but only orders against data that has actually
            // reached the disk. Without this the rename can land while the
            // contents have not.
            file.sync_all()?;
        }
        std::fs::rename(&tmp, self.path.as_path())
    }

    /// The agreed live set, and the configured voters.
    ///
    /// Read on every placement decision, so it is a lock and a clone rather than
    /// anything cleverer — both sets hold a handful of integers.
    #[must_use]
    pub fn live_and_voters(&self) -> (Vec<NodeIdx>, Vec<NodeIdx>) {
        let state = self.inner.lock();
        (
            state.live.nodes.iter().copied().collect(),
            state.membership.membership().voter_ids().collect(),
        )
    }

    /// Run `f` against the state and persist the result before returning.
    fn update<T>(&self, f: impl FnOnce(&mut Persisted) -> T) -> io::Result<T> {
        let mut state = self.inner.lock();
        let out = f(&mut state);
        self.flush(&state)?;
        Ok(out)
    }
}

impl RaftLogReader<Membership> for RaftStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<Membership>>, io::Error> {
        Ok(self
            .inner
            .lock()
            .entries
            .range(range)
            .map(|(_, e)| e.clone())
            .collect())
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<Membership>>, io::Error> {
        Ok(self.inner.lock().vote)
    }
}

impl RaftLogStorage<Membership> for RaftStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<Membership>, io::Error> {
        let state = self.inner.lock();
        let last = state
            .entries
            .last_key_value()
            .map(|(_, e)| e.log_id())
            .or_else(|| state.purged);
        Ok(LogState {
            last_purged_log_id: state.purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<Membership>) -> Result<(), io::Error> {
        self.update(|s| s.vote = Some(*vote))
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<Membership>>,
    ) -> Result<(), io::Error> {
        self.update(|s| s.committed = committed)
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<Membership>>, io::Error> {
        Ok(self.inner.lock().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<Membership>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<Membership>> + Send,
    {
        self.update(|s| {
            for entry in entries {
                s.entries.insert(entry.log_id().index, entry);
            }
        })?;
        // Signalled only after the write is on disk, which is what makes an
        // acknowledged append genuinely durable.
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<Membership>>,
    ) -> Result<(), io::Error> {
        let from = last_log_id.map_or(0, |id| id.index + 1);
        self.update(|s| s.entries.retain(|index, _| *index < from))
    }

    async fn purge(&mut self, log_id: LogIdOf<Membership>) -> Result<(), io::Error> {
        self.update(|s| {
            s.entries.retain(|index, _| *index > log_id.index);
            s.purged = Some(log_id);
        })
    }
}

/// Builds a snapshot of the applied state.
pub struct SnapshotBuilder {
    store: RaftStore,
}

impl RaftSnapshotBuilder<Membership> for SnapshotBuilder {
    type SnapshotData = Vec<u8>;

    async fn build_snapshot(&mut self) -> Result<SnapshotOf<Membership, Vec<u8>>, io::Error> {
        let (applied, membership, live) = {
            let state = self.store.inner.lock();
            (state.applied, state.membership.clone(), state.live.clone())
        };
        let data = serde_json::to_vec(&(&applied, &membership, &live))?;
        let meta = SnapshotMetaOf::<Membership> {
            last_log_id: applied,
            last_membership: membership,
        };
        self.store
            .update(|s| s.snapshot = Some((meta.clone(), data.clone())))?;
        Ok(openraft::storage::Snapshot {
            meta,
            snapshot: data,
        })
    }
}

impl RaftStateMachine<Membership> for RaftStore {
    type SnapshotData = Vec<u8>;
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<Membership>>, StoredMembershipOf<Membership>), io::Error> {
        let state = self.inner.lock();
        Ok((state.applied, state.membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures_util::Stream<
                Item = Result<openraft::storage::EntryResponder<Membership>, io::Error>,
            > + Unpin
            + Send,
    {
        use futures_util::StreamExt;
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            self.update(|s| {
                s.applied = Some(entry.log_id());
                // The only payload this state machine cares about. `Blank` is an
                // election marker and `Normal` cannot occur — there is no
                // application command to replicate, because the member set is
                // the entire state.
                match &entry.payload {
                    EntryPayload::Membership(m) => {
                        s.membership = StoredMembership::new(Some(entry.log_id()), m.clone());
                    }
                    EntryPayload::Normal(live) => s.live = live.clone(),
                    // An election marker. It advances the applied log id and
                    // nothing else, which is exactly what falling through does.
                    EntryPayload::Blank => {}
                }
            })?;
            if let Some(responder) = responder {
                responder.send(());
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            store: self.clone(),
        }
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<Membership>,
        snapshot: Vec<u8>,
    ) -> Result<(), io::Error> {
        let (applied, membership, live): (
            Option<LogIdOf<Membership>>,
            StoredMembershipOf<Membership>,
            LiveSet,
        ) = serde_json::from_slice(&snapshot)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.update(|s| {
            s.applied = applied;
            s.membership = membership;
            s.live = live;
            s.snapshot = Some((meta.clone(), snapshot));
        })
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<Membership, Vec<u8>>>, io::Error> {
        Ok(self
            .inner
            .lock()
            .snapshot
            .clone()
            .map(|(meta, snapshot)| openraft::storage::Snapshot { meta, snapshot }))
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
    use openraft::entry::RaftEntry;
    use openraft::vote::RaftVote;

    fn vote(term: u64, node: crate::cluster::types::NodeIdx) -> VoteOf<Membership> {
        use openraft::vote::RaftLeaderId;
        RaftVote::from_leader_id(
            openraft::impls::leader_id_adv::LeaderId::new(term, node),
            true,
        )
    }

    fn log_id(index: u64) -> LogIdOf<Membership> {
        use openraft::vote::RaftLeaderId;
        openraft::LogId::new(
            openraft::impls::leader_id_adv::LeaderId::new(1, 1).to_committed(),
            index,
        )
    }

    /// The reason this file exists. A node that restarts having forgotten its
    /// vote can vote again in the same term, and two leaders in one term is the
    /// failure membership consensus is here to prevent.
    #[tokio::test]
    async fn a_vote_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.json");

        let mut store = RaftStore::open(&path).unwrap();
        store.save_vote(&vote(7, 1)).await.unwrap();

        let mut reopened = RaftStore::open(&path).unwrap();
        assert!(reopened.read_vote().await.unwrap() == Some(vote(7, 1)));
    }

    /// A corrupt store is reported rather than replaced. Starting fresh would
    /// discard the vote, which is the same failure by a quieter route.
    #[tokio::test]
    async fn a_corrupt_store_is_reported_not_reset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.json");
        std::fs::write(&path, b"not json").unwrap();

        let Err(err) = RaftStore::open(&path) else {
            panic!("a corrupt store was accepted");
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    /// The live set is what placement reads, so it has to come back the same
    /// after a restart or a recovered node would host a different set of
    /// instances from everyone else.
    #[tokio::test]
    async fn the_applied_live_set_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.json");

        {
            let store = RaftStore::open(&path).unwrap();
            store
                .update(|s| {
                    s.live = LiveSet {
                        nodes: [1, 3].into(),
                    };
                })
                .unwrap();
        }

        let reopened = RaftStore::open(&path).unwrap();
        assert_eq!(reopened.live_and_voters().0, vec![1, 3]);
    }

    /// The log survives a restart too, and keeps its numbering.
    #[tokio::test]
    async fn the_log_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.json");

        {
            let mut store = RaftStore::open(&path).unwrap();
            let entries: Vec<EntryOf<Membership>> =
                (1..=3).map(|i| RaftEntry::new_blank(log_id(i))).collect();
            store.append(entries, IOFlushed::noop()).await.unwrap();
        }

        let mut reopened = RaftStore::open(&path).unwrap();
        assert_eq!(reopened.try_get_log_entries(1..10).await.unwrap().len(), 3);
    }

    /// Truncation drops the suffix and purging drops the prefix. Confusing the
    /// two silently discards committed entries, and the symptom shows up much
    /// later as a follower that cannot catch up.
    #[tokio::test]
    async fn truncate_and_purge_cut_opposite_ends() {
        let mut store = RaftStore::in_memory_unsafe();
        let entries: Vec<EntryOf<Membership>> =
            (1..=5).map(|i| RaftEntry::new_blank(log_id(i))).collect();
        store.append(entries, IOFlushed::noop()).await.unwrap();

        store.truncate_after(Some(log_id(3))).await.unwrap();
        let kept = store.try_get_log_entries(1..10).await.unwrap();
        assert_eq!(kept.len(), 3, "truncate_after must drop the suffix");

        store.purge(log_id(1)).await.unwrap();
        let kept = store.try_get_log_entries(1..10).await.unwrap();
        assert_eq!(kept.len(), 2, "purge must drop the prefix");

        // Purging moves the log's floor, not its numbering: the entries left
        // keep the indexes they had.
        assert_eq!(
            store.get_log_state().await.unwrap().last_log_id,
            Some(log_id(3))
        );
    }
}
