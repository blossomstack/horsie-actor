use crate::envelope::Envelope;
use std::collections::{HashSet, VecDeque};

/// Remembers which envelopes have already been applied.
///
/// The sender cannot tell a lost message from a slow one, so it must be free to
/// retry — which means the receiver is what turns at-least-once delivery into
/// effectively-once *processing*. Without this, a retry after a timeout applies
/// the same command twice.
///
/// Bounded on purpose. This set is carried in the receiving actor's own state
/// and replayed on recovery, so an unbounded one would grow forever and make
/// every snapshot bigger than the last. The bound is a window: a redelivery
/// older than it is one the sender gave up on long ago.
#[derive(Debug, Clone)]
pub struct Dedup {
    seen: HashSet<u128>,
    /// Insertion order, so the oldest id is the one evicted.
    order: VecDeque<u128>,
    capacity: usize,
}

impl Dedup {
    /// A window of `capacity` message ids.
    ///
    /// # Panics
    /// Never — a zero capacity is raised to one, because a window of zero would
    /// accept every redelivery and quietly defeat the whole mechanism.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            seen: HashSet::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Whether this envelope should be applied, recording it if so.
    ///
    /// `true` means new, `false` means already applied — drop it.
    pub fn accept(&mut self, env: &Envelope) -> bool {
        self.accept_id(env.message_id)
    }

    /// [`accept`](Self::accept) by id, for callers that have no envelope.
    pub fn accept_id(&mut self, message_id: u128) -> bool {
        if !self.seen.insert(message_id) {
            return false;
        }
        self.order.push_back(message_id);
        if self.order.len() > self.capacity
            && let Some(evicted) = self.order.pop_front()
        {
            self.seen.remove(&evicted);
        }
        true
    }

    /// How many ids the window currently holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.order.len()
    }

    /// Whether nothing has been seen yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn env(message_id: u128) -> Envelope {
        Envelope {
            type_name: "counter".into(),
            message_id,
            payload: Vec::new(),
        }
    }

    /// The core guarantee: a redelivered envelope is applied once.
    #[tokio::test]
    async fn a_redelivered_envelope_is_applied_once() {
        let mut dedup = Dedup::with_capacity(128);
        let e = env(1);
        assert!(dedup.accept(&e), "first delivery is new");
        assert!(!dedup.accept(&e), "redelivery is a duplicate");
    }

    /// Distinct messages are all applied — a dedup that rejected too much would
    /// look identical to message loss.
    #[tokio::test]
    async fn distinct_messages_are_all_accepted() {
        let mut dedup = Dedup::with_capacity(128);
        for id in 1..=10 {
            assert!(dedup.accept(&env(id)), "message {id} should be new");
        }
        assert_eq!(dedup.len(), 10);
    }

    /// The window evicts oldest-first and stays at its bound, so the set this
    /// carries into an actor's snapshot cannot grow without limit.
    #[tokio::test]
    async fn the_window_is_bounded_and_evicts_the_oldest() {
        let mut dedup = Dedup::with_capacity(3);
        for id in 1..=3 {
            assert!(dedup.accept(&env(id)));
        }
        // Still inside the window.
        assert!(!dedup.accept(&env(1)));

        // Pushes the window past 1.
        assert!(dedup.accept(&env(4)));
        assert_eq!(dedup.len(), 3);

        // 1 has aged out, so it reads as new again. That is the deliberate
        // trade: the window bounds memory, and a redelivery this old is one the
        // sender abandoned long ago.
        assert!(dedup.accept(&env(1)));
        // 2 was evicted by that insert; 3 and 4 are still remembered.
        assert!(!dedup.accept(&env(3)));
        assert!(!dedup.accept(&env(4)));
    }

    /// A zero capacity would accept every redelivery and silently defeat the
    /// mechanism, so it is raised to one rather than honoured.
    #[tokio::test]
    async fn a_zero_capacity_still_dedups() {
        let mut dedup = Dedup::with_capacity(0);
        assert!(dedup.accept(&env(1)));
        assert!(!dedup.accept(&env(1)));
    }
}
