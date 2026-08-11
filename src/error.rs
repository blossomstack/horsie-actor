use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors surfaced by [`Journal`](crate::Journal) operations.
///
/// Encodable, because a command may answer with one and the actor answering may
/// be on another host. An error that could not cross would quietly force every
/// such reply down to a string, losing the one field a caller acts on —
/// [`Conflict`](JournalError::Conflict)'s sequence numbers.
#[derive(Debug, Error, Serialize, Deserialize)]
pub enum JournalError {
    /// The underlying storage backend failed.
    #[error("journal backend error: {0}")]
    Backend(String),

    /// An event or snapshot could not be (de)serialized.
    #[error("journal serialization error: {0}")]
    Serialization(String),

    /// The log did not end where the writer believed it did, so somebody else
    /// has written to it and this writer's state is stale. Rejected rather than
    /// applied, because applying it would splice two divergent histories
    /// together.
    ///
    /// This is the whole write fence. It needs no notion of ownership: a writer
    /// that is behind is detected by being behind, whatever the reason — a
    /// second host, a process that was frozen past a failover, a stale
    /// reactivation. Being *wrong* about who owns an instance is survivable;
    /// writing from a state that no longer exists is not.
    #[error("write conflict: {pid} is at sequence {actual}, writer expected {expected}")]
    Conflict {
        pid: String,
        expected: u64,
        actual: u64,
    },
}

/// Error returned when delivering a command to an actor's mailbox fails.
///
/// Encodable for the same reason as [`JournalError`]: an actor that forwards
/// and reports back is a normal shape, and the report may cross a host.
#[derive(Debug, Error, Serialize, Deserialize)]
pub enum TellError {
    /// The target actor has stopped and its mailbox is closed.
    #[error("actor mailbox closed")]
    MailboxClosed,

    /// The actor is hosted elsewhere and the command could not be got to it —
    /// the host is unreachable, or the command would not encode.
    #[error("the command could not be delivered to the host")]
    Undeliverable,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// The point of encoding these rather than stringifying them at the host
    /// boundary: a caller acts on the fields, not on the message.
    #[test]
    fn a_conflict_keeps_its_sequence_numbers_across_a_host() {
        let wire = serde_json::to_vec(&JournalError::Conflict {
            pid: "session/7".into(),
            expected: 41,
            actual: 42,
        })
        .expect("a journal error encodes");
        let back: JournalError = serde_json::from_slice(&wire).expect("and decodes");
        let JournalError::Conflict {
            pid,
            expected,
            actual,
        } = back
        else {
            panic!("a conflict must decode as a conflict");
        };
        assert_eq!((pid.as_str(), expected, actual), ("session/7", 41, 42));
    }

    #[test]
    fn a_tell_error_round_trips() {
        let wire = serde_json::to_vec(&TellError::Undeliverable).expect("encodes");
        let back: TellError = serde_json::from_slice(&wire).expect("decodes");
        assert!(matches!(back, TellError::Undeliverable));
    }
}
