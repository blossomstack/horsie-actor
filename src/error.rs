use crate::envelope::Epoch;
use thiserror::Error;

/// Errors surfaced by [`Journal`](crate::Journal) operations.
#[derive(Debug, Error)]
pub enum JournalError {
    /// The underlying storage backend failed.
    #[error("journal backend error: {0}")]
    Backend(String),

    /// An event or snapshot could not be (de)serialized.
    #[error("journal serialization error: {0}")]
    Serialization(String),

    /// The write carried an ownership epoch older than the one the log has
    /// already seen, so some other host owns this instance now and this write
    /// would have merged two histories. Rejected rather than applied.
    #[error("write fenced: {pid} is at epoch {current}, write carried {attempted}")]
    Fenced {
        pid: String,
        current: Epoch,
        attempted: Epoch,
    },
}

/// Error returned when delivering a command to an actor's mailbox fails.
#[derive(Debug, Error)]
pub enum TellError {
    /// The target actor has stopped and its mailbox is closed.
    #[error("actor mailbox closed")]
    MailboxClosed,

    /// The actor is hosted elsewhere and the command could not be got to it —
    /// the host is unreachable, or the command would not encode.
    #[error("the command could not be delivered to the host")]
    Undeliverable,

    /// `ask` was used on an actor hosted by another node, and a reply cannot
    /// yet be routed back across a host boundary.
    ///
    /// Refused immediately rather than left to hang. A reply handle does not
    /// survive being carried to another host: it either fails to encode, or —
    /// if the field was skipped to make the command encodable — arrives absent,
    /// the handler answers nobody, and the caller waits forever. An error the
    /// caller can see beats a request that never returns.
    #[error("ask is not yet routable to an actor hosted on another node")]
    AskNotRoutable,
}
