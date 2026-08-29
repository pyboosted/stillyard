use thiserror::Error;

/// Errors exposed by the public Stillyard client contract.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("invalid specification: {0}")]
    InvalidSpec(String),

    #[error("serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("local protocol error: {0}")]
    Protocol(String),

    #[error("managed wait rejected ({code}): {detail}")]
    ManagedWaitRejected { code: String, detail: String },

    #[error("operation rejected ({code}): {detail}")]
    Rejected { code: String, detail: String },

    #[error("view cursor is stale: {detail}")]
    ViewStale { detail: String },

    #[error("view unavailable: {detail}")]
    ViewUnavailable { detail: String },

    #[error("{detail}")]
    NotFound { detail: String },

    #[error("I/O failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("the local daemon is unavailable: {0}")]
    Unavailable(String),

    #[error("the operation deadline elapsed")]
    DeadlineElapsed,

    #[error("the operation was canceled")]
    Canceled,

    #[error("this operation is unsupported on {0}")]
    UnsupportedPlatform(&'static str),
}

/// Public result alias.
pub type Result<T> = std::result::Result<T, Error>;
