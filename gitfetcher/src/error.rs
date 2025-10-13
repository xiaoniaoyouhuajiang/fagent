use thiserror::Error;

#[derive(Debug, Error)]
pub enum GitFetcherError {
    #[error("missing field: {0}")]
    MissingField(&'static str),

    #[error("invalid parameter: {0}")]
    InvalidParam(String),

    #[error("resource not found: {0}")]
    NotFound(String),

    #[error("GitHub API error: {0}")]
    GitHub(#[from] octocrab::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] deltalake::arrow::error::ArrowError),

    #[error("Base64 error: {0}")]
    Base64(#[from] base64::DecodeError),

    #[error("Parser error: {0}")]
    ParseFailure(String),

    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, GitFetcherError>;
