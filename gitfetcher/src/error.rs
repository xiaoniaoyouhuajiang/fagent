use thiserror::Error;

#[derive(Error, Debug)]
pub enum FetcherError {
    #[error("GitHub API error: {0}")]
    GitHub(#[from] octocrab::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("CSV serialization error: {0}")]
    Csv(#[from] csv::Error),

    #[error("An unknown error has occurred")]
    Unknown,
}

pub type Result<T> = std::result::Result<T, FetcherError>;
