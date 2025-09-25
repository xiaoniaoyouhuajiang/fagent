use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("SQLite operation failed: {0}")]
    SQLite(#[from] rusqlite::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization/deserialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Graph engine operation failed: {0}")]
    Graph(#[from] helix_db::helix_engine::types::GraphError),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Initialization failed: {0}")]
    Initialization(String),

    #[error("Entity not found: {0}")]
    NotFound(String),

    #[error("Invalid argument: {0}")]
    InvalidArg(String),

    #[error("Synchronization failed: {0}")]
    SyncError(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, StorageError>;
