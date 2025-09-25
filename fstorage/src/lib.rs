pub mod catalog;
pub mod config;
pub mod errors;
pub mod lake;
pub mod models;
pub mod sync;

use crate::catalog::Catalog;
use crate::config::StorageConfig;
use crate::errors::{Result, StorageError};
use crate::lake::Lake;
use crate::sync::{DataSynchronizer, FStorageSynchronizer};
use helix_db::helix_engine::traversal_core::{HelixGraphEngine, HelixGraphEngineOpts};
use std::sync::Arc;

/// The main entry point for the `fstorage` library.
///
/// `FStorage` acts as the primary interface for the data storage layer of the AI agent.
/// It encapsulates all the necessary components for managing a local data lake,
/// including:
/// - A multi-layered data lake (`Lake`) built with Delta Lake for versioned, reliable storage.
/// - A metadata database (`Catalog`) using SQLite to track data readiness, API budgets, and task logs.
/// - A graph database instance (`HelixGraphEngine`) for high-performance querying of indexed data.
/// - A dynamic data synchronization mechanism (`DataSynchronizer`) to keep the local data fresh.
///
/// # Example
///
/// ```rust,no_run
/// use fstorage::{FStorage, config::StorageConfig};
/// use tempfile::tempdir;
///
/// #[tokio::main]
/// async fn main() {
///     let dir = tempdir().unwrap();
///     let config = StorageConfig::new(dir.path());
///     let storage = FStorage::new(config).await.unwrap();
///
///     // Now you can use storage.synchronizer, storage.engine, etc.
/// }
/// ```
pub struct FStorage {
    pub config: StorageConfig,
    pub catalog: Arc<Catalog>,
    pub lake: Arc<Lake>,
    pub engine: Arc<HelixGraphEngine>,
    pub synchronizer: Arc<dyn DataSynchronizer + Send + Sync>,
}

impl FStorage {
    /// Creates a new instance of FStorage and initializes it.
    ///
    /// This will:
    /// 1. Create the necessary directories for the data lake and graph engine.
    /// 2. Open a connection to the SQLite catalog and initialize its schema.
    /// 3. Initialize the HelixGraphEngine.
    pub async fn new(config: StorageConfig) -> Result<Self> {
        // Ensure engine directory exists
        tokio::fs::create_dir_all(&config.engine_path).await?;

        let catalog = Arc::new(Catalog::new(&config)?);
        catalog.initialize_schema()?;

        let lake = Arc::new(Lake::new(config.clone()).await?);

        let engine_opts = HelixGraphEngineOpts {
            path: config.engine_path.to_str().unwrap().to_string(),
            ..Default::default()
        };
        let engine = Arc::new(HelixGraphEngine::new(engine_opts)?);

        let synchronizer = Arc::new(FStorageSynchronizer::new(
            Arc::clone(&catalog),
            Arc::clone(&lake),
            Arc::clone(&engine),
        ));

        Ok(Self {
            config,
            catalog,
            lake,
            engine,
            synchronizer,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_fstorage_initialization() {
        let dir = tempdir().unwrap();
        let config = StorageConfig::new(dir.path());

        let storage = FStorage::new(config.clone()).await;
        assert!(storage.is_ok());

        // Check if files and directories were created
        assert!(config.lake_path.exists());
        assert!(config.lake_path.join("bronze").exists());
        assert!(config.lake_path.join("silver/entities").exists());
        assert!(config.catalog_path.exists());
        assert!(config.engine_path.exists());
    }
}
