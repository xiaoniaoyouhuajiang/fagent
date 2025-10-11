pub mod auto_fetchable;
pub mod catalog;
pub mod config;
pub mod embedding;
pub mod errors;
pub mod fetch;
pub mod lake;
pub mod models;
pub mod schemas;
pub mod sync;
pub mod utils;

use crate::catalog::Catalog;
use crate::config::StorageConfig;
use crate::embedding::{EmbeddingProvider, NullEmbeddingProvider, OpenAIProvider};
use crate::errors::Result;
use crate::fetch::Fetcher;
use crate::lake::Lake;
use crate::sync::{DataSynchronizer, FStorageSynchronizer};
use helix_db::helix_engine::traversal_core::{HelixGraphEngine, HelixGraphEngineOpts};
use std::sync::{Arc, Mutex};

/// The main entry point for the `fstorage` library.
///
/// `FStorage` acts as the primary interface for the data storage layer of the AI agent.
pub struct FStorage {
    pub config: StorageConfig,
    pub catalog: Arc<Catalog>,
    pub lake: Arc<Lake>,
    pub engine: Arc<HelixGraphEngine>,
    pub synchronizer: Arc<Mutex<dyn DataSynchronizer + Send + Sync>>,
}

impl FStorage {
    /// Creates a new instance of FStorage and initializes it.
    pub async fn new(config: StorageConfig) -> Result<Self> {
        // Load environment variables
        dotenvy::dotenv().ok();

        // Ensure engine directory exists
        tokio::fs::create_dir_all(&config.engine_path).await?;

        let catalog = Arc::new(Catalog::new(&config)?);
        catalog.initialize_schema()?;

        let engine_path = config
            .engine_path
            .to_str()
            .ok_or_else(|| crate::errors::StorageError::Config("Non-UTF8 engine path".into()))?
            .to_string();
        let engine_opts = HelixGraphEngineOpts {
            path: engine_path,
            ..Default::default()
        };
        let engine = Arc::new(HelixGraphEngine::new(engine_opts)?);

        let lake = Arc::new(Lake::new(config.clone(), Arc::clone(&engine)).await?);

        // Initialize the embedding provider
        let embedding_model = engine
            .storage
            .storage_config
            .embedding_model
            .clone()
            .unwrap_or_else(|| "text-embedding-ada-002".to_string());
        let embedding_provider: Arc<dyn EmbeddingProvider> = match std::env::var("OPENAI_API_KEY") {
            Ok(key) => Arc::new(OpenAIProvider::new(embedding_model, key)),
            Err(_) => {
                log::warn!(
                    "OPENAI_API_KEY not found, using NullEmbeddingProvider. Vector embeddings will be empty."
                );
                Arc::new(NullEmbeddingProvider)
            }
        };

        let synchronizer = Arc::new(Mutex::new(FStorageSynchronizer::new(
            Arc::clone(&catalog),
            Arc::clone(&lake),
            Arc::clone(&engine),
            embedding_provider.clone(),
        )));

        Ok(Self {
            config,
            catalog,
            lake,
            engine,
            synchronizer,
        })
    }

    /// Registers a fetcher with the synchronizer.
    ///
    /// This method allows the application's entry point (e.g., `fagent`) to
    /// inject concrete fetcher implementations into the storage layer.
    pub fn register_fetcher(&self, fetcher: Arc<dyn Fetcher>) {
        self.synchronizer.lock().unwrap().register_fetcher(fetcher);
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
        assert!(config.catalog_path.exists());
        assert!(config.engine_path.exists());
    }
}
