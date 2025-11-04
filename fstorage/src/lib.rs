pub mod auto_fetchable;
pub mod catalog;
pub mod config;
pub mod embedding;
pub mod errors;
pub mod fetch;
pub mod lake;
pub mod models;
pub mod schema_registry;
pub mod schemas;
pub mod sync;
pub mod utils;

use crate::catalog::Catalog;
use crate::config::StorageConfig;
use crate::embedding::{
    EmbeddingProvider, FastEmbedProvider, NullEmbeddingProvider, OpenAIProvider,
};
use crate::errors::Result;
use crate::fetch::{Fetcher, FetcherCapability};
use crate::lake::Lake;
use crate::models::{
    EntityIdentifier, EntityMetadata, HybridSearchHit, MultiEntitySearchHit, PathResult,
    ReadinessReport, TableSummary, TextSearchHit, VectorSearchHit,
};
use crate::sync::{DataSynchronizer, FStorageSynchronizer};
use helix_db::helix_engine::traversal_core::{HelixGraphEngine, HelixGraphEngineOpts};
use std::collections::HashMap;
use std::sync::Arc;

/// The main entry point for the `fstorage` library.
///
/// `FStorage` acts as the primary interface for the data storage layer of the AI agent.
pub struct FStorage {
    pub config: StorageConfig,
    pub catalog: Arc<Catalog>,
    pub lake: Arc<Lake>,
    pub engine: Arc<HelixGraphEngine>,
    pub synchronizer: Arc<FStorageSynchronizer>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
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
            Err(_) => match FastEmbedProvider::new_default() {
                Ok(provider) => {
                    log::info!(
                        "FASTEMBED backend initialized (no OPENAI_API_KEY present); vectors will be generated locally."
                    );
                    Arc::new(provider)
                }
                Err(err) => {
                    log::warn!(
                        "FASTEMBED initialization failed ({}); falling back to NullEmbeddingProvider. Vector embeddings will be empty.",
                        err
                    );
                    Arc::new(NullEmbeddingProvider)
                }
            },
        };

        let synchronizer = Arc::new(FStorageSynchronizer::new(
            Arc::clone(&catalog),
            Arc::clone(&lake),
            Arc::clone(&engine),
            embedding_provider.clone(),
        ));

        Ok(Self {
            config,
            catalog,
            lake,
            engine,
            synchronizer,
            embedding_provider,
        })
    }

    /// Registers a fetcher with the synchronizer.
    ///
    /// This method allows the application's entry point (e.g., `fagent`) to
    /// inject concrete fetcher implementations into the storage layer.
    pub fn register_fetcher(&self, fetcher: Arc<dyn Fetcher>) {
        self.synchronizer.register_fetcher(fetcher);
    }

    /// Lists the capabilities for all registered fetchers.
    pub fn list_fetchers_capability(&self) -> Vec<FetcherCapability> {
        self.synchronizer.list_fetcher_capabilities()
    }

    /// Lists known entities/edges along with their ingestion metadata tracked in the catalog.
    pub fn list_known_entities(&self) -> Result<Vec<EntityMetadata>> {
        let offsets = self.catalog.list_ingestion_offsets()?;
        let mut entities: Vec<_> = offsets
            .into_iter()
            .map(|offset| EntityMetadata {
                table_path: offset.table_path,
                entity_type: offset.entity_type,
                category: offset.category.as_str().to_string(),
                primary_keys: offset.primary_keys,
                last_version: offset.last_version,
            })
            .collect();
        entities.sort_by(|a, b| a.table_path.cmp(&b.table_path));
        Ok(entities)
    }

    /// Lists Delta tables under a given prefix, returning their schema summaries.
    pub async fn list_tables(&self, prefix: &str) -> Result<Vec<TableSummary>> {
        self.lake.list_tables(prefix).await
    }

    /// Returns readiness reports for a collection of entities.
    pub async fn get_readiness(
        &self,
        entities: &[EntityIdentifier],
    ) -> Result<HashMap<String, ReadinessReport>> {
        self.synchronizer.check_readiness(entities).await
    }

    pub async fn search_text_bm25(
        &self,
        entity_type: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<TextSearchHit>> {
        self.lake.search_bm25(entity_type, query, limit).await
    }

    pub async fn search_vectors(
        &self,
        entity_type: &str,
        query_vector: &[f64],
        limit: usize,
    ) -> Result<Vec<VectorSearchHit>> {
        self.lake
            .search_vectors(entity_type, query_vector, limit)
            .await
    }

    pub async fn search_vectors_by_text(
        &self,
        entity_type: &str,
        query_text: &str,
        limit: usize,
    ) -> Result<Vec<VectorSearchHit>> {
        let trimmed = query_text.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }
        let embedding = self
            .embedding_provider
            .embed(vec![trimmed.to_string()])
            .await?;
        let vector = embedding.into_iter().next().unwrap_or_default();
        self.search_vectors(entity_type, &vector, limit).await
    }

    pub async fn search_hybrid(
        &self,
        entity_type: &str,
        query_text: &str,
        alpha: f32,
        limit: usize,
    ) -> Result<Vec<HybridSearchHit>> {
        let trimmed = query_text.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }
        let embedding = self
            .embedding_provider
            .embed(vec![trimmed.to_string()])
            .await?;
        let vector = embedding.into_iter().next().unwrap_or_default();
        self.lake
            .search_hybrid(entity_type, trimmed, &vector, alpha, limit)
            .await
    }

    pub async fn search_hybrid_multi(
        &self,
        entity_types: &[String],
        query_text: &str,
        alpha: f32,
        limit: usize,
    ) -> Result<Vec<MultiEntitySearchHit>> {
        let trimmed = query_text.trim();
        if entity_types.is_empty() || trimmed.is_empty() {
            return Ok(Vec::new());
        }
        let embedding = self
            .embedding_provider
            .embed(vec![trimmed.to_string()])
            .await?;
        let vector = embedding.into_iter().next().unwrap_or_default();
        self.lake
            .search_hybrid_multi(entity_types, trimmed, &vector, alpha, limit)
            .await
    }

    pub async fn embed_texts(&self, texts: Vec<String>) -> Result<Vec<Vec<f64>>> {
        self.embedding_provider.embed(texts).await
    }

    pub fn embedding_provider(&self) -> Arc<dyn EmbeddingProvider> {
        Arc::clone(&self.embedding_provider)
    }

    pub async fn shortest_path(
        &self,
        from_id: &str,
        to_id: &str,
        edge_label: Option<&str>,
    ) -> Result<Option<PathResult>> {
        self.lake.shortest_path(from_id, to_id, edge_label).await
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
