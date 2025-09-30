use crate::catalog::Catalog;
use crate::errors::{Result, StorageError};
use crate::fetch::{FetchResponse, Fetcher, GraphData};
use crate::lake::Lake;
use crate::models::{EntityIdentifier, ReadinessReport, SyncContext, SyncBudget};
use async_trait::async_trait;
use helix_db::helix_engine::traversal_core::HelixGraphEngine;
use std::collections::HashMap;
use std::sync::Arc;

/// Defines the core interface for dynamically synchronizing data.
#[async_trait]
pub trait DataSynchronizer {
    /// Registers a concrete fetcher implementation with the synchronizer.
    fn register_fetcher(&mut self, fetcher: Arc<dyn Fetcher>);

    /// Checks the readiness of one or more data entities.
    async fn check_readiness(
        &self,
        entities: &[EntityIdentifier],
    ) -> Result<HashMap<String, ReadinessReport>>;

    /// Performs a data synchronization operation using a named fetcher.
    async fn sync(
        &self,
        fetcher_name: &str,
        params: serde_json::Value,
        context: SyncContext,
        budget: SyncBudget,
    ) -> Result<()>;

    /// Runs a full ETL process from the data lake to the graph engine.
    async fn run_full_etl_from_lake(&self, target_repo_uri: &str) -> Result<()>;
}

pub struct FStorageSynchronizer {
    catalog: Arc<Catalog>,
    lake: Arc<Lake>,
    engine: Arc<HelixGraphEngine>,
    fetchers: HashMap<&'static str, Arc<dyn Fetcher>>,
}

impl FStorageSynchronizer {
    pub fn new(
        catalog: Arc<Catalog>,
        lake: Arc<Lake>,
        engine: Arc<HelixGraphEngine>,
    ) -> Self {
        Self {
            catalog,
            lake,
            engine,
            fetchers: HashMap::new(),
        }
    }

    /// Processes the GraphData variant of a FetchResponse, writing entities to the lake.
    async fn process_graph_data(&self, graph_data: GraphData) -> Result<()> {
        for fetchable_collection in graph_data.into_inner() {
            let entity_type = fetchable_collection.entity_type_any();
            let record_batch = fetchable_collection.to_record_batch_any()?;
            
            // TODO: This table name logic should be more robust, maybe defined in the schema
            let table_name = format!("entities/{}", entity_type.to_lowercase());
            
            // TODO: Primary keys should be retrieved to perform a merge operation
            self.lake
                .write_batches(&table_name, vec![record_batch], None)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl DataSynchronizer for FStorageSynchronizer {
    fn register_fetcher(&mut self, fetcher: Arc<dyn Fetcher>) {
        self.fetchers.insert(fetcher.name(), fetcher);
    }

    async fn check_readiness(
        &self,
        entities: &[EntityIdentifier],
    ) -> Result<HashMap<String, ReadinessReport>> {
        let mut reports = HashMap::new();
        let now = chrono::Utc::now().timestamp();

        for entity in entities {
            let readiness = self.catalog.get_readiness(&entity.uri)?;
            let (is_fresh, gap) = if let Some(r) = readiness {
                if let (Some(last_synced), Some(ttl)) = (r.last_synced_at, r.ttl_seconds) {
                    let gap = now - last_synced;
                    (gap < ttl, Some(gap))
                } else {
                    (false, None)
                }
            } else {
                (false, None)
            };

            let report = ReadinessReport {
                is_fresh,
                freshness_gap_seconds: gap,
                coverage_metrics: serde_json::Value::Null, // Placeholder
            };
            reports.insert(entity.uri.clone(), report);
        }

        Ok(reports)
    }

    async fn sync(
        &self,
        fetcher_name: &str,
        params: serde_json::Value,
        context: SyncContext,
        _budget: SyncBudget,
    ) -> Result<()> {
        let task_name = format!("sync_with_{}", fetcher_name);
        let task_id = self.catalog.create_task_log(&task_name)?;

        // 1. Find and execute the appropriate fetcher
        let fetcher = self.fetchers.get(fetcher_name).ok_or_else(|| {
            StorageError::Config(format!("Fetcher '{}' not registered.", fetcher_name))
        })?;
        let response = fetcher.fetch(params).await?;

        // 2. Process the response based on its type
        match response {
            FetchResponse::GraphData(graph_data) => {
                self.process_graph_data(graph_data).await?;
            }
            FetchResponse::TextForVectorization { node_uri, text, metadata } => {
                // Placeholder: Logic to convert text to vector and store in HelixDB
                log::info!("Received text for vectorization for node: {}", node_uri);
                // self.engine.vector_core.insert(...)
            }
            FetchResponse::PanelData { table_name, batch } => {
                // Placeholder: Logic to write panel data to a dedicated analytics table
                log::info!("Received panel data for table: {}", table_name);
                self.lake.write_batches(&table_name, vec![batch], None).await?;
            }
        }

        // 3. Update catalog for all affected entities
        let now = chrono::Utc::now().timestamp();
        for entity in &context.target_entities {
            let readiness = crate::models::EntityReadiness {
                entity_uri: entity.uri.clone(),
                entity_type: entity.entity_type.clone(),
                last_synced_at: Some(now),
                ttl_seconds: Some(3600), // Example TTL: 1 hour
                coverage_metrics: "{}".to_string(),
            };
            self.catalog.upsert_readiness(&readiness)?;
        }

        self.catalog
            .update_task_log_status(task_id, "SUCCESS", "Sync completed successfully.")?;

        Ok(())
    }

    async fn run_full_etl_from_lake(&self, target_repo_uri: &str) -> Result<()> {
        let task_name = format!("full_etl_for_{}", target_repo_uri);
        let task_id = self.catalog.create_task_log(&task_name)?;

        // --- This is where the actual ETL logic would go ---
        log::info!("Starting ETL from Lake to Engine for {}", target_repo_uri);
        // --- End of placeholder logic ---

        self.catalog.update_task_log_status(
            task_id,
            "SUCCESS",
            "ETL completed successfully (simulated).",
        )?;
        Ok(())
    }
}
