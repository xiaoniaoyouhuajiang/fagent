use crate::catalog::Catalog;
use crate::errors::Result;
use crate::fetch::{Fetchable, Fetcher};
use crate::lake::Lake;
use crate::models::{EntityIdentifier, ReadinessReport, SyncBudget, SyncContext};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

/// Defines the core interface for dynamically synchronizing data.
///
/// This trait abstracts the logic for checking data freshness, performing
/// budgeted data fetching from external sources, and running ETL processes
/// to populate the graph engine.
#[async_trait]
pub trait DataSynchronizer {
    /// Checks the readiness of one or more data entities.
    ///
    /// This method queries the metadata catalog to determine if the local data for
    /// the given entities is considered "fresh" based on their TTLs.
    async fn check_readiness(
        &self,
        entities: &[EntityIdentifier],
    ) -> Result<HashMap<String, ReadinessReport>>;

    /// Performs a budgeted data completion operation for a specific entity type.
    async fn budgeted_complete<T: Fetchable>(
        &self,
        fetcher: Arc<dyn Fetcher<T>>,
        context: SyncContext,
        budget: SyncBudget,
    ) -> Result<HashMap<String, ReadinessReport>>;

    /// Runs a full ETL process from the data lake to the graph engine.
    ///
    /// This method is responsible for reading structured data from the silver layer
    /// and loading it into the `HelixGraphEngine`, effectively rebuilding or updating
    /// the queryable graph index.
    async fn run_full_etl_from_lake(&self, target_repo_uri: &str) -> Result<()>;
}

use helix_db::helix_engine::traversal_core::HelixGraphEngine;

pub struct FStorageSynchronizer {
    catalog: Arc<Catalog>,
    lake: Arc<Lake>,
    engine: Arc<HelixGraphEngine>,
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
        }
    }
}

#[async_trait]
impl DataSynchronizer for FStorageSynchronizer {
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

    async fn budgeted_complete<T: Fetchable>(
        &self,
        fetcher: Arc<dyn Fetcher<T>>,
        context: SyncContext,
        _budget: SyncBudget,
    ) -> Result<HashMap<String, ReadinessReport>> {
        let task_name = format!("budgeted_complete_for_{:?}", context.target_entities);
        let task_id = self.catalog.create_task_log(&task_name)?;

        for entity in &context.target_entities {
            // 1. Fetch data from external source using the provided fetcher
            let fetched_data: Vec<T> = fetcher.fetch(&entity.uri).await?;

            if fetched_data.is_empty() {
                continue;
            }

            // 2. Convert fetched data to a RecordBatch
            let batch = T::to_record_batch(fetched_data.into_iter())?;

            // 3. Write to the silver layer in Delta Lake format, using merge for idempotency
            let table_name = T::table_name();
            let primary_keys = T::primary_keys();
            self.lake
                .write_batches(&table_name, vec![batch], Some(primary_keys))
                .await?;

            // 4. Update catalog
            let now = chrono::Utc::now().timestamp();
            let readiness = crate::models::EntityReadiness {
                entity_uri: entity.uri.clone(),
                entity_type: entity.entity_type.clone(),
                last_synced_at: Some(now),
                ttl_seconds: Some(3600), // Example TTL: 1 hour
                coverage_metrics: "{}".to_string(),
            };
            self.catalog.upsert_readiness(&readiness)?;
        }

        self.catalog.update_task_log_status(
            task_id,
            "SUCCESS",
            "Completed successfully.",
        )?;

        // Return the new readiness status
        self.check_readiness(&context.target_entities).await
    }

    async fn run_full_etl_from_lake(&self, target_repo_uri: &str) -> Result<()> {
        let task_name = format!("full_etl_for_{}", target_repo_uri);
        let task_id = self.catalog.create_task_log(&task_name)?;

        // --- This is where the actual ETL logic would go ---
        // 1. Read from silver tables (e.g., entities/repos, edges/pr_changes_file)
        //    let repos_data = self.read_silver_table("entities/repos").await?;

        // 2. Read from dicts (placeholder)

        // 3. Transform and load into Helix Engine
        //    for row in repos_data.iter() {
        //        let node = ...; // transform row to helix-db node
        //        self.engine.add_node(node)?;
        //    }

        // For now, we just log success.
        // --- End of placeholder logic ---

        self.catalog.update_task_log_status(
            task_id,
            "SUCCESS",
            "ETL completed successfully (simulated).",
        )?;
        Ok(())
    }
}
