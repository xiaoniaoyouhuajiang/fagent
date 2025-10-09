use crate::catalog::Catalog;
use crate::errors::{Result, StorageError};
use crate::fetch::{EntityCategory, FetchResponse, Fetcher, GraphData};
use crate::lake::Lake;
use crate::models::{EntityIdentifier, ReadinessReport, SyncContext, SyncBudget};
use async_trait::async_trait;
use bincode;
use deltalake::arrow::record_batch::RecordBatch;
use helix_db::{
    helix_engine::{
        bm25::bm25::{BM25, BM25Flatten},
        storage_core::storage_methods::StorageMethods,
        traversal_core::{
            ops::{
                g::G,
                source::{e_from_id::EFromIdAdapter, n_from_id::NFromIdAdapter},
                util::update::UpdateAdapter,
            },
            HelixGraphEngine,
        },
    },
    protocol::value::Value,
    utils::{
        items::{Edge, Node},
        label_hash::hash_label,
    },
};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

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

use crate::embedding::EmbeddingProvider;

pub struct FStorageSynchronizer {
    catalog: Arc<Catalog>,
    lake: Arc<Lake>,
    engine: Arc<HelixGraphEngine>,
    fetchers: HashMap<&'static str, Arc<dyn Fetcher>>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
}

impl FStorageSynchronizer {
    pub fn new(
        catalog: Arc<Catalog>,
        lake: Arc<Lake>,
        engine: Arc<HelixGraphEngine>,
        embedding_provider: Arc<dyn EmbeddingProvider>,
    ) -> Self {
        Self {
            catalog,
            lake,
            engine,
            fetchers: HashMap::new(),
            embedding_provider,
        }
    }

    /// HOT PATH HELPER: Converts a value from an Arrow Array at a given index to a HelixDB Value.
    fn arrow_value_to_helix_value(
        column: &Arc<dyn deltalake::arrow::array::Array>,
        row_idx: usize,
    ) -> Option<helix_db::protocol::value::Value> {
        use deltalake::arrow::array::*;
        use deltalake::arrow::datatypes::DataType;
        use helix_db::protocol::value::Value;

        if column.is_null(row_idx) {
            return None;
        }

        match column.data_type() {
            DataType::Utf8 => {
                let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
                Some(Value::String(arr.value(row_idx).to_string()))
            }
            DataType::Int64 => {
                let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
                Some(Value::I64(arr.value(row_idx)))
            }
            DataType::Int32 => {
                let arr = column.as_any().downcast_ref::<Int32Array>().unwrap();
                Some(Value::I32(arr.value(row_idx)))
            }
            DataType::Boolean => {
                let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                Some(Value::Boolean(arr.value(row_idx)))
            }
            DataType::Timestamp(_, _) => {
                // Assuming Timestamp is stored as Nanoseconds in Arrow
                let arr = column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>();
                
                if let Some(arr) = arr {
                     let datetime = chrono::DateTime::from_timestamp_nanos(arr.value(row_idx));
                     Some(Value::String(datetime.to_rfc3339())) // Store timestamps as ISO 8601 strings
                } else {
                    None
                }
            }
            _ => None, // Add other type conversions as needed
        }
    }

    /// HOT PATH HELPER: Incrementally updates the graph engine from a collection of entities.
    fn update_engine_from_batch(
        &self,
        fetchable_collection: Box<dyn crate::fetch::AnyFetchable>,
        batch: &RecordBatch,
    ) -> Result<()> {
        let entity_type = fetchable_collection.entity_type_any();
        log::info!(
            "Hot Path: Incrementally updating engine for entity type '{}' with {} records.",
            entity_type,
            batch.num_rows()
        );

        let schema = batch.schema();
        let mut txn = self.engine.storage.graph_env.write_txn()?;

        match fetchable_collection.category_any() {
            EntityCategory::Node => {
                for i in 0..batch.num_rows() {
                    let mut properties = HashMap::new();
                    let mut node_id_str: Option<String> = None;

                    for (field, column) in schema.fields().iter().zip(batch.columns()) {
                        if let Some(value) = Self::arrow_value_to_helix_value(column, i) {
                            if field.name() == "id" {
                                node_id_str = Some(value.to_string());
                            } else {
                                properties.insert(field.name().clone(), value);
                            }
                        }
                    }

                    let id_str = if let Some(s) = node_id_str {
                        s
                    } else {
                        log::warn!(
                            "Skipping node of type '{}' at row {} due to missing 'id'",
                            entity_type,
                            i
                        );
                        continue;
                    };

                    let id_u128 = match Uuid::parse_str(&id_str) {
                        Ok(id) => id.as_u128(),
                        Err(_) => {
                            log::warn!("Failed to parse UUID for node id: {}", id_str);
                            continue;
                        }
                    };

                    if self.engine.storage.get_node(&txn, &id_u128).is_ok() {
                        let props_vec: Vec<(String, Value)> = properties.into_iter().collect();
                        let traversal = G::new(self.engine.storage.clone(), &txn).n_from_id(&id_u128).collect_to::<Vec<_>>();
                        G::new_mut_from(self.engine.storage.clone(), &mut txn, traversal)
                            .update(Some(props_vec))
                            .for_each(|_| {});
                        log::debug!("Updating node: {} ({})", id_str, entity_type);
                    } else {
                        let node = Node {
                            id: id_u128,
                            label: entity_type.to_string(),
                            version: self.engine.storage.version_info.get_latest(entity_type),
                            properties: Some(properties),
                        };

                        let bytes = node.encode_node()?;
                        self.engine.storage.nodes_db.put(&mut txn, &id_u128, &bytes)?;

                        if let Some(props) = &node.properties {
                            for (key, value) in props {
                                if let Some(db) = self.engine.storage.secondary_indices.get(key) {
                                    let value_bytes = bincode::serialize(value).map_err(|e| StorageError::SyncError(e.to_string()))?;
                                    db.put(&mut txn, &value_bytes, &node.id)?;
                                }
                            }
                            if let Some(bm25) = &self.engine.storage.bm25 {
                                let mut data = props.flatten_bm25();
                                data.push_str(&node.label);
                                bm25.insert_doc(&mut txn, node.id, &data)?;
                            }
                        }
                        log::debug!("Inserting node: {} ({})", id_str, entity_type);
                    }
                }
            }
            EntityCategory::Edge => {
                for i in 0..batch.num_rows() {
                    let mut properties = HashMap::new();
                    let mut edge_id_str: Option<String> = None;
                    let mut from_node_id_str: Option<String> = None;
                    let mut to_node_id_str: Option<String> = None;

                    for (field, column) in schema.fields().iter().zip(batch.columns()) {
                        if let Some(value) = Self::arrow_value_to_helix_value(column, i) {
                            match field.name().as_str() {
                                "id" => edge_id_str = Some(value.to_string()),
                                "from_node_id" => from_node_id_str = Some(value.to_string()),
                                "to_node_id" => to_node_id_str = Some(value.to_string()),
                                _ => {
                                    properties.insert(field.name().clone(), value);
                                }
                            }
                        }
                    }

                    let (id_str, from_str, to_str) =
                        if let (Some(id), Some(from), Some(to)) =
                            (edge_id_str, from_node_id_str, to_node_id_str)
                        {
                            (id, from, to)
                        } else {
                            log::warn!("Skipping edge of type '{}' at row {} due to missing id, from_node_id, or to_node_id", entity_type, i);
                            continue;
                        };

                    let id_u128 = match Uuid::parse_str(&id_str) {
                        Ok(id) => id.as_u128(),
                        Err(_) => {
                            log::warn!("Failed to parse UUID for edge id: {}", id_str);
                            continue;
                        }
                    };
                    let from_u128 = match Uuid::parse_str(&from_str) {
                        Ok(id) => id.as_u128(),
                        Err(_) => {
                            log::warn!("Failed to parse UUID for from_node_id: {}", from_str);
                            continue;
                        }
                    };
                    let to_u128 = match Uuid::parse_str(&to_str) {
                        Ok(id) => id.as_u128(),
                        Err(_) => {
                            log::warn!("Failed to parse UUID for to_node_id: {}", to_str);
                            continue;
                        }
                    };

                    if self.engine.storage.get_edge(&txn, &id_u128).is_ok() {
                        let props_vec: Vec<(String, Value)> = properties.into_iter().collect();
                        let traversal = G::new(self.engine.storage.clone(), &txn).e_from_id(&id_u128).collect_to::<Vec<_>>();
                        G::new_mut_from(self.engine.storage.clone(), &mut txn, traversal)
                            .update(Some(props_vec))
                            .for_each(|_| {});
                        log::debug!("Updating edge: {} ({})", id_str, entity_type);
                    } else {
                        let edge = Edge {
                            id: id_u128,
                            label: entity_type.to_string(),
                            version: self.engine.storage.version_info.get_latest(entity_type),
                            properties: Some(properties),
                            from_node: from_u128,
                            to_node: to_u128,
                        };

                        let bytes = edge.encode_edge()?;
                        self.engine.storage.edges_db.put(&mut txn, &id_u128, &bytes)?;

                        let label_hash = hash_label(&edge.label, None);
                        self.engine.storage.out_edges_db.put(
                            &mut txn,
                            &helix_db::helix_engine::storage_core::HelixGraphStorage::out_edge_key(&edge.from_node, &label_hash),
                            &helix_db::helix_engine::storage_core::HelixGraphStorage::pack_edge_data(&edge.id, &edge.to_node),
                        )?;
                        self.engine.storage.in_edges_db.put(
                            &mut txn,
                            &helix_db::helix_engine::storage_core::HelixGraphStorage::in_edge_key(&edge.to_node, &label_hash),
                            &helix_db::helix_engine::storage_core::HelixGraphStorage::pack_edge_data(&edge.id, &edge.from_node),
                        )?;
                        log::debug!("Inserting edge: {} ({})", id_str, entity_type);
                    }
                }
            }
        }

        txn.commit()?;
        Ok(())
    }

    /// COLD & HOT PATH: Processes a unified GraphData object.
    async fn process_graph_data(&self, graph_data: GraphData) -> Result<()> {
        // --- STAGE 2: Persistence - Process all entities (original and newly created) ---
        for fetchable_collection in graph_data.entities {
            let record_batch = fetchable_collection.to_record_batch_any()?;
            let entity_type = fetchable_collection.entity_type_any();
            
            // Cold Path: Write to Data Lake
            let table_name = format!("entities/{}", entity_type.to_lowercase());
            self.lake
                .write_batches(&table_name, vec![record_batch.clone()], None)
                .await?;

            // Hot Path: Write to Graph Engine
            self.update_engine_from_batch(fetchable_collection, &record_batch)?;
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
                coverage_metrics: serde_json::Value::Null,
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

        let fetcher = self.fetchers.get(fetcher_name).ok_or_else(|| {
            StorageError::Config(format!("Fetcher '{}' not registered.", fetcher_name))
        })?;
        
        // The fetcher is now responsible for all transformation, including vectorization.
        let response = fetcher.fetch(params, self.embedding_provider.clone()).await?;

        match response {
            FetchResponse::GraphData(graph_data) => {
                self.process_graph_data(graph_data).await?;
            }
            FetchResponse::PanelData { table_name, batch } => {
                log::info!("Cold Path: Writing panel data to table '{}'", &table_name);
                self.lake.write_batches(&table_name, vec![batch], None).await?;
            }
        }

        let now = chrono::Utc::now().timestamp();
        for entity in &context.target_entities {
            let readiness = crate::models::EntityReadiness {
                entity_uri: entity.uri.clone(),
                entity_type: entity.entity_type.clone(),
                last_synced_at: Some(now),
                ttl_seconds: Some(3600),
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
        log::info!("Starting ETL from Lake to Engine for {}", target_repo_uri);
        self.catalog.update_task_log_status(
            task_id,
            "SUCCESS",
            "ETL completed successfully (simulated).",
        )?;
        Ok(())
    }
}