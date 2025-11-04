use crate::auto_fetchable;
use crate::catalog::Catalog;
use crate::errors::{Result, StorageError};
use crate::fetch::{
    EntityCategory, FetchResponse, Fetcher, FetcherCapability, GraphData, ProbeReport,
};
use crate::lake::Lake;
use crate::models::{EntityIdentifier, ReadinessReport, SyncBudget, SyncContext};
use crate::schema_registry::{
    vector_index, vector_rules, SourceNodeId, SourceNodeType, SCHEMA_REGISTRY,
};
use crate::utils;
use async_trait::async_trait;
use bincode;
use chrono::{DateTime, Utc};
use deltalake::arrow::array::{
    Array, Float32Array, ListArray, StringArray, TimestampMicrosecondArray,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use heed3::{RoTxn, RwTxn};
use helix_db::{
    helix_engine::{
        bm25::bm25::{BM25Flatten, BM25},
        storage_core::storage_methods::StorageMethods,
        traversal_core::{
            ops::{
                g::G,
                source::{e_from_id::EFromIdAdapter, n_from_id::NFromIdAdapter},
                util::update::UpdateAdapter,
                vectors::insert::InsertVAdapter,
            },
            traversal_value::Traversable,
            HelixGraphEngine,
        },
        vector_core::vector::HVector,
    },
    protocol::value::Value,
    utils::{
        items::{Edge, Node},
        label_hash::hash_label,
    },
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use uuid::Uuid;

/// Defines the core interface for dynamically synchronizing data.
#[async_trait]
pub trait DataSynchronizer {
    /// Registers a concrete fetcher implementation with the synchronizer.
    fn register_fetcher(&self, fetcher: Arc<dyn Fetcher>);

    /// Lists the capabilities of all registered fetchers.
    fn list_fetcher_capabilities(&self) -> Vec<FetcherCapability>;

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

    /// COLD & HOT PATH: Processes a unified GraphData object.
    async fn process_graph_data(&self, graph_data: GraphData) -> Result<()>;
}

use crate::embedding::EmbeddingProvider;

pub struct FStorageSynchronizer {
    catalog: Arc<Catalog>,
    lake: Arc<Lake>,
    engine: Arc<HelixGraphEngine>,
    fetchers: RwLock<HashMap<String, Arc<dyn Fetcher>>>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
}

#[derive(Debug, Clone)]
struct EdgeWrite {
    id: Option<String>,
    from_node_id: Option<String>,
    to_node_id: Option<String>,
    from_node_type: Option<String>,
    to_node_type: Option<String>,
    created_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
struct VectorIndexWrite {
    id_value: String,
    vector_uuid: String,
    updated_at: Option<DateTime<Utc>>,
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
            fetchers: RwLock::new(HashMap::new()),
            embedding_provider,
        }
    }

    fn string_from_columns(
        columns: &[Arc<dyn deltalake::arrow::array::Array>],
        column_index: &HashMap<String, usize>,
        column: &str,
        row: usize,
    ) -> Option<String> {
        column_index.get(column).and_then(|idx| {
            columns[*idx]
                .as_any()
                .downcast_ref::<StringArray>()
                .and_then(|arr| {
                    if arr.is_null(row) {
                        None
                    } else {
                        Some(arr.value(row).to_string())
                    }
                })
        })
    }

    fn resolve_vector_rule(
        rule: &crate::schema_registry::VectorEdgeRule,
        columns: &[Arc<dyn deltalake::arrow::array::Array>],
        column_index: &HashMap<String, usize>,
        row: usize,
        row_created_at: Option<DateTime<Utc>>,
        vector_uuid: &str,
        vector_identity: &str,
    ) -> Result<Option<EdgeWrite>> {
        let (from_node_id, from_node_type) = match &rule.source {
            SourceNodeId::PrimaryKey {
                entity_type: src_entity,
                mappings,
            } => {
                let mut key_pairs: Vec<(&'static str, String)> = Vec::new();
                for mapping in mappings {
                    let Some(value) = Self::string_from_columns(
                        columns,
                        column_index,
                        mapping.vector_column,
                        row,
                    ) else {
                        log::warn!(
                            "Vector edge '{}' skipped: column '{}' missing on row {}",
                            rule.edge_type,
                            mapping.vector_column,
                            row
                        );
                        return Ok(None);
                    };
                    key_pairs.push((mapping.primary_key, value));
                }
                if key_pairs.is_empty() {
                    return Ok(None);
                }
                let node_id =
                    Uuid::from_u128(utils::id::stable_node_id_u128(src_entity, &key_pairs))
                        .to_string();
                let from_type = match &rule.source_node_type {
                    SourceNodeType::Literal(value) => value.to_string(),
                    SourceNodeType::FromKeyPattern(column) => {
                        let Some(key_value) =
                            Self::string_from_columns(columns, column_index, column, row)
                        else {
                            log::warn!(
                                "Missing key column '{}' while deriving node type for edge '{}'",
                                column,
                                rule.edge_type
                            );
                            return Ok(None);
                        };
                        if let Some(prefix) = extract_node_type_from_key(&key_value) {
                            prefix.to_string()
                        } else {
                            log::warn!(
                                "Unable to parse node type from key '{}' for edge '{}'",
                                key_value,
                                rule.edge_type
                            );
                            return Ok(None);
                        }
                    }
                };
                (node_id, from_type)
            }
            SourceNodeId::DirectColumn { column } => {
                let Some(node_id) = Self::string_from_columns(columns, column_index, column, row)
                else {
                    log::warn!(
                        "Vector edge '{}' skipped: source id column '{}' missing on row {}",
                        rule.edge_type,
                        column,
                        row
                    );
                    return Ok(None);
                };
                let from_type = match &rule.source_node_type {
                    SourceNodeType::Literal(value) => value.to_string(),
                    SourceNodeType::FromKeyPattern(column) => {
                        let Some(key_value) =
                            Self::string_from_columns(columns, column_index, column, row)
                        else {
                            log::warn!(
                                "Missing key column '{}' while deriving node type for edge '{}'",
                                column,
                                rule.edge_type
                            );
                            return Ok(None);
                        };
                        if let Some(prefix) = extract_node_type_from_key(&key_value) {
                            prefix.to_string()
                        } else {
                            log::warn!(
                                "Unable to parse node type from key '{}' for edge '{}'",
                                key_value,
                                rule.edge_type
                            );
                            return Ok(None);
                        }
                    }
                };
                (node_id, from_type)
            }
        };

        let edge_id =
            utils::id::stable_edge_id_u128(rule.edge_type, &from_node_id, vector_identity);
        Ok(Some(EdgeWrite {
            id: Some(Uuid::from_u128(edge_id).to_string()),
            from_node_id: Some(from_node_id),
            to_node_id: Some(vector_uuid.to_string()),
            from_node_type: Some(from_node_type),
            to_node_type: Some(rule.target_node_type.to_string()),
            created_at: row_created_at,
            updated_at: row_created_at,
        }))
    }

    fn build_edge_record_batch(edges: &[EdgeWrite]) -> Result<RecordBatch> {
        let ids: Vec<Option<String>> = edges.iter().map(|edge| edge.id.clone()).collect();
        let from_ids: Vec<Option<String>> =
            edges.iter().map(|edge| edge.from_node_id.clone()).collect();
        let to_ids: Vec<Option<String>> =
            edges.iter().map(|edge| edge.to_node_id.clone()).collect();
        let from_types: Vec<Option<String>> = edges
            .iter()
            .map(|edge| edge.from_node_type.clone())
            .collect();
        let to_types: Vec<Option<String>> =
            edges.iter().map(|edge| edge.to_node_type.clone()).collect();
        let created_at: Vec<Option<DateTime<Utc>>> =
            edges.iter().map(|edge| edge.created_at).collect();
        let updated_at: Vec<Option<DateTime<Utc>>> =
            edges.iter().map(|edge| edge.updated_at).collect();

        let fields = vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("from_node_id", DataType::Utf8, true),
            Field::new("to_node_id", DataType::Utf8, true),
            Field::new("from_node_type", DataType::Utf8, true),
            Field::new("to_node_type", DataType::Utf8, true),
            Field::new(
                "created_at",
                DataType::Timestamp(
                    deltalake::arrow::datatypes::TimeUnit::Microsecond,
                    Some("UTC".into()),
                ),
                true,
            ),
            Field::new(
                "updated_at",
                DataType::Timestamp(
                    deltalake::arrow::datatypes::TimeUnit::Microsecond,
                    Some("UTC".into()),
                ),
                true,
            ),
        ];

        let arrays = vec![
            auto_fetchable::to_arrow_array(ids)?,
            auto_fetchable::to_arrow_array(from_ids)?,
            auto_fetchable::to_arrow_array(to_ids)?,
            auto_fetchable::to_arrow_array(from_types)?,
            auto_fetchable::to_arrow_array(to_types)?,
            auto_fetchable::to_arrow_array(created_at)?,
            auto_fetchable::to_arrow_array(updated_at)?,
        ];

        let schema = Schema::new(fields);
        Ok(RecordBatch::try_new(Arc::new(schema), arrays)?)
    }

    fn build_vector_index_batch(
        id_column: &str,
        updates: &[VectorIndexWrite],
    ) -> Result<RecordBatch> {
        let id_values: Vec<Option<String>> = updates
            .iter()
            .map(|update| Some(update.id_value.clone()))
            .collect();
        let vector_ids: Vec<Option<String>> = updates
            .iter()
            .map(|update| Some(update.vector_uuid.clone()))
            .collect();
        let updated_at: Vec<Option<DateTime<Utc>>> =
            updates.iter().map(|update| update.updated_at).collect();

        let fields = vec![
            Field::new(id_column, DataType::Utf8, false),
            Field::new("vector_uuid", DataType::Utf8, false),
            Field::new(
                "updated_at",
                DataType::Timestamp(
                    deltalake::arrow::datatypes::TimeUnit::Microsecond,
                    Some("UTC".into()),
                ),
                true,
            ),
        ];

        let arrays = vec![
            auto_fetchable::to_arrow_array(id_values)?,
            auto_fetchable::to_arrow_array(vector_ids)?,
            auto_fetchable::to_arrow_array(updated_at)?,
        ];

        let schema = Schema::new(fields);
        Ok(RecordBatch::try_new(Arc::new(schema), arrays)?)
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
            DataType::Timestamp(unit, _) => match unit {
                deltalake::arrow::datatypes::TimeUnit::Microsecond => {
                    let arr = column
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    let micros = arr.value(row_idx);
                    let dt = chrono::DateTime::from_timestamp_micros(micros).unwrap();
                    Some(Value::String(dt.to_rfc3339()))
                }
                _ => {
                    // Handle other time units if necessary, for now just log and ignore
                    log::warn!("Unsupported timestamp unit: {:?}", unit);
                    None
                }
            },
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
        let category = fetchable_collection.category_any();
        let primary_keys: Vec<String> = fetchable_collection
            .primary_keys_any()
            .into_iter()
            .map(|k| k.to_string())
            .collect();
        self.update_engine_from_batch_with_meta(entity_type, category, &primary_keys, batch)
    }

    fn update_engine_from_batch_with_meta(
        &self,
        entity_type: &str,
        category: crate::fetch::EntityCategory,
        primary_keys: &[String],
        batch: &RecordBatch,
    ) -> Result<()> {
        log::info!(
            "Hot Path: Incrementally updating engine for entity type '{}' with {} records.",
            entity_type,
            batch.num_rows()
        );

        let schema = batch.schema();
        let mut txn = self.engine.storage.graph_env.write_txn()?;

        match category {
            EntityCategory::Node => {
                for i in 0..batch.num_rows() {
                    let mut properties = HashMap::new();
                    let mut node_id_str: Option<String> = None;

                    for (field, column) in schema.fields().iter().zip(batch.columns()) {
                        if let Some(value) = Self::arrow_value_to_helix_value(column, i) {
                            match field.name().as_str() {
                                "id" => {
                                    node_id_str = Some(value.inner_stringify());
                                }
                                _ => {
                                    properties.insert(field.name().clone(), value);
                                }
                            }
                        }
                    }

                    let id_u128 = if let Some(id_str) = node_id_str {
                        match Uuid::parse_str(&id_str) {
                            Ok(id) => id.as_u128(),
                            Err(_) => {
                                log::warn!("Failed to parse UUID for node id: {}", id_str);
                                continue;
                            }
                        }
                    } else {
                        if primary_keys.is_empty() {
                            log::warn!(
                                "Skipping node of type '{}' at row {} due to missing 'id' and no primary keys defined.",
                                entity_type,
                                i
                            );
                            continue;
                        }
                        let key_values: Vec<_> = primary_keys
                            .iter()
                            .filter_map(|key| {
                                schema.index_of(key).ok().map(|idx| {
                                    let col = batch.column(idx);
                                    let val = Self::arrow_value_to_helix_value(col, i)
                                        .map(|v| v.inner_stringify())
                                        .unwrap_or_default();
                                    (key.as_str(), val)
                                })
                            })
                            .collect();

                        utils::id::stable_node_id_u128(entity_type, &key_values)
                    };

                    if self.engine.storage.get_node(&txn, &id_u128).is_ok() {
                        let props_vec: Vec<(String, Value)> = properties.into_iter().collect();
                        let traversal = G::new(self.engine.storage.clone(), &txn)
                            .n_from_id(&id_u128)
                            .collect_to::<Vec<_>>();
                        G::new_mut_from(self.engine.storage.clone(), &mut txn, traversal)
                            .update(Some(props_vec))
                            .for_each(|_| {});
                        log::debug!(
                            "Updating node: {} ({})",
                            Uuid::from_u128(id_u128).to_string(),
                            entity_type
                        );
                    } else {
                        let node = Node {
                            id: id_u128,
                            label: entity_type.to_string(),
                            version: self.engine.storage.version_info.get_latest(entity_type),
                            properties: Some(properties),
                        };

                        let bytes = node.encode_node()?;
                        self.engine
                            .storage
                            .nodes_db
                            .put(&mut txn, &id_u128, &bytes)?;

                        if let Some(props) = &node.properties {
                            for (key, value) in props {
                                if let Some(db) = self.engine.storage.secondary_indices.get(key) {
                                    let value_bytes = bincode::serialize(value)
                                        .map_err(|e| StorageError::SyncError(e.to_string()))?;
                                    db.put(&mut txn, &value_bytes, &node.id)?;
                                }
                            }
                            if let Some(bm25) = &self.engine.storage.bm25 {
                                let mut data = props.flatten_bm25();
                                data.push_str(&node.label);
                                bm25.insert_doc(&mut txn, node.id, &data)?;
                            }
                        }
                        log::debug!(
                            "Inserting node: {} ({})",
                            Uuid::from_u128(id_u128).to_string(),
                            entity_type
                        );
                    }
                }
            }
            EntityCategory::Vector => {
                for i in 0..batch.num_rows() {
                    let mut properties = HashMap::new();
                    let mut embedding: Option<Vec<f64>> = None;

                    for (field, column) in schema.fields().iter().zip(batch.columns()) {
                        if field.name() == "embedding" {
                            let list_array = column.as_any().downcast_ref::<ListArray>();
                            if let Some(list_array) = list_array {
                                if !list_array.is_null(i) {
                                    let values = list_array.value(i);
                                    let float_array = values
                                        .as_any()
                                        .downcast_ref::<Float32Array>()
                                        .expect("embedding list should contain f32 values");
                                    let mut vec = Vec::with_capacity(float_array.len());
                                    for idx in 0..float_array.len() {
                                        vec.push(float_array.value(idx) as f64);
                                    }
                                    embedding = Some(vec);
                                }
                            }
                        } else if let Some(value) = Self::arrow_value_to_helix_value(column, i) {
                            properties.insert(field.name().clone(), value);
                        }
                    }

                    let Some(embedding_vec) = embedding else {
                        log::warn!(
                            "Skipping vector of type '{}' at row {} due to missing embedding values",
                            entity_type,
                            i
                        );
                        continue;
                    };

                    if embedding_vec.is_empty() {
                        log::warn!(
                            "Skipping vector of type '{}' at row {} because embedding is empty",
                            entity_type,
                            i
                        );
                        continue;
                    }

                    let props_vec: Vec<(String, Value)> = properties.into_iter().collect();
                    let fields_opt = if props_vec.is_empty() {
                        None
                    } else {
                        Some(props_vec)
                    };

                    let _ = G::new_mut(self.engine.storage.clone(), &mut txn)
                        .insert_v::<fn(&HVector, &RoTxn) -> bool>(
                            &embedding_vec,
                            entity_type,
                            fields_opt,
                        )
                        .collect_to::<Vec<_>>();
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
                                "id" => edge_id_str = Some(value.inner_stringify()),
                                "from_node_id" => from_node_id_str = Some(value.inner_stringify()),
                                "to_node_id" => to_node_id_str = Some(value.inner_stringify()),
                                _ => {
                                    properties.insert(field.name().clone(), value);
                                }
                            }
                        }
                    }

                    let (from_str, to_str) = if let (Some(from), Some(to)) =
                        (from_node_id_str.clone(), to_node_id_str.clone())
                    {
                        (from, to)
                    } else {
                        log::warn!(
                            "Skipping edge of type '{}' at row {} due to missing from_node_id or to_node_id",
                            entity_type,
                            i
                        );
                        continue;
                    };

                    let id_u128 = if let Some(id_str) = edge_id_str.clone() {
                        match Uuid::parse_str(&id_str) {
                            Ok(id) => id.as_u128(),
                            Err(_) => {
                                log::warn!("Failed to parse UUID for edge id: {}", id_str);
                                continue;
                            }
                        }
                    } else {
                        utils::id::stable_edge_id_u128(entity_type, &from_str, &to_str)
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

                    match self.engine.storage.get_edge(&txn, &id_u128) {
                        Ok(existing_edge) => {
                            if existing_edge.from_node != from_u128
                                || existing_edge.to_node != to_u128
                            {
                                self.engine
                                    .storage
                                    .drop_edge(&mut txn, &id_u128)
                                    .map_err(|e| StorageError::SyncError(e.to_string()))?;
                                self.insert_edge_into_engine(
                                    &mut txn,
                                    id_u128,
                                    entity_type,
                                    properties,
                                    from_u128,
                                    to_u128,
                                )?;
                                log::debug!(
                                    "Rewriting edge: {} ({})",
                                    Uuid::from_u128(id_u128).to_string(),
                                    entity_type
                                );
                                continue;
                            } else {
                                let props_vec: Vec<(String, Value)> =
                                    properties.into_iter().map(|(k, v)| (k, v)).collect();
                                let traversal = G::new(self.engine.storage.clone(), &txn)
                                    .e_from_id(&id_u128)
                                    .collect_to::<Vec<_>>();
                                G::new_mut_from(self.engine.storage.clone(), &mut txn, traversal)
                                    .update(Some(props_vec))
                                    .for_each(|_| {});
                                log::debug!(
                                    "Updating edge: {} ({})",
                                    Uuid::from_u128(id_u128).to_string(),
                                    entity_type
                                );
                                continue;
                            }
                        }
                        Err(_) => {
                            self.insert_edge_into_engine(
                                &mut txn,
                                id_u128,
                                entity_type,
                                properties,
                                from_u128,
                                to_u128,
                            )?;
                            log::debug!(
                                "Inserting edge: {} ({})",
                                Uuid::from_u128(id_u128).to_string(),
                                entity_type
                            );
                        }
                    }
                }
            }
        }

        txn.commit()?;
        Ok(())
    }

    fn build_node_index_batch(
        entity_type: &str,
        batch: &RecordBatch,
        primary_keys: &[String],
    ) -> Result<Option<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let schema = batch.schema();
        let mut ids: Vec<String> = Vec::with_capacity(batch.num_rows());
        let mut pk_columns: HashMap<String, Vec<Option<String>>> = primary_keys
            .iter()
            .map(|k| (k.clone(), Vec::with_capacity(batch.num_rows())))
            .collect();
        let mut updated: Vec<Option<chrono::DateTime<chrono::Utc>>> =
            Vec::with_capacity(batch.num_rows());

        for row in 0..batch.num_rows() {
            let mut node_id_str: Option<String> = None;
            if let Ok(idx) = schema.index_of("id") {
                let column = batch.column(idx);
                if let Some(value) = Self::arrow_value_to_helix_value(column, row) {
                    node_id_str = Some(value.inner_stringify());
                }
            }

            let mut pk_values: HashMap<String, Option<String>> = HashMap::new();
            for key in primary_keys {
                let value = if let Ok(idx) = schema.index_of(key) {
                    let column = batch.column(idx);
                    if let Some(value) = Self::arrow_value_to_helix_value(column, row) {
                        Some(value.inner_stringify())
                    } else {
                        None
                    }
                } else {
                    None
                };
                pk_values.insert(key.clone(), value);
            }

            let id_u128 = if let Some(id_str) = node_id_str {
                match Uuid::parse_str(&id_str) {
                    Ok(id) => id.as_u128(),
                    Err(_) => {
                        log::warn!(
                            "Failed to parse UUID for node id '{}' while building index for '{}'",
                            id_str,
                            entity_type
                        );
                        continue;
                    }
                }
            } else {
                let pk_pairs: Vec<(&str, String)> = primary_keys
                    .iter()
                    .filter_map(|key| {
                        pk_values
                            .get(key)
                            .and_then(|value| value.clone().map(|val| (key.as_str(), val)))
                    })
                    .collect();
                if pk_pairs.len() != primary_keys.len() || pk_pairs.is_empty() {
                    log::warn!(
                        "Skipping index entry for '{}' row {} due to missing id and primary keys",
                        entity_type,
                        row
                    );
                    continue;
                }
                utils::id::stable_node_id_u128(entity_type, &pk_pairs)
            };

            let id_string = Uuid::from_u128(id_u128).to_string();
            ids.push(id_string);
            updated.push(Some(Utc::now()));

            for key in primary_keys {
                if let Some(column) = pk_columns.get_mut(key) {
                    column.push(pk_values.get(key).cloned().unwrap_or(None));
                }
            }
        }

        if ids.is_empty() {
            return Ok(None);
        }

        let mut fields = Vec::new();
        fields.push(Field::new("id", DataType::Utf8, false));
        let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
        arrays.push(Arc::new(StringArray::from(ids)) as Arc<dyn Array>);

        for key in primary_keys {
            fields.push(Field::new(key, DataType::Utf8, true));
            if let Some(values) = pk_columns.get(key) {
                arrays.push(Arc::new(StringArray::from(values.clone())) as Arc<dyn Array>);
            } else {
                arrays.push(
                    Arc::new(StringArray::from(vec![None::<String>; updated.len()]))
                        as Arc<dyn Array>,
                );
            }
        }

        fields.push(Field::new(
            "updated_at",
            DataType::Timestamp(
                deltalake::arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
            true,
        ));
        arrays.push(auto_fetchable::to_arrow_array(updated)?);

        let index_schema = Schema::new(fields);

        let batch = RecordBatch::try_new(Arc::new(index_schema), arrays)?;

        Ok(Some(batch))
    }

    async fn process_vector_collection(
        &self,
        _fetchable_collection: Box<dyn crate::fetch::AnyFetchable>,
        mut record_batch: RecordBatch,
        entity_type: &str,
        table_name: String,
        merge_keys: Vec<String>,
    ) -> Result<()> {
        let schema = record_batch.schema();
        let num_rows = record_batch.num_rows();

        if num_rows == 0 {
            let merge_on = if merge_keys.is_empty() {
                None
            } else {
                Some(merge_keys.clone())
            };
            self.lake
                .write_batches(&table_name, vec![record_batch], merge_on)
                .await?;
            self.catalog.ensure_ingestion_offset(
                &table_name,
                entity_type,
                crate::fetch::EntityCategory::Vector,
                &merge_keys,
            )?;
            return Ok(());
        }

        // Ensure the entity is known within the registry for consistent metadata.
        let _vector_meta = SCHEMA_REGISTRY.entity(entity_type).ok_or_else(|| {
            StorageError::InvalidArg(format!(
                "Vector entity type '{}' is not registered in schema metadata",
                entity_type
            ))
        })?;

        let rules = vector_rules(entity_type);
        let vector_index_meta = vector_index(entity_type).cloned();

        let id_idx = schema.column_with_name("id").map(|(idx, _)| idx);
        let created_at_idx = schema.column_with_name("created_at").map(|(idx, _)| idx);

        let columns = record_batch.columns();
        let mut column_index: HashMap<String, usize> = HashMap::new();
        for (idx, field) in schema.fields().iter().enumerate() {
            column_index.insert(field.name().clone(), idx);
        }

        // Load existing vector UUIDs for embedding IDs that appear in this batch.
        let mut existing_index: HashMap<String, String> = HashMap::new();
        if let Some(meta) = vector_index_meta.as_ref() {
            if let Some((idx, _)) = schema.column_with_name(meta.id_column) {
                if let Some(arr) = columns[idx].as_any().downcast_ref::<StringArray>() {
                    let mut unique_ids: HashSet<String> = HashSet::new();
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            continue;
                        }
                        unique_ids.insert(arr.value(row).to_string());
                    }
                    if !unique_ids.is_empty() {
                        let ids: Vec<String> = unique_ids.into_iter().collect();
                        existing_index = self
                            .lake
                            .load_vector_index_map(meta.index_table, meta.id_column, &ids)
                            .await?;
                    }
                } else {
                    log::warn!(
                        "Vector entity '{}' expects id column '{}' but batch column is not Utf8; dedup disabled.",
                        entity_type,
                        meta.id_column
                    );
                }
            } else {
                log::warn!(
                    "Vector entity '{}' missing id column '{}' in batch schema; dedup disabled for this batch.",
                    entity_type,
                    meta.id_column
                );
            }
        }

        let mut vector_ids: Vec<Option<String>> = Vec::with_capacity(num_rows);
        let mut edges_by_type: HashMap<String, Vec<EdgeWrite>> = HashMap::new();
        let mut index_updates: HashMap<String, VectorIndexWrite> = HashMap::new();

        let mut txn = self.engine.storage.graph_env.write_txn()?;

        for row in 0..num_rows {
            let mut properties = HashMap::new();
            let mut embedding: Option<Vec<f64>> = None;
            let mut row_created_at: Option<DateTime<Utc>> = None;
            let mut id_value: Option<String> = None;

            let existing_id = id_idx.and_then(|idx| {
                columns[idx]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .and_then(|arr| {
                        if arr.is_null(row) {
                            None
                        } else {
                            Some(arr.value(row).to_string())
                        }
                    })
            });

            if let Some(idx) = created_at_idx {
                if let Some(arr) = columns[idx]
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                {
                    if !arr.is_null(row) {
                        let value = arr.value(row);
                        let seconds = value.div_euclid(1_000_000);
                        let micros = (value.rem_euclid(1_000_000)) as u32;
                        if let Some(datetime) =
                            DateTime::<Utc>::from_timestamp(seconds, micros * 1000)
                        {
                            row_created_at = Some(datetime);
                        }
                    }
                }
            }

            for (field, column) in schema.fields().iter().zip(columns.iter()) {
                match field.name().as_str() {
                    "embedding" => {
                        let list_array = column.as_any().downcast_ref::<ListArray>();
                        if let Some(list_array) = list_array {
                            if !list_array.is_null(row) {
                                let values = list_array.value(row);
                                let float_array = values
                                    .as_any()
                                    .downcast_ref::<Float32Array>()
                                    .expect("embedding list should contain f32 values");
                                let mut vec = Vec::with_capacity(float_array.len());
                                for idx in 0..float_array.len() {
                                    vec.push(float_array.value(idx) as f64);
                                }
                                embedding = Some(vec);
                            }
                        }
                    }
                    "id" => {}
                    name if vector_index_meta
                        .as_ref()
                        .map(|meta| meta.id_column == name)
                        .unwrap_or(false) =>
                    {
                        if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
                            if !arr.is_null(row) {
                                let value = arr.value(row).to_string();
                                id_value = Some(value.clone());
                                properties.insert(field.name().clone(), Value::String(value));
                            }
                        }
                    }
                    _ => {
                        if let Some(value) = Self::arrow_value_to_helix_value(column, row) {
                            properties.insert(field.name().clone(), value);
                        }
                    }
                }
            }

            let existing_uuid = id_value
                .as_ref()
                .and_then(|id| existing_index.get(id).cloned());

            let vector_uuid_final: String;
            let vector_record_identity_final: String;

            if let Some(existing_uuid) = existing_uuid {
                let record_identity = existing_id.clone().unwrap_or(existing_uuid.clone());
                vector_ids.push(Some(record_identity.clone()));
                vector_uuid_final = existing_uuid;
                vector_record_identity_final = record_identity;
            } else {
                let Some(embedding_vec) = embedding else {
                    log::warn!(
                        "Skipping vector of type '{}' at row {} due to missing embedding values",
                        entity_type,
                        row
                    );
                    vector_ids.push(existing_id);
                    continue;
                };

                if embedding_vec.is_empty() {
                    log::warn!(
                        "Skipping vector of type '{}' at row {} because embedding is empty",
                        entity_type,
                        row
                    );
                    vector_ids.push(existing_id);
                    continue;
                }

                let props_vec: Vec<(String, Value)> = properties.clone().into_iter().collect();
                let fields_opt = if props_vec.is_empty() {
                    None
                } else {
                    Some(props_vec)
                };

                let traversal = G::new_mut(self.engine.storage.clone(), &mut txn)
                    .insert_v::<fn(&HVector, &RoTxn) -> bool>(
                        &embedding_vec,
                        entity_type,
                        fields_opt,
                    )
                    .collect_to_obj();
                let new_uuid = traversal.uuid();
                let record_identity = existing_id.clone().unwrap_or_else(|| new_uuid.clone());
                vector_ids.push(Some(record_identity.clone()));

                if let (Some(_), Some(id)) = (vector_index_meta.as_ref(), id_value.as_ref()) {
                    let timestamp = row_created_at.clone().unwrap_or_else(|| {
                        let now = Utc::now();
                        row_created_at = Some(now);
                        now
                    });
                    existing_index.insert(id.clone(), new_uuid.clone());
                    index_updates.insert(
                        id.clone(),
                        VectorIndexWrite {
                            id_value: id.clone(),
                            vector_uuid: new_uuid.clone(),
                            updated_at: Some(timestamp),
                        },
                    );
                }

                vector_uuid_final = new_uuid;
                vector_record_identity_final = record_identity;
            }

            if let (Some(_), Some(id)) = (vector_index_meta.as_ref(), id_value.as_ref()) {
                let timestamp = row_created_at.clone().unwrap_or_else(|| {
                    let now = Utc::now();
                    row_created_at = Some(now);
                    now
                });
                existing_index.insert(id.clone(), vector_uuid_final.clone());
                index_updates
                    .entry(id.clone())
                    .and_modify(|entry| {
                        entry.vector_uuid = vector_uuid_final.clone();
                        entry.updated_at = Some(timestamp);
                    })
                    .or_insert(VectorIndexWrite {
                        id_value: id.clone(),
                        vector_uuid: vector_uuid_final.clone(),
                        updated_at: Some(timestamp),
                    });
            }

            if let Some(rule_set) = rules {
                for rule in &rule_set.rules {
                    match Self::resolve_vector_rule(
                        rule,
                        &columns,
                        &column_index,
                        row,
                        row_created_at,
                        &vector_uuid_final,
                        &vector_record_identity_final,
                    ) {
                        Ok(Some(edge)) => {
                            edges_by_type
                                .entry(rule.edge_type.to_string())
                                .or_default()
                                .push(edge);
                        }
                        Ok(None) => {}
                        Err(err) => {
                            log::warn!(
                                "Failed to materialize edge '{}' for vector '{}' row {}: {}",
                                rule.edge_type,
                                entity_type,
                                row,
                                err
                            );
                        }
                    }
                }
            }
        }

        txn.commit()?;

        if let Some(idx) = id_idx {
            let id_array = auto_fetchable::to_arrow_array(vector_ids)?;
            let mut columns = record_batch.columns().to_vec();
            columns[idx] = id_array;
            record_batch = RecordBatch::try_new(schema.clone(), columns)?;
        }

        let merge_on = if merge_keys.is_empty() {
            None
        } else {
            Some(merge_keys.clone())
        };
        self.lake
            .write_batches(&table_name, vec![record_batch.clone()], merge_on)
            .await?;
        self.catalog.ensure_ingestion_offset(
            &table_name,
            entity_type,
            crate::fetch::EntityCategory::Vector,
            &merge_keys,
        )?;

        for (edge_type, edges) in edges_by_type.into_iter() {
            if edges.is_empty() {
                continue;
            }

            let edge_batch = Self::build_edge_record_batch(&edges)?;
            let edge_table = format!(
                "silver/edges/{}",
                edge_type
                    .strip_prefix("edge_")
                    .unwrap_or(&edge_type)
                    .to_lowercase()
            );

            self.lake
                .write_batches(
                    &edge_table,
                    vec![edge_batch.clone()],
                    Some(vec!["id".to_string()]),
                )
                .await?;
            self.catalog.ensure_ingestion_offset(
                &edge_table,
                &edge_type,
                crate::fetch::EntityCategory::Edge,
                &vec!["id".to_string()],
            )?;
            self.update_engine_from_batch_with_meta(
                &edge_type,
                crate::fetch::EntityCategory::Edge,
                &vec!["id".to_string()],
                &edge_batch,
            )?;
        }

        if let (Some(meta), true) = (vector_index_meta.as_ref(), !index_updates.is_empty()) {
            let updates: Vec<VectorIndexWrite> = index_updates.into_values().collect();
            let index_batch = Self::build_vector_index_batch(meta.id_column, &updates)?;
            self.lake
                .write_batches(
                    meta.index_table,
                    vec![index_batch.clone()],
                    Some(vec![meta.id_column.to_string()]),
                )
                .await?;
            self.catalog.ensure_ingestion_offset(
                meta.index_table,
                entity_type,
                crate::fetch::EntityCategory::Vector,
                &vec![meta.id_column.to_string()],
            )?;
        }

        Ok(())
    }

    fn insert_edge_into_engine(
        &self,
        txn: &mut RwTxn<'_>,
        id_u128: u128,
        entity_type: &str,
        properties: HashMap<String, Value>,
        from_u128: u128,
        to_u128: u128,
    ) -> Result<()> {
        let edge = Edge {
            id: id_u128,
            label: entity_type.to_string(),
            version: self.engine.storage.version_info.get_latest(entity_type),
            properties: Some(properties),
            from_node: from_u128,
            to_node: to_u128,
        };

        let bytes = edge.encode_edge()?;
        self.engine.storage.edges_db.put(txn, &id_u128, &bytes)?;

        let label_hash = hash_label(&edge.label, None);
        self.engine.storage.out_edges_db.put(
            txn,
            &helix_db::helix_engine::storage_core::HelixGraphStorage::out_edge_key(
                &edge.from_node,
                &label_hash,
            ),
            &helix_db::helix_engine::storage_core::HelixGraphStorage::pack_edge_data(
                &edge.id,
                &edge.to_node,
            ),
        )?;
        self.engine.storage.in_edges_db.put(
            txn,
            &helix_db::helix_engine::storage_core::HelixGraphStorage::in_edge_key(
                &edge.to_node,
                &label_hash,
            ),
            &helix_db::helix_engine::storage_core::HelixGraphStorage::pack_edge_data(
                &edge.id,
                &edge.from_node,
            ),
        )?;
        Ok(())
    }
}

#[async_trait]
impl DataSynchronizer for FStorageSynchronizer {
    async fn process_graph_data(&self, graph_data: GraphData) -> Result<()> {
        // --- STAGE 2: Persistence - Process all entities (original and newly created) ---
        for fetchable_collection in graph_data.entities {
            let record_batch = fetchable_collection.to_record_batch_any()?;
            let entity_type = fetchable_collection.entity_type_any();
            let category = fetchable_collection.category_any();
            let table_name = match category {
                EntityCategory::Edge => {
                    let edge_suffix = entity_type
                        .strip_prefix("edge_")
                        .unwrap_or(entity_type)
                        .to_lowercase();
                    format!("silver/edges/{}", edge_suffix)
                }
                _ => fetchable_collection.table_name(),
            };
            let merge_keys: Vec<String> = fetchable_collection
                .primary_keys_any()
                .into_iter()
                .map(|k| k.to_string())
                .collect();

            if matches!(category, EntityCategory::Vector) {
                self.process_vector_collection(
                    fetchable_collection,
                    record_batch,
                    entity_type,
                    table_name,
                    merge_keys,
                )
                .await?;
                continue;
            }

            let merge_on = if merge_keys.is_empty() {
                None
            } else {
                Some(merge_keys.clone())
            };
            self.lake
                .write_batches(&table_name, vec![record_batch.clone()], merge_on)
                .await?;
            self.catalog.ensure_ingestion_offset(
                &table_name,
                entity_type,
                category,
                &merge_keys,
            )?;

            if matches!(category, EntityCategory::Node) {
                if let Some(index_batch) =
                    Self::build_node_index_batch(entity_type, &record_batch, &merge_keys)?
                {
                    if merge_keys.is_empty() {
                        log::debug!(
                            "Skipping index write for '{}' because no primary keys are defined",
                            entity_type
                        );
                    } else {
                        let index_table_name = format!("silver/index/{}", entity_type);
                        let index_merge_keys = merge_keys.clone();
                        self.lake
                            .write_batches(
                                &index_table_name,
                                vec![index_batch],
                                Some(index_merge_keys.clone()),
                            )
                            .await?;
                        self.catalog.ensure_ingestion_offset(
                            &index_table_name,
                            entity_type,
                            category,
                            &index_merge_keys,
                        )?;
                    }
                }
            }

            // Hot Path: Write to Graph Engine
            self.update_engine_from_batch(fetchable_collection, &record_batch)?;
        }

        Ok(())
    }
    fn register_fetcher(&self, fetcher: Arc<dyn Fetcher>) {
        let name = fetcher.name().to_string();
        let mut guard = self.fetchers.write().unwrap();
        guard.insert(name, fetcher);
    }

    fn list_fetcher_capabilities(&self) -> Vec<FetcherCapability> {
        let guard = self.fetchers.read().unwrap();
        let mut caps: Vec<_> = guard.values().map(|fetcher| fetcher.capability()).collect();
        caps.sort_by(|a, b| a.name.cmp(b.name));
        caps
    }

    async fn check_readiness(
        &self,
        entities: &[EntityIdentifier],
    ) -> Result<HashMap<String, ReadinessReport>> {
        let mut reports = HashMap::new();
        let now = chrono::Utc::now().timestamp();

        for entity in entities {
            let readiness_record = self.catalog.get_readiness(&entity.uri)?;
            let mut coverage_metrics = serde_json::Value::Null;
            let mut ttl_fresh = false;
            let mut gap = None;

            if let Some(ref readiness) = readiness_record {
                coverage_metrics = serde_json::from_str(&readiness.coverage_metrics)
                    .unwrap_or(serde_json::Value::Null);
                if let (Some(last_synced), Some(ttl)) =
                    (readiness.last_synced_at, readiness.ttl_seconds)
                {
                    let delta = now - last_synced;
                    gap = Some(delta);
                    ttl_fresh = delta < ttl;
                }
            }

            let mut anchor_fresh = true;
            let mut probe_report: Option<ProbeReport> = None;

            if let Some(fetcher_name) = entity.fetcher_name.as_deref() {
                let fetcher_arc = {
                    let guard = self.fetchers.read().unwrap();
                    guard.get(fetcher_name).cloned()
                };
                if let Some(fetcher) = fetcher_arc {
                    let anchor_key = entity.anchor_key.as_deref().unwrap_or("default");
                    let stored_anchor =
                        self.catalog
                            .get_source_anchor(&entity.uri, fetcher_name, anchor_key)?;
                    let local_anchor_value =
                        stored_anchor.and_then(|anchor| anchor.anchor_value.clone());
                    let params = entity
                        .params
                        .clone()
                        .unwrap_or_else(|| serde_json::Value::Null);
                    match fetcher.probe(params).await {
                        Ok(mut report) => {
                            if report.anchor_key.is_none() {
                                report.anchor_key = Some(anchor_key.to_string());
                            }
                            if report.local_anchor.is_none() {
                                report.local_anchor = local_anchor_value.clone();
                            }
                            anchor_fresh = match (&report.remote_anchor, &report.local_anchor) {
                                (Some(remote), Some(local)) => remote == local,
                                (Some(_), None) => false,
                                (None, _) => report.fresh.unwrap_or(true),
                            };
                            report.fresh = Some(anchor_fresh);
                            probe_report = Some(report);
                        }
                        Err(err) => {
                            log::warn!(
                                "Probe for entity '{}' via fetcher '{}' failed: {}",
                                entity.uri,
                                fetcher_name,
                                err
                            );
                            anchor_fresh = false;
                        }
                    }
                } else {
                    log::debug!(
                        "Fetcher '{}' requested for readiness probe but not registered.",
                        fetcher_name
                    );
                    anchor_fresh = false;
                }
            }

            let is_fresh = ttl_fresh && anchor_fresh;

            let report = ReadinessReport {
                is_fresh,
                freshness_gap_seconds: gap,
                coverage_metrics,
                probe_report,
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

        let fetcher = {
            let guard = self.fetchers.read().unwrap();
            guard.get(fetcher_name).cloned()
        }
        .ok_or_else(|| {
            StorageError::Config(format!("Fetcher '{}' not registered.", fetcher_name))
        })?;
        let capability = fetcher.capability();
        let ttl_default = capability.default_ttl_secs.unwrap_or(3600);

        // The fetcher is now responsible for all transformation, including vectorization.
        let response = fetcher
            .fetch(params.clone(), self.embedding_provider.clone())
            .await?;

        match response {
            FetchResponse::GraphData(graph_data) => {
                self.process_graph_data(graph_data).await?;
            }
            FetchResponse::PanelData { table_name, batch } => {
                log::info!("Cold Path: Writing panel data to table '{}'", &table_name);
                self.lake
                    .write_batches(&table_name, vec![batch], None)
                    .await?;
            }
        }

        let now = chrono::Utc::now().timestamp();
        for entity in &context.target_entities {
            let readiness = crate::models::EntityReadiness {
                entity_uri: entity.uri.clone(),
                entity_type: entity.entity_type.clone(),
                last_synced_at: Some(now),
                ttl_seconds: Some(ttl_default),
                coverage_metrics: "{}".to_string(),
            };
            self.catalog.upsert_readiness(&readiness)?;

            if entity
                .fetcher_name
                .as_deref()
                .map(|name| name == fetcher_name)
                .unwrap_or(false)
            {
                let anchor_key = entity.anchor_key.as_deref().unwrap_or("default");
                let probe_params = entity
                    .params
                    .clone()
                    .unwrap_or_else(|| serde_json::Value::Null);
                match fetcher.probe(probe_params).await {
                    Ok(report) => {
                        let anchor_value_ref = report.remote_anchor.as_deref();
                        self.catalog.upsert_source_anchor(
                            &entity.uri,
                            fetcher_name,
                            anchor_key,
                            anchor_value_ref,
                            now,
                        )?;
                    }
                    Err(err) => {
                        log::warn!(
                            "Post-sync probe for entity '{}' via fetcher '{}' failed: {}",
                            entity.uri,
                            fetcher_name,
                            err
                        );
                    }
                }
            }
        }

        self.catalog
            .update_task_log_status(task_id, "SUCCESS", "Sync completed successfully.")?;

        Ok(())
    }

    async fn run_full_etl_from_lake(&self, target_repo_uri: &str) -> Result<()> {
        let task_name = format!("full_etl_for_{}", target_repo_uri);
        let task_id = self.catalog.create_task_log(&task_name)?;
        log::info!("Starting ETL from Lake to Engine for {}", target_repo_uri);
        let offsets = self.catalog.list_ingestion_offsets()?;
        let mut processed_tables = 0usize;

        for offset in offsets {
            let (changes, latest_version) = self
                .lake
                .read_changes_since(&offset.table_path, offset.last_version)
                .await?;
            if changes.is_empty() {
                continue;
            }
            let primary_keys = offset.primary_keys.clone();
            for (version, batches) in changes {
                for batch in batches {
                    self.update_engine_from_batch_with_meta(
                        &offset.entity_type,
                        offset.category,
                        &primary_keys,
                        &batch,
                    )?;
                }
                self.catalog
                    .update_ingestion_offset(&offset.table_path, version)?;
            }
            if latest_version > offset.last_version {
                processed_tables += 1;
            }
        }

        let status_message = if processed_tables > 0 {
            format!("Processed {} table(s) from lake.", processed_tables)
        } else {
            "No new lake updates to process.".to_string()
        };

        self.catalog
            .update_task_log_status(task_id, "SUCCESS", &status_message)?;
        Ok(())
    }
}

fn extract_node_type_from_key(key: &str) -> Option<&str> {
    key.splitn(2, "::")
        .next()
        .filter(|segment| !segment.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use crate::embedding::NullEmbeddingProvider;
    use crate::fetch::Fetchable;
    use crate::schemas::generated_schemas::{Project, ReadmeChunk};
    use chrono::Utc;
    use helix_db::helix_engine::traversal_core::HelixGraphEngineOpts;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_run_full_etl_updates_offsets() {
        let dir = tempdir().unwrap();
        let config = StorageConfig::new(dir.path());
        tokio::fs::create_dir_all(&config.engine_path)
            .await
            .unwrap();

        let catalog = Arc::new(Catalog::new(&config).unwrap());
        catalog.initialize_schema().unwrap();

        let engine_opts = HelixGraphEngineOpts {
            path: config.engine_path.to_str().unwrap().to_string(),
            ..Default::default()
        };
        let engine = Arc::new(HelixGraphEngine::new(engine_opts).unwrap());
        let lake = Arc::new(
            Lake::new(config.clone(), Arc::clone(&engine))
                .await
                .unwrap(),
        );

        let synchronizer = FStorageSynchronizer::new(
            Arc::clone(&catalog),
            Arc::clone(&lake),
            Arc::clone(&engine),
            Arc::new(NullEmbeddingProvider),
        );

        let mut graph_data = GraphData::new();
        graph_data.add_entities(vec![Project {
            url: Some("https://example.com/repo".to_string()),
            name: Some("alpha".to_string()),
            description: None,
            language: None,
            stars: None,
            forks: None,
        }]);

        graph_data.add_entities(vec![ReadmeChunk {
            id: None,
            project_url: Some("https://example.com/repo".to_string()),
            revision_sha: Some("alpha-sha".to_string()),
            source_file: Some("README.md".to_string()),
            start_line: Some(1),
            end_line: Some(5),
            text: Some("alpha project".to_string()),
            embedding: Some(vec![0.5_f32, 0.25_f32, 0.25_f32]),
            embedding_model: Some("fixture".to_string()),
            embedding_id: Some("alpha-readme-1".to_string()),
            token_count: Some(4),
            chunk_order: Some(0),
            created_at: Some(Utc::now()),
            updated_at: None,
        }]);

        synchronizer.process_graph_data(graph_data).await.unwrap();

        let offset = catalog
            .get_ingestion_offset(&Project::table_name())
            .unwrap()
            .unwrap();
        assert_eq!(offset.last_version, -1);

        synchronizer
            .run_full_etl_from_lake("test_repo")
            .await
            .unwrap();

        let offset_after = catalog
            .get_ingestion_offset(&Project::table_name())
            .unwrap()
            .unwrap();
        assert_eq!(offset_after.last_version, 0);

        let mut updated_data = GraphData::new();
        updated_data.add_entities(vec![Project {
            url: Some("https://example.com/repo".to_string()),
            name: Some("beta".to_string()),
            description: None,
            language: None,
            stars: Some(10),
            forks: None,
        }]);
        synchronizer.process_graph_data(updated_data).await.unwrap();

        synchronizer
            .run_full_etl_from_lake("test_repo")
            .await
            .unwrap();

        let offset_final = catalog
            .get_ingestion_offset(&Project::table_name())
            .unwrap()
            .unwrap();
        assert_eq!(offset_final.last_version, 1);
    }
}
