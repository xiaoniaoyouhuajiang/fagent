use crate::config::StorageConfig;
use crate::errors::{Result, StorageError};
use crate::models::{
    ColumnSummary, HybridSearchHit, MultiEntitySearchHit, TableSummary, TextSearchHit,
    VectorSearchHit,
};
use crate::utils;
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use deltalake::arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, UInt32Array, UInt64Array,
};
use deltalake::arrow::datatypes::DataType;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::datasource::MemTable;
use deltalake::datafusion::datasource::TableProvider;
use deltalake::datafusion::execution::context::{SessionConfig, SessionContext};
use deltalake::kernel::Action;
use deltalake::operations::DeltaOps;
use deltalake::protocol::SaveMode;
use deltalake::DeltaTable;
use deltalake::DeltaTableBuilder;
use deltalake::ObjectStore;
use deltalake::Path;
use heed3::RoTxn;
use helix_db::helix_engine::bm25::bm25::BM25;
use helix_db::helix_engine::storage_core::storage_methods::StorageMethods;
use helix_db::helix_engine::storage_core::HelixGraphStorage;
use helix_db::helix_engine::traversal_core::HelixGraphEngine;
use helix_db::helix_engine::types::{GraphError, VectorError};
use helix_db::helix_engine::vector_core::hnsw::HNSW;
use helix_db::helix_engine::vector_core::vector::HVector;
use helix_db::protocol::value::Value as HelixValue;
use helix_db::utils::items::{Edge, Node};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

async fn read_parquet_batches(
    object_store: Arc<dyn ObjectStore>,
    path: &str,
) -> Result<Vec<RecordBatch>> {
    let object_path = Path::from(path);
    let meta = object_store.get(&object_path).await?.bytes().await?;
    let reader = deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        meta.clone(),
    )
    .map_err(|e| StorageError::Other(e.into()))?
    .build()
    .map_err(|e| StorageError::Other(e.into()))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| StorageError::Other(e.into()))?);
    }
    Ok(batches)
}

pub struct Lake {
    pub(crate) config: StorageConfig,
    engine: Arc<HelixGraphEngine>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NeighborDirection {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NeighborEdgeOrientation {
    Outgoing,
    Incoming,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NeighborRecord {
    pub orientation: NeighborEdgeOrientation,
    pub edge: HashMap<String, JsonValue>,
    pub node_id: String,
    pub node: Option<HashMap<String, JsonValue>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Subgraph {
    pub nodes: Vec<HashMap<String, JsonValue>>,
    pub edges: Vec<HashMap<String, JsonValue>>,
}

impl Lake {
    fn extract_text_field(map: &HashMap<String, JsonValue>, keys: &[&str]) -> Option<String> {
        for key in keys {
            if let Some(value) = map.get(*key).and_then(|value| value.as_str()) {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }

        if let Some(properties) = map.get("properties").and_then(|value| value.as_object()) {
            for key in keys {
                if let Some(value) = properties.get(*key).and_then(|value| value.as_str()) {
                    let trimmed = value.trim();
                    if !trimmed.is_empty() {
                        return Some(trimmed.to_string());
                    }
                }
            }
        }

        None
    }

    pub async fn new(config: StorageConfig, engine: Arc<HelixGraphEngine>) -> Result<Self> {
        tokio::fs::create_dir_all(&config.lake_path).await?;
        Ok(Self { config, engine })
    }

    #[inline]
    fn single_partition_session() -> SessionContext {
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1))
    }

    /// Convert a file path to a URL for Delta Lake operations
    fn path_to_url(&self, path: &std::path::Path) -> Result<Url> {
        // Use absolute path instead of canonicalize to avoid errors when path doesn't exist yet
        let absolute_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|e| StorageError::Io(e))?
                .join(path)
        };

        // Ensure the path and its parent directories exist
        std::fs::create_dir_all(&absolute_path).map_err(|e| StorageError::Io(e))?;

        Url::from_file_path(absolute_path)
            .map_err(|_| StorageError::Config(format!("Invalid path: {:?}", path)))
    }

    // create delta table
    pub async fn get_or_create_table(&self, table_name: &str) -> Result<DeltaTable> {
        let table_path = self.config.lake_path.join(table_name);

        // 确保父目录存在
        if let Some(parent) = table_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::create_dir_all(&table_path).await?;

        if let Some(parent) = table_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let table_uri = self.path_to_url(&table_path)?;

        match deltalake::open_table(table_uri.clone()).await {
            Ok(table) => Ok(table),
            Err(deltalake::DeltaTableError::NotATable(_)) => {
                // 如果表尚未初始化，返回一个尚未加载的 DeltaTable 句柄，
                // 后续写入操作会在第一次写入时创建表并注入 Schema。
                let table = DeltaTableBuilder::from_uri(table_uri.clone())?.build()?;
                Ok(table)
            }
            Err(e) => Err(StorageError::from(e)),
        }
    }

    /// 将RecordBatch写入指定的Delta Table，支持主键幂等写（基于 `merge_on`）。
    pub async fn write_batches(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        merge_on: Option<Vec<String>>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let table_path = self.config.lake_path.join(table_name);
        let table_uri = self.path_to_url(&table_path)?;
        let delta_log_path = table_path.join("_delta_log");
        let table_exists = tokio::fs::metadata(&delta_log_path).await.is_ok();

        if !table_exists {
            let table_display_name = table_name.replace('/', "_");
            DeltaOps::try_from_uri(table_uri)
                .await?
                .write(batches.clone())
                .with_save_mode(SaveMode::Overwrite)
                .with_table_name(table_display_name)
                .await?;
            return Ok(());
        }

        if let Some(keys) = merge_on.clone() {
            // If the table already exists, rewrite it with de-duplicated data using DataFusion.
            match deltalake::open_table(table_uri).await {
                Ok(existing_table) => {
                    let schema = batches
                        .get(0)
                        .map(|b| b.schema())
                        .ok_or_else(|| StorageError::InvalidArg("Missing batch schema".into()))?;

                    let ctx = Self::single_partition_session();
                    let mem_table = MemTable::try_new(schema.clone(), vec![batches.clone()])
                        .map_err(|e| StorageError::Other(e.into()))?;
                    ctx.register_table("new_data", Arc::new(mem_table))
                        .map_err(|e| StorageError::Other(e.into()))?;

                    let provider_table = existing_table.clone();
                    ctx.register_table("existing", Arc::new(provider_table))
                        .map_err(|e| StorageError::Other(e.into()))?;

                    let key_list = keys
                        .iter()
                        .map(|k| format!("\"{}\"", k))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let sql = format!(
                        "SELECT * FROM new_data UNION ALL SELECT existing.* FROM existing LEFT ANTI JOIN new_data USING ({})",
                        key_list
                    );

                    let final_df = ctx
                        .sql(&sql)
                        .await
                        .map_err(|e| StorageError::Other(e.into()))?;
                    let final_batches = final_df
                        .collect()
                        .await
                        .map_err(|e| StorageError::Other(e.into()))?;

                    DeltaOps(existing_table)
                        .write(final_batches)
                        .with_save_mode(SaveMode::Overwrite)
                        .await?;

                    return Ok(());
                }
                Err(e) => {
                    return Err(StorageError::from(e));
                }
            }
        }

        DeltaOps::try_from_uri(table_uri)
            .await?
            .write(batches)
            .await?;

        Ok(())
    }

    /// 写入边数据到数据湖
    ///
    /// # 参数
    /// * `edge_type` - 边类型名称（如 "HAS_VERSION", "CALLS" 等）
    /// * `edges` - 边数据向量，必须实现 Fetchable trait
    ///
    /// # 返回
    /// * `Result<()>` - 操作结果
    pub async fn write_edges<T: crate::fetch::Fetchable>(
        &self,
        edge_type: &str,
        edges: Vec<T>,
    ) -> Result<()> {
        let batch = T::to_record_batch(edges)?;
        let table_path = format!("silver/edges/{}", edge_type.to_lowercase());
        let merge_keys: Vec<String> = T::primary_keys()
            .into_iter()
            .map(|k| k.to_string())
            .collect();
        let merge_on = if merge_keys.is_empty() {
            None
        } else {
            Some(merge_keys)
        };
        self.write_batches(&table_path, vec![batch], merge_on).await
    }

    // Note: These methods are left for API compatibility but should ideally query the hot path (HelixDB).
    // The current implementation is a placeholder.
    pub async fn get_out_edges(
        &self,
        node_id: &str,
        edge_type: Option<&str>,
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        self.get_adjacent_edges(node_id, edge_type, Direction::Out)
            .await
    }

    pub async fn get_in_edges(
        &self,
        node_id: &str,
        edge_type: Option<&str>,
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        self.get_adjacent_edges(node_id, edge_type, Direction::In)
            .await
    }

    pub async fn read_changes_since(
        &self,
        table_name: &str,
        start_version: i64,
    ) -> Result<(Vec<(i64, Vec<RecordBatch>)>, i64)> {
        let table_path = self.config.lake_path.join(table_name);
        let table_uri = self.path_to_url(&table_path)?;

        let mut table = DeltaTableBuilder::from_uri(table_uri)?.build()?;
        table.load().await?;
        let latest_version = table.version().unwrap_or(-1);

        if latest_version <= start_version {
            return Ok((Vec::new(), latest_version));
        }

        let log_store = table.log_store();

        let mut changes = Vec::new();

        for version in (start_version + 1)..=latest_version {
            if let Some(bytes) = log_store.read_commit_entry(version).await? {
                let mut version_batches = Vec::new();
                for line in bytes.split(|b| *b == b'\n') {
                    if line.is_empty() {
                        continue;
                    }
                    let action: Action =
                        serde_json::from_slice(line).map_err(|e| StorageError::Other(e.into()))?;
                    if let Action::Add(add) = action {
                        let file_path = add.path;
                        let batches =
                            read_parquet_batches(table.object_store(), &file_path).await?;
                        version_batches.extend(batches);
                    }
                }
                if !version_batches.is_empty() {
                    changes.push((version, version_batches));
                }
            }
        }

        Ok((changes, latest_version))
    }
}

#[derive(Clone, Copy)]
enum Direction {
    Out,
    In,
}

impl Lake {
    fn helix_value_to_json(value: &HelixValue) -> JsonValue {
        match value {
            HelixValue::String(s) => JsonValue::String(s.clone()),
            HelixValue::F32(f) => serde_json::Number::from_f64(f64::from(*f))
                .map(JsonValue::Number)
                .unwrap_or_else(|| JsonValue::String(f.to_string())),
            HelixValue::F64(f) => serde_json::Number::from_f64(*f)
                .map(JsonValue::Number)
                .unwrap_or_else(|| JsonValue::String(f.to_string())),
            HelixValue::I8(v) => JsonValue::Number((*v).into()),
            HelixValue::I16(v) => JsonValue::Number((*v).into()),
            HelixValue::I32(v) => JsonValue::Number((*v).into()),
            HelixValue::I64(v) => JsonValue::Number((*v).into()),
            HelixValue::U8(v) => JsonValue::Number((*v).into()),
            HelixValue::U16(v) => JsonValue::Number((*v).into()),
            HelixValue::U32(v) => JsonValue::Number((*v).into()),
            HelixValue::U64(v) => JsonValue::Number((*v).into()),
            HelixValue::U128(v) => JsonValue::String(v.to_string()),
            HelixValue::Date(d) => JsonValue::String(d.to_string()),
            HelixValue::Boolean(b) => JsonValue::Bool(*b),
            HelixValue::Id(id) => JsonValue::String(id.stringify()),
            HelixValue::Array(values) => {
                JsonValue::Array(values.iter().map(Self::helix_value_to_json).collect())
            }
            HelixValue::Object(map) => {
                let mut json_map = JsonMap::new();
                for (k, v) in map {
                    json_map.insert(k.clone(), Self::helix_value_to_json(v));
                }
                JsonValue::Object(json_map)
            }
            HelixValue::Empty => JsonValue::Null,
        }
    }

    fn edge_to_map(edge: Edge) -> HashMap<String, JsonValue> {
        let mut result = HashMap::new();
        result.insert(
            "id".to_string(),
            JsonValue::String(Uuid::from_u128(edge.id).to_string()),
        );
        result.insert("label".to_string(), JsonValue::String(edge.label.clone()));
        result.insert(
            "from_node_id".to_string(),
            JsonValue::String(Uuid::from_u128(edge.from_node).to_string()),
        );
        result.insert(
            "to_node_id".to_string(),
            JsonValue::String(Uuid::from_u128(edge.to_node).to_string()),
        );

        match edge.properties {
            Some(props) if !props.is_empty() => {
                let mut json_map = JsonMap::new();
                for (key, value) in props {
                    json_map.insert(key, Self::helix_value_to_json(&value));
                }
                result.insert("properties".to_string(), JsonValue::Object(json_map));
            }
            _ => {
                result.insert("properties".to_string(), JsonValue::Null);
            }
        }

        result
    }

    fn node_to_map(node: Node) -> HashMap<String, JsonValue> {
        let mut result = HashMap::new();
        result.insert(
            "id".to_string(),
            JsonValue::String(Uuid::from_u128(node.id).to_string()),
        );
        result.insert("label".to_string(), JsonValue::String(node.label.clone()));
        let properties = if let Some(props) = node.properties {
            let mut json_map = serde_json::Map::new();
            for (k, v) in props {
                json_map.insert(k, Self::helix_value_to_json(&v));
            }
            JsonValue::Object(json_map)
        } else {
            JsonValue::Null
        };
        result.insert("properties".to_string(), properties);
        result
    }

    fn vector_to_map(vector: HVector) -> HashMap<String, JsonValue> {
        let mut result = HashMap::new();
        result.insert(
            "id".to_string(),
            JsonValue::String(Uuid::from_u128(vector.id).to_string()),
        );
        if let Some(distance) = vector.distance {
            if let Some(number) = JsonNumber::from_f64(distance) {
                result.insert("distance".to_string(), JsonValue::Number(number));
            }
            let similarity = 1.0 / (1.0 + distance);
            if let Some(number) = JsonNumber::from_f64(similarity) {
                result.insert("similarity".to_string(), JsonValue::Number(number));
            }
        } else {
            result.insert("distance".to_string(), JsonValue::Null);
            result.insert("similarity".to_string(), JsonValue::Null);
        }
        if let Some(props) = vector.properties.clone() {
            let mut json_map = JsonMap::new();
            for (k, v) in props {
                json_map.insert(k, Self::helix_value_to_json(&v));
            }
            result.insert("properties".to_string(), JsonValue::Object(json_map));
        } else {
            result.insert("properties".to_string(), JsonValue::Null);
        }
        result
    }

    fn vector_to_node_map(vector: &HVector) -> HashMap<String, JsonValue> {
        let mut result = HashMap::new();
        result.insert(
            "id".to_string(),
            JsonValue::String(Uuid::from_u128(vector.id).to_string()),
        );

        let label = vector
            .get_label()
            .map(|value| value.inner_stringify())
            .unwrap_or_else(|| "VECTOR".to_string());
        result.insert("label".to_string(), JsonValue::String(label));

        if let Some(props) = vector.properties.clone() {
            let mut json_map = JsonMap::new();
            for (key, value) in props {
                json_map.insert(key, Self::helix_value_to_json(&value));
            }
            result.insert("properties".to_string(), JsonValue::Object(json_map));
        } else {
            result.insert("properties".to_string(), JsonValue::Null);
        }

        result
    }

    fn record_batch_row_to_map(
        batch: &RecordBatch,
        row: usize,
    ) -> Result<HashMap<String, JsonValue>> {
        let schema = batch.schema();
        let mut map = HashMap::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let value = if column.is_null(row) {
                JsonValue::Null
            } else {
                Self::arrow_cell_to_json(column, row).unwrap_or(JsonValue::Null)
            };
            map.insert(field.name().clone(), value);
        }
        Ok(map)
    }

    fn json_value_to_string(value: &JsonValue) -> Option<String> {
        match value {
            JsonValue::Null => None,
            JsonValue::String(s) => Some(s.clone()),
            JsonValue::Number(n) => Some(n.to_string()),
            JsonValue::Bool(b) => Some(b.to_string()),
            other => Some(other.to_string()),
        }
    }

    fn row_matches_primary_keys(
        batch: &RecordBatch,
        row: usize,
        primary_keys: &[(String, Option<String>)],
    ) -> Result<bool> {
        let schema = batch.schema();
        for (key, expected) in primary_keys {
            let idx = match schema.index_of(key) {
                Ok(idx) => idx,
                Err(_) => return Ok(false),
            };
            let column = batch.column(idx);
            if let Some(expected_value) = expected {
                if column.is_null(row) {
                    return Ok(false);
                }
                let actual_json = match Self::arrow_cell_to_json(column, row) {
                    Some(value) => value,
                    None => return Ok(false),
                };
                let actual = match Self::json_value_to_string(&actual_json) {
                    Some(value) => value,
                    None => return Ok(false),
                };
                if actual != *expected_value {
                    return Ok(false);
                }
            } else if !column.is_null(row) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn lookup_node_in_index(
        &self,
        entity_type: &str,
        node_id: &str,
    ) -> Result<Option<HashMap<String, JsonValue>>> {
        let index_path = self
            .config
            .lake_path
            .join(format!("silver/index/{}", entity_type));
        if tokio::fs::metadata(&index_path).await.is_err() {
            return Ok(None);
        }

        let table_uri = match self.path_to_url(&index_path) {
            Ok(uri) => uri,
            Err(_) => return Ok(None),
        };

        let index_table = match deltalake::open_table(table_uri).await {
            Ok(table) => table,
            Err(deltalake::DeltaTableError::NotATable(_)) => return Ok(None),
            Err(e) => return Err(StorageError::from(e)),
        };

        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let alias = format!("index_{}", entity_type.replace('-', "_"));
        ctx.register_table(&alias, Arc::new(index_table))
            .map_err(|e| StorageError::Other(e.into()))?;

        let escaped_id = node_id.replace('\'', "''");
        let sql = format!(
            "SELECT * FROM {alias} WHERE id = '{escaped}' LIMIT 1",
            alias = alias,
            escaped = escaped_id
        );
        let index_batches = ctx
            .sql(&sql)
            .await
            .map_err(|e| StorageError::Other(e.into()))?
            .collect()
            .await
            .map_err(|e| StorageError::Other(e.into()))?;

        if index_batches.is_empty() || index_batches[0].num_rows() == 0 {
            return Ok(None);
        }

        let index_batch = &index_batches[0];
        let schema = index_batch.schema();
        let mut pk_values: Vec<(String, Option<String>)> = Vec::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let name = field.name();
            if name == "id" || name == "updated_at" {
                continue;
            }
            let column = index_batch.column(col_idx);
            if column.is_null(0) {
                pk_values.push((name.clone(), None));
            } else if let Some(value) = Self::arrow_cell_to_json(column, 0) {
                pk_values.push((name.clone(), Self::json_value_to_string(&value)));
            } else {
                pk_values.push((name.clone(), None));
            }
        }

        let entity_path = self
            .config
            .lake_path
            .join(format!("silver/entities/{}", entity_type));
        if tokio::fs::metadata(&entity_path).await.is_err() {
            return Ok(None);
        }

        let entity_uri = match self.path_to_url(&entity_path) {
            Ok(uri) => uri,
            Err(_) => return Ok(None),
        };
        let entity_table = match deltalake::open_table(entity_uri).await {
            Ok(table) => table,
            Err(deltalake::DeltaTableError::NotATable(_)) => return Ok(None),
            Err(e) => return Err(StorageError::from(e)),
        };

        let entity_ctx =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let entity_alias = format!("entity_{}", entity_type.replace('-', "_"));
        entity_ctx
            .register_table(&entity_alias, Arc::new(entity_table))
            .map_err(|e| StorageError::Other(e.into()))?;
        let entity_batches = entity_ctx
            .sql(&format!("SELECT * FROM {}", entity_alias))
            .await
            .map_err(|e| StorageError::Other(e.into()))?
            .collect()
            .await
            .map_err(|e| StorageError::Other(e.into()))?;

        for batch in entity_batches {
            for row in 0..batch.num_rows() {
                if Self::row_matches_primary_keys(&batch, row, &pk_values)? {
                    let mut map = Self::record_batch_row_to_map(&batch, row)?;
                    map.insert("id".to_string(), JsonValue::String(node_id.to_string()));
                    return Ok(Some(map));
                }
            }
        }

        Ok(None)
    }

    async fn get_available_index_entity_types(&self) -> Result<Vec<String>> {
        let index_path = self.config.lake_path.join("silver/index");
        let mut types = Vec::new();

        if tokio::fs::metadata(&index_path).await.is_ok() {
            let mut entries = tokio::fs::read_dir(&index_path).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if path.is_dir() {
                    if let Some(dir_name) = path.file_name().and_then(|name| name.to_str()) {
                        types.push(dir_name.to_string());
                    }
                }
            }
        }

        Ok(types)
    }

    pub async fn get_node_by_id(
        &self,
        id: &str,
        entity_type_hint: Option<&str>,
    ) -> Result<Option<HashMap<String, JsonValue>>> {
        if let Ok(uuid) = Uuid::parse_str(id) {
            let node_key = uuid.as_u128();
            let txn = self.engine.storage.graph_env.read_txn()?;
            if let Ok(node) = self.engine.storage.get_node(&txn, &node_key) {
                return Ok(Some(Self::node_to_map(node)));
            }

            match self
                .engine
                .storage
                .vectors
                .get_vector(&txn, node_key, 0, true)
            {
                Ok(vector) => return Ok(Some(Self::vector_to_node_map(&vector))),
                Err(VectorError::VectorNotFound(_)) | Err(VectorError::EntryPointNotFound) => {}
                Err(err) => return Err(StorageError::Graph(err.into())),
            }
        }

        let candidate_types = if let Some(hint) = entity_type_hint {
            vec![hint.to_string()]
        } else {
            self.get_available_index_entity_types().await?
        };

        for entity_type in candidate_types {
            if let Some(node) = self.lookup_node_in_index(&entity_type, id).await? {
                return Ok(Some(node));
            }
        }

        Ok(None)
    }

    pub async fn get_node_by_keys(
        &self,
        entity_type: &str,
        primary_keys: &[(&str, &str)],
    ) -> Result<Option<HashMap<String, JsonValue>>> {
        if primary_keys.is_empty() {
            return Err(StorageError::InvalidArg(
                "primary_keys must not be empty for get_node_by_keys".into(),
            ));
        }

        let key_values: Vec<(&str, String)> = primary_keys
            .iter()
            .map(|(key, value)| (*key, (*value).to_string()))
            .collect();
        let id_u128 = utils::id::stable_node_id_u128(entity_type, &key_values);
        let id_string = Uuid::from_u128(id_u128).to_string();

        if let Some(node) = self.get_node_by_id(&id_string, Some(entity_type)).await? {
            return Ok(Some(node));
        }

        self.lookup_node_in_table_by_keys(entity_type, primary_keys, &id_string)
            .await
    }

    pub async fn load_vector_index_map(
        &self,
        index_table: &str,
        id_column: &str,
        ids: &[String],
    ) -> Result<HashMap<String, String>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let table_path = self.config.lake_path.join(index_table);
        if tokio::fs::metadata(&table_path).await.is_err() {
            return Ok(HashMap::new());
        }

        let table_uri = self.path_to_url(&table_path)?;
        let table = match deltalake::open_table(table_uri).await {
            Ok(table) => table,
            Err(deltalake::DeltaTableError::NotATable(_)) => return Ok(HashMap::new()),
            Err(err) => return Err(StorageError::from(err)),
        };

        let ctx = Self::single_partition_session();
        ctx.register_table("vector_index", Arc::new(table))
            .map_err(|e| StorageError::Other(e.into()))?;

        let escaped_ids: Vec<String> = ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "''")))
            .collect();
        if escaped_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let sql = format!(
            "SELECT \"{id_col}\" as id_value, vector_uuid FROM vector_index WHERE \"{id_col}\" IN ({values})",
            id_col = id_column,
            values = escaped_ids.join(", ")
        );
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| StorageError::Other(e.into()))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| StorageError::Other(e.into()))?;

        let mut map = HashMap::new();
        for batch in batches {
            if batch.num_columns() < 2 {
                continue;
            }
            let id_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    StorageError::Other(anyhow!(
                        "Vector index '{}' id column '{}' is not Utf8",
                        index_table,
                        id_column
                    ))
                })?;
            let uuid_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    StorageError::Other(anyhow!(
                        "Vector index '{}' missing Utf8 vector_uuid column",
                        index_table
                    ))
                })?;

            for row in 0..batch.num_rows() {
                if id_array.is_null(row) || uuid_array.is_null(row) {
                    continue;
                }
                map.insert(
                    id_array.value(row).to_string(),
                    uuid_array.value(row).to_string(),
                );
            }
        }

        Ok(map)
    }

    pub async fn neighbors(
        &self,
        node_id: &str,
        edge_types: Option<&[&str]>,
        direction: NeighborDirection,
        limit: usize,
    ) -> Result<Vec<NeighborRecord>> {
        let edge_filters = edge_types.map(|types| {
            types
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<String>>()
        });
        let cap = if limit == 0 { usize::MAX } else { limit };

        let mut collected: Vec<(HashMap<String, JsonValue>, NeighborEdgeOrientation)> = Vec::new();

        match direction {
            NeighborDirection::Outgoing => {
                let edges = self
                    .collect_adjacent_edges(node_id, edge_filters.as_deref(), Direction::Out)
                    .await?;
                Self::push_edges_with_cap(
                    &mut collected,
                    edges,
                    NeighborEdgeOrientation::Outgoing,
                    cap,
                );
            }
            NeighborDirection::Incoming => {
                let edges = self
                    .collect_adjacent_edges(node_id, edge_filters.as_deref(), Direction::In)
                    .await?;
                Self::push_edges_with_cap(
                    &mut collected,
                    edges,
                    NeighborEdgeOrientation::Incoming,
                    cap,
                );
            }
            NeighborDirection::Both => {
                let outgoing = self
                    .collect_adjacent_edges(node_id, edge_filters.as_deref(), Direction::Out)
                    .await?;
                Self::push_edges_with_cap(
                    &mut collected,
                    outgoing,
                    NeighborEdgeOrientation::Outgoing,
                    cap,
                );
                if collected.len() < cap {
                    let incoming = self
                        .collect_adjacent_edges(node_id, edge_filters.as_deref(), Direction::In)
                        .await?;
                    Self::push_edges_with_cap(
                        &mut collected,
                        incoming,
                        NeighborEdgeOrientation::Incoming,
                        cap,
                    );
                }
            }
        }

        let mut results = Vec::new();
        for (edge_map, orientation) in collected.into_iter() {
            if results.len() >= cap {
                break;
            }

            let neighbor_value = match orientation {
                NeighborEdgeOrientation::Outgoing => edge_map.get("to_node_id"),
                NeighborEdgeOrientation::Incoming => edge_map.get("from_node_id"),
            };

            let neighbor_id = match neighbor_value.and_then(|value| value.as_str()) {
                Some(id) if !id.is_empty() => id.to_string(),
                _ => continue,
            };

            let entity_type_hint = edge_map
                .get("properties")
                .and_then(|value| value.as_object())
                .and_then(|props| match orientation {
                    NeighborEdgeOrientation::Outgoing => props.get("to_node_type"),
                    NeighborEdgeOrientation::Incoming => props.get("from_node_type"),
                })
                .and_then(|value| value.as_str())
                .map(|s| s.to_string());

            let node = self
                .get_node_by_id(&neighbor_id, entity_type_hint.as_deref())
                .await?;

            results.push(NeighborRecord {
                orientation,
                edge: edge_map,
                node_id: neighbor_id,
                node,
            });
        }

        Ok(results)
    }

    pub async fn subgraph_bfs(
        &self,
        start_id: &str,
        edge_types: Option<&[&str]>,
        depth: usize,
        node_limit: usize,
        edge_limit: usize,
    ) -> Result<Subgraph> {
        let start_uuid = Uuid::parse_str(start_id)
            .map_err(|_| StorageError::InvalidArg(format!("Invalid node id '{}'", start_id)))?;

        let node_cap = if node_limit == 0 {
            usize::MAX
        } else {
            node_limit
        };
        let edge_cap = if edge_limit == 0 {
            usize::MAX
        } else {
            edge_limit
        };

        let mut queue: VecDeque<(u128, usize)> = VecDeque::new();
        queue.push_back((start_uuid.as_u128(), 0));

        let mut visited_nodes: HashSet<u128> = HashSet::new();
        let mut seen_edges: HashSet<u128> = HashSet::new();
        let mut known_vector_nodes: HashSet<u128> = HashSet::new();
        let mut missing_vector_nodes: HashSet<u128> = HashSet::new();
        let mut included_nodes: HashSet<u128> = HashSet::new();
        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        let allowed_edge_types = edge_types.map(|types| {
            types
                .iter()
                .map(|value| value.to_string())
                .collect::<HashSet<String>>()
        });

        let txn = self.engine.storage.graph_env.read_txn()?;

        while let Some((node_key, level)) = queue.pop_front() {
            if visited_nodes.contains(&node_key) {
                continue;
            }

            let node_map = match self.engine.storage.get_node(&txn, &node_key) {
                Ok(node) => Self::node_to_map(node),
                Err(GraphError::NodeNotFound) => {
                    if missing_vector_nodes.contains(&node_key) {
                        visited_nodes.insert(node_key);
                        continue;
                    }

                    match self
                        .engine
                        .storage
                        .vectors
                        .get_vector(&txn, node_key, 0, true)
                    {
                        Ok(vector) => {
                            known_vector_nodes.insert(node_key);
                            Self::vector_to_node_map(&vector)
                        }
                        Err(VectorError::VectorNotFound(_))
                        | Err(VectorError::EntryPointNotFound) => {
                            missing_vector_nodes.insert(node_key);
                            visited_nodes.insert(node_key);
                            continue;
                        }
                        Err(err) => return Err(StorageError::Graph(err.into())),
                    }
                }
                Err(other) => return Err(StorageError::from(other)),
            };

            if included_nodes.insert(node_key) {
                nodes.push(node_map.clone());
            }
            visited_nodes.insert(node_key);

            if node_cap != usize::MAX && nodes.len() >= node_cap {
                return Ok(Subgraph { nodes, edges });
            }

            if level >= depth {
                continue;
            }

            let prefix = node_key.to_be_bytes();
            let iter = self
                .engine
                .storage
                .out_edges_db
                .prefix_iter(&txn, &prefix)?;

            for entry in iter {
                if edge_cap != usize::MAX && edges.len() >= edge_cap {
                    break;
                }

                let (_raw_key, raw_value) = entry?;
                let (edge_id, next_node_id) =
                    HelixGraphStorage::unpack_adj_edge_data(raw_value.as_ref())?;
                if seen_edges.contains(&edge_id) {
                    continue;
                }

                let edge = match self.engine.storage.get_edge(&txn, &edge_id) {
                    Ok(edge) => edge,
                    Err(GraphError::EdgeNotFound) => continue,
                    Err(other) => return Err(StorageError::from(other)),
                };

                if let Some(ref allowed) = allowed_edge_types {
                    if !allowed.contains(&edge.label) {
                        continue;
                    }
                }

                let mut neighbor_map: Option<HashMap<String, JsonValue>> = None;
                if !included_nodes.contains(&next_node_id) {
                    neighbor_map = self.load_node_map_for_id(
                        &txn,
                        next_node_id,
                        &mut known_vector_nodes,
                        &mut missing_vector_nodes,
                    )?;
                    if neighbor_map.is_none() {
                        continue;
                    }
                } else if missing_vector_nodes.contains(&next_node_id) {
                    continue;
                }

                edges.push(Self::edge_to_map(edge));
                seen_edges.insert(edge_id);

                if let Some(map) = neighbor_map {
                    if included_nodes.insert(next_node_id) {
                        nodes.push(map);
                    }
                    if node_cap != usize::MAX && nodes.len() >= node_cap {
                        return Ok(Subgraph { nodes, edges });
                    }
                }

                if !visited_nodes.contains(&next_node_id) && level + 1 <= depth {
                    queue.push_back((next_node_id, level + 1));
                }

                if edge_cap != usize::MAX && edges.len() >= edge_cap {
                    break;
                }
            }

            if edge_cap != usize::MAX && edges.len() >= edge_cap {
                break;
            }
        }

        Ok(Subgraph { nodes, edges })
    }

    fn load_node_map_for_id(
        &self,
        txn: &RoTxn,
        node_id: u128,
        known_vector_nodes: &mut HashSet<u128>,
        missing_vector_nodes: &mut HashSet<u128>,
    ) -> Result<Option<HashMap<String, JsonValue>>> {
        match self.engine.storage.get_node(txn, &node_id) {
            Ok(node) => Ok(Some(Self::node_to_map(node))),
            Err(GraphError::NodeNotFound) => {
                if missing_vector_nodes.contains(&node_id) {
                    return Ok(None);
                }
                match self
                    .engine
                    .storage
                    .vectors
                    .get_vector(txn, node_id, 0, true)
                {
                    Ok(vector) => {
                        known_vector_nodes.insert(node_id);
                        Ok(Some(Self::vector_to_node_map(&vector)))
                    }
                    Err(VectorError::VectorNotFound(_)) | Err(VectorError::EntryPointNotFound) => {
                        missing_vector_nodes.insert(node_id);
                        Ok(None)
                    }
                    Err(err) => Err(StorageError::Graph(err.into())),
                }
            }
            Err(err) => Err(StorageError::from(err)),
        }
    }

    pub async fn query_table(
        &self,
        table_name: &str,
        filters: Option<&[(&str, &str)]>,
        limit: Option<usize>,
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        let Some(table) = self.open_delta_table(table_name).await? else {
            return Ok(Vec::new());
        };

        let ctx = Self::single_partition_session();
        let alias = Self::sanitize_table_alias(table_name);
        ctx.register_table(&alias, Arc::new(table))
            .map_err(|e| StorageError::Other(e.into()))?;

        let mut clauses = Vec::new();
        if let Some(filters) = filters {
            for (column, value) in filters {
                let escaped_column = Self::escape_sql_identifier(column);
                let escaped_value = Self::escape_sql_literal(value);
                clauses.push(format!("{escaped_column} = '{escaped_value}'"));
            }
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let limit_clause = limit
            .map(|value| format!(" LIMIT {}", value))
            .unwrap_or_default();
        let sql = format!("SELECT * FROM {alias}{where_clause}{limit_clause}");

        let batches = ctx
            .sql(&sql)
            .await
            .map_err(|e| StorageError::Other(e.into()))?
            .collect()
            .await
            .map_err(|e| StorageError::Other(e.into()))?;

        Self::record_batches_to_maps(&batches)
    }

    pub async fn search_index_nodes(
        &self,
        entity_type: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }

        let table_name = format!("silver/index/{entity_type}");
        let Some(table) = self.open_delta_table(&table_name).await? else {
            return Ok(Vec::new());
        };

        let schema = table.schema();
        let mut clauses = Vec::new();
        let mut has_updated_at = false;
        let lowered_query = trimmed.to_lowercase();
        let text_pattern = format!("%{}%", lowered_query);
        let escaped_text_pattern = Self::escape_sql_literal(&text_pattern);
        let id_pattern = format!("%{}%", lowered_query);
        let escaped_id_pattern = Self::escape_sql_literal(&id_pattern);

        for field in schema.fields() {
            match field.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    let identifier = Self::escape_sql_identifier(field.name());
                    if field.name() == "id" {
                        clauses.push(format!("LOWER({identifier}) LIKE '{escaped_id_pattern}'"));
                    } else {
                        clauses.push(format!("LOWER({identifier}) LIKE '{escaped_text_pattern}'"));
                    }
                }
                DataType::Timestamp(_, _) => {
                    if field.name() == "updated_at" {
                        has_updated_at = true;
                    }
                }
                _ => {}
            }
        }

        if clauses.is_empty() {
            return Ok(Vec::new());
        }

        let alias = Self::sanitize_table_alias(&table_name);
        let ctx = Self::single_partition_session();
        ctx.register_table(&alias, Arc::new(table))
            .map_err(|e| StorageError::Other(e.into()))?;

        let where_clause = clauses.join(" OR ");
        let order_clause = if has_updated_at {
            " ORDER BY updated_at DESC"
        } else {
            ""
        };
        let sql = format!(
            "SELECT * FROM {alias} WHERE {where_clause}{order_clause} LIMIT {limit}",
            alias = alias,
            where_clause = where_clause,
            order_clause = order_clause,
            limit = limit
        );

        let batches = ctx
            .sql(&sql)
            .await
            .map_err(|e| StorageError::Other(e.into()))?
            .collect()
            .await
            .map_err(|e| StorageError::Other(e.into()))?;

        Self::record_batches_to_maps(&batches)
    }

    pub async fn table_sql(
        &self,
        table_name: &str,
        sql: &str,
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        let Some(table) = self.open_delta_table(table_name).await? else {
            return Ok(Vec::new());
        };

        let ctx = Self::single_partition_session();
        let alias = Self::sanitize_table_alias(table_name);
        ctx.register_table(&alias, Arc::new(table))
            .map_err(|e| StorageError::Other(e.into()))?;

        let final_sql = if sql.contains("{{table}}") {
            sql.replace("{{table}}", &alias)
        } else {
            sql.to_string()
        };

        let batches = ctx
            .sql(&final_sql)
            .await
            .map_err(|e| StorageError::Other(e.into()))?
            .collect()
            .await
            .map_err(|e| StorageError::Other(e.into()))?;

        Self::record_batches_to_maps(&batches)
    }

    async fn get_adjacent_edges(
        &self,
        node_id: &str,
        edge_type: Option<&str>,
        direction: Direction,
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        if let Ok(node_uuid) = Uuid::parse_str(node_id) {
            match self
                .get_adjacent_edges_from_helix(node_uuid.as_u128(), edge_type, direction)
                .await
            {
                Ok(edges) if !edges.is_empty() => return Ok(edges),
                Ok(_) => { /* Fall back to lake */ }
                Err(StorageError::InvalidArg(_)) | Err(StorageError::NotFound(_)) => {
                    // Node missing in Helix, fall back to lake
                }
                Err(e) => return Err(e),
            }
        }

        self.get_adjacent_edges_from_lake(node_id, edge_type, direction)
            .await
    }

    async fn get_adjacent_edges_from_helix(
        &self,
        node_key: u128,
        edge_type: Option<&str>,
        direction: Direction,
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        let label_filter = edge_type.map(str::to_string);
        let txn = self.engine.storage.graph_env.read_txn()?;

        match self.engine.storage.get_node(&txn, &node_key) {
            Ok(_) => {}
            Err(GraphError::NodeNotFound) => {
                match self
                    .engine
                    .storage
                    .vectors
                    .get_vector(&txn, node_key, 0, true)
                {
                    Ok(_) => {}
                    Err(VectorError::VectorNotFound(_)) | Err(VectorError::EntryPointNotFound) => {
                        return Err(StorageError::NotFound(format!(
                            "Node '{}' was not found in Helix storage",
                            Uuid::from_u128(node_key)
                        )));
                    }
                    Err(err) => return Err(StorageError::Graph(err.into())),
                }
            }
            Err(other) => return Err(StorageError::from(other)),
        }

        let prefix = &node_key.to_be_bytes();
        let iter = match direction {
            Direction::Out => self.engine.storage.out_edges_db.prefix_iter(&txn, prefix)?,
            Direction::In => self.engine.storage.in_edges_db.prefix_iter(&txn, prefix)?,
        };

        let mut edges = Vec::new();
        for entry in iter {
            let (_key, value) = entry?;
            let (edge_id, other_node_id) = HelixGraphStorage::unpack_adj_edge_data(value.as_ref())?;
            let edge = self.engine.storage.get_edge(&txn, &edge_id)?;

            let matches_direction = match direction {
                Direction::Out => edge.from_node == node_key && edge.to_node == other_node_id,
                Direction::In => edge.to_node == node_key && edge.from_node == other_node_id,
            };
            if !matches_direction {
                continue;
            }

            if let Some(expected_label) = &label_filter {
                if &edge.label != expected_label {
                    continue;
                }
            }

            edges.push(Self::edge_to_map(edge));
        }

        Ok(edges)
    }

    async fn get_adjacent_edges_from_lake(
        &self,
        node_id: &str,
        edge_type: Option<&str>,
        direction: Direction,
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        let edge_types = if let Some(et) = edge_type {
            vec![et.to_string()]
        } else {
            self.get_available_edge_types().await?
        };

        let mut results = Vec::new();
        for et in edge_types {
            let table_path = self.config.lake_path.join(format!("silver/edges/{}", et));
            if tokio::fs::metadata(&table_path).await.is_err() {
                continue;
            }

            let table_uri = match self.path_to_url(&table_path) {
                Ok(uri) => uri,
                Err(_) => continue,
            };

            let table = match deltalake::open_table(table_uri).await {
                Ok(table) => table,
                Err(deltalake::DeltaTableError::NotATable(_)) => continue,
                Err(e) => return Err(StorageError::from(e)),
            };

            let ctx = Self::single_partition_session();
            let alias = format!("edges_{}", et.replace('-', "_"));
            ctx.register_table(&alias, Arc::new(table))
                .map_err(|e| StorageError::Other(e.into()))?;

            let filter_column = match direction {
                Direction::Out => "from_node_id",
                Direction::In => "to_node_id",
            };

            let escaped_node_id = node_id.replace('\'', "''");
            let sql = format!(
                "SELECT * FROM {alias} WHERE {filter_column} = '{escaped_node_id}'",
                alias = alias,
                filter_column = filter_column,
                escaped_node_id = escaped_node_id
            );

            let batches = ctx
                .sql(&sql)
                .await
                .map_err(|e| StorageError::Other(e.into()))?
                .collect()
                .await
                .map_err(|e| StorageError::Other(e.into()))?;

            if batches.is_empty() {
                continue;
            }

            let label = edge_type
                .map(|s| s.to_string())
                .unwrap_or_else(|| et.clone())
                .to_uppercase();
            let mut mapped = Self::record_batches_to_edge_maps(&batches, &label)?;
            results.append(&mut mapped);
        }

        Ok(results)
    }

    async fn collect_adjacent_edges(
        &self,
        node_id: &str,
        edge_types: Option<&[String]>,
        direction: Direction,
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        if let Some(types) = edge_types {
            let mut edges = Vec::new();
            for edge_type in types {
                let mut batch = self
                    .get_adjacent_edges(node_id, Some(edge_type.as_str()), direction)
                    .await?;
                edges.append(&mut batch);
            }
            Ok(edges)
        } else {
            self.get_adjacent_edges(node_id, None, direction).await
        }
    }

    fn push_edges_with_cap(
        target: &mut Vec<(HashMap<String, JsonValue>, NeighborEdgeOrientation)>,
        edges: Vec<HashMap<String, JsonValue>>,
        orientation: NeighborEdgeOrientation,
        cap: usize,
    ) {
        for edge in edges {
            if target.len() >= cap {
                break;
            }
            target.push((edge, orientation));
        }
    }

    async fn open_delta_table(&self, table_name: &str) -> Result<Option<DeltaTable>> {
        let table_path = self.config.lake_path.join(table_name);
        if tokio::fs::metadata(&table_path).await.is_err() {
            return Ok(None);
        }
        let table_uri = match self.path_to_url(&table_path) {
            Ok(uri) => uri,
            Err(_) => return Ok(None),
        };

        match deltalake::open_table(table_uri).await {
            Ok(table) => Ok(Some(table)),
            Err(deltalake::DeltaTableError::NotATable(_)) => Ok(None),
            Err(e) => Err(StorageError::from(e)),
        }
    }

    async fn lookup_node_in_table_by_keys(
        &self,
        entity_type: &str,
        primary_keys: &[(&str, &str)],
        computed_id: &str,
    ) -> Result<Option<HashMap<String, JsonValue>>> {
        let table_name = format!("silver/entities/{}", entity_type);
        let Some(table) = self.open_delta_table(&table_name).await? else {
            return Ok(None);
        };

        let ctx = Self::single_partition_session();
        let alias = Self::sanitize_table_alias(&table_name);
        ctx.register_table(&alias, Arc::new(table))
            .map_err(|e| StorageError::Other(e.into()))?;

        let mut predicates = Vec::new();
        for (column, value) in primary_keys {
            let escaped_column = Self::escape_sql_identifier(column);
            let escaped_value = Self::escape_sql_literal(value);
            predicates.push(format!("{escaped_column} = '{escaped_value}'"));
        }
        if predicates.is_empty() {
            return Ok(None);
        }
        let where_clause = predicates.join(" AND ");
        let sql = format!("SELECT * FROM {alias} WHERE {where_clause} LIMIT 1");

        let batches = ctx
            .sql(&sql)
            .await
            .map_err(|e| StorageError::Other(e.into()))?
            .collect()
            .await
            .map_err(|e| StorageError::Other(e.into()))?;

        if batches.is_empty() {
            return Ok(None);
        }

        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let mut map = Self::record_batch_row_to_map(batch, 0)?;
            map.entry("id".to_string())
                .or_insert_with(|| JsonValue::String(computed_id.to_string()));
            return Ok(Some(map));
        }

        Ok(None)
    }

    fn record_batches_to_edge_maps(
        batches: &[RecordBatch],
        label: &str,
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        let mut edges = Vec::new();
        for batch in batches {
            let schema = batch.schema();
            for row in 0..batch.num_rows() {
                let mut edge_map = HashMap::new();
                let mut properties = JsonMap::new();

                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let column = batch.column(col_idx);
                    let value = if column.is_null(row) {
                        JsonValue::Null
                    } else {
                        Self::arrow_cell_to_json(column, row).unwrap_or(JsonValue::Null)
                    };

                    match field.name().as_str() {
                        "id" | "from_node_id" | "to_node_id" => {
                            edge_map.insert(field.name().clone(), value);
                        }
                        _ => {
                            if value != JsonValue::Null {
                                properties.insert(field.name().clone(), value);
                            }
                        }
                    }
                }

                edge_map.insert("label".to_string(), JsonValue::String(label.to_string()));
                if properties.is_empty() {
                    edge_map.insert("properties".to_string(), JsonValue::Null);
                } else {
                    edge_map.insert("properties".to_string(), JsonValue::Object(properties));
                }
                edges.push(edge_map);
            }
        }
        Ok(edges)
    }

    fn record_batches_to_maps(batches: &[RecordBatch]) -> Result<Vec<HashMap<String, JsonValue>>> {
        let mut rows = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                rows.push(Self::record_batch_row_to_map(batch, row)?);
            }
        }
        Ok(rows)
    }

    fn sanitize_table_alias(table_name: &str) -> String {
        let candidate: String = table_name
            .chars()
            .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
            .collect();
        if candidate.is_empty() {
            "table".to_string()
        } else {
            candidate
        }
    }

    fn escape_sql_literal(value: &str) -> String {
        value.replace('\'', "''")
    }

    fn escape_sql_identifier(identifier: &str) -> String {
        let mut escaped = String::with_capacity(identifier.len() + 2);
        escaped.push('"');
        for ch in identifier.chars() {
            if ch == '"' {
                escaped.push('"');
            }
            escaped.push(ch);
        }
        escaped.push('"');
        escaped
    }

    fn arrow_cell_to_json(column: &ArrayRef, row_idx: usize) -> Option<JsonValue> {
        match column.data_type() {
            DataType::Utf8 => {
                let arr = column.as_any().downcast_ref::<StringArray>()?;
                Some(JsonValue::String(arr.value(row_idx).to_string()))
            }
            DataType::Int64 => {
                let arr = column.as_any().downcast_ref::<Int64Array>()?;
                Some(JsonValue::Number(arr.value(row_idx).into()))
            }
            DataType::Int32 => {
                let arr = column.as_any().downcast_ref::<Int32Array>()?;
                Some(JsonValue::Number(arr.value(row_idx).into()))
            }
            DataType::UInt64 => {
                let arr = column.as_any().downcast_ref::<UInt64Array>()?;
                Some(JsonValue::Number(arr.value(row_idx).into()))
            }
            DataType::UInt32 => {
                let arr = column.as_any().downcast_ref::<UInt32Array>()?;
                Some(JsonValue::Number(arr.value(row_idx).into()))
            }
            DataType::Float64 => {
                let arr = column.as_any().downcast_ref::<Float64Array>()?;
                serde_json::Number::from_f64(arr.value(row_idx)).map(JsonValue::Number)
            }
            DataType::Float32 => {
                let arr = column.as_any().downcast_ref::<Float32Array>()?;
                serde_json::Number::from_f64(arr.value(row_idx) as f64).map(JsonValue::Number)
            }
            DataType::Boolean => {
                let arr = column.as_any().downcast_ref::<BooleanArray>()?;
                Some(JsonValue::Bool(arr.value(row_idx)))
            }
            DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, _) => {
                let arr = column
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()?;
                let micros = arr.value(row_idx);
                DateTime::<Utc>::from_timestamp_micros(micros)
                    .map(|dt| JsonValue::String(dt.to_rfc3339()))
            }
            _ => None,
        }
    }
}

impl Lake {
    /// 获取边数据统计信息
    ///
    /// # 返回
    /// * `Result<HashMap<String, i64>>` - 边类型到数量的映射
    pub async fn get_edge_statistics(&self) -> Result<HashMap<String, i64>> {
        let mut stats = HashMap::new();

        // 获取所有可用的边类型
        let edge_types = self.get_available_edge_types().await.unwrap_or_default();

        for et in edge_types {
            let table_path = format!("silver/edges/{}", et);
            if let Ok(table_uri) = self.path_to_url(&self.config.lake_path.join(&table_path)) {
                if let Ok(table) = deltalake::open_table(table_uri).await {
                    // 使用表的版本数量估算记录数（简化实现）
                    let file_count = table.version().map_or(0, |v| v as i64);
                    stats.insert(et, file_count);
                }
            }
        }

        Ok(stats)
    }

    /// 获取所有可用的边类型
    ///
    /// # 返回
    /// * `Result<Vec<String>>` - 边类型列表
    async fn get_available_edge_types(&self) -> Result<Vec<String>> {
        let edges_path = self.config.lake_path.join("silver/edges");
        let mut edge_types = Vec::new();

        if tokio::fs::metadata(&edges_path).await.is_ok() {
            let mut entries = tokio::fs::read_dir(&edges_path).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if path.is_dir() {
                    if let Some(dir_name) = path.file_name().and_then(|name| name.to_str()) {
                        edge_types.push(dir_name.to_string());
                    }
                }
            }
        }

        Ok(edge_types)
    }

    pub async fn list_tables(&self, prefix: &str) -> Result<Vec<TableSummary>> {
        let mut tables = Vec::new();
        let base_path = if prefix.is_empty() {
            self.config.lake_path.clone()
        } else {
            self.config.lake_path.join(prefix)
        };

        if tokio::fs::metadata(&base_path).await.is_err() {
            return Ok(tables);
        }

        let mut stack = vec![base_path];

        while let Some(current) = stack.pop() {
            let delta_log = current.join("_delta_log");
            if tokio::fs::metadata(&delta_log).await.is_ok() {
                if let Ok(uri) = self.path_to_url(&current) {
                    match deltalake::open_table(uri.clone()).await {
                        Ok(table) => {
                            let schema = table.schema();
                            let mut columns: Vec<ColumnSummary> = schema
                                .fields()
                                .iter()
                                .map(|field| ColumnSummary {
                                    name: field.name().to_string(),
                                    data_type: field.data_type().to_string(),
                                    nullable: field.is_nullable(),
                                })
                                .collect();
                            columns.sort_by(|a, b| a.name.cmp(&b.name));
                            let relative = current
                                .strip_prefix(&self.config.lake_path)
                                .unwrap_or(&current)
                                .to_string_lossy()
                                .to_string();
                            tables.push(TableSummary {
                                table_path: relative,
                                columns,
                            });
                        }
                        Err(err) => {
                            log::warn!("Failed to open table at '{}': {}", uri, err);
                        }
                    }
                }
            }

            let mut entries = match tokio::fs::read_dir(&current).await {
                Ok(entries) => entries,
                Err(_) => continue,
            };

            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|e| StorageError::Other(e.into()))?
            {
                if entry
                    .file_type()
                    .await
                    .map_err(|e| StorageError::Other(e.into()))?
                    .is_dir()
                {
                    stack.push(entry.path());
                }
            }
        }

        tables.sort_by(|a, b| a.table_path.cmp(&b.table_path));
        Ok(tables)
    }

    pub async fn search_bm25(
        &self,
        entity_type: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<TextSearchHit>> {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }
        let bm25 = self.engine.storage.bm25.as_ref().ok_or_else(|| {
            StorageError::SyncError("BM25 index is not enabled for this store".into())
        })?;
        let txn = self.engine.storage.graph_env.read_txn()?;
        let limit = limit.max(1);
        let raw_results = bm25
            .search(&txn, trimmed, limit)
            .map_err(StorageError::Graph)?;
        let mut hits = Vec::with_capacity(raw_results.len());
        for (doc_id, score) in raw_results {
            match self.engine.storage.get_node(&txn, &doc_id) {
                Ok(node) if node.label == entity_type => {
                    hits.push(TextSearchHit {
                        score,
                        node: Self::node_to_map(node),
                    });
                }
                Ok(_) => continue,
                Err(GraphError::NodeNotFound) => continue,
                Err(err) => return Err(StorageError::from(err)),
            }
        }
        Ok(hits)
    }

    pub async fn search_vectors(
        &self,
        entity_type: &str,
        query_vector: &[f64],
        limit: usize,
    ) -> Result<Vec<VectorSearchHit>> {
        if query_vector.is_empty() {
            return Ok(Vec::new());
        }
        let txn = self.engine.storage.graph_env.read_txn()?;
        let limit = limit.max(1);
        let results = match self
            .engine
            .storage
            .vectors
            .search::<fn(&HVector, &RoTxn) -> bool>(
                &txn,
                query_vector,
                limit,
                entity_type,
                None,
                false,
            ) {
            Ok(results) => results,
            Err(VectorError::EntryPointNotFound) => Vec::new(),
            Err(err) => return Err(StorageError::Graph(err.into())),
        };
        Ok(results
            .into_iter()
            .filter_map(|vector| {
                let label_matches = vector
                    .get_label()
                    .map(|value| value.inner_stringify() == entity_type)
                    .unwrap_or(true);
                if !label_matches {
                    return None;
                }
                let distance_f64 = vector.distance.unwrap_or(0.0);
                let distance = distance_f64 as f32;
                let similarity = (1.0 / (1.0 + distance_f64)) as f32;
                Some(VectorSearchHit {
                    distance,
                    similarity,
                    vector: Self::vector_to_map(vector),
                })
            })
            .collect())
    }

    pub async fn search_hybrid(
        &self,
        entity_type: &str,
        query_text: &str,
        query_vector: &[f64],
        alpha: f32,
        limit: usize,
    ) -> Result<Vec<HybridSearchHit>> {
        if query_text.trim().is_empty() && query_vector.is_empty() {
            return Ok(Vec::new());
        }

        let alpha = alpha.clamp(0.0, 1.0);
        let limit = limit.max(1);

        let bm25_handle = if !query_text.trim().is_empty() {
            let storage = Arc::clone(&self.engine.storage);
            let text = query_text.trim().to_string();
            Some(tokio::task::spawn_blocking(
                move || -> Result<Vec<(u128, f32)>> {
                    let txn = storage.graph_env.read_txn()?;
                    let bm25 = storage.bm25.as_ref().ok_or_else(|| {
                        StorageError::SyncError("BM25 index is not enabled for this store".into())
                    })?;
                    bm25.search(&txn, &text, limit * 2)
                        .map_err(StorageError::Graph)
                },
            ))
        } else {
            None
        };

        let vector_handle = if !query_vector.is_empty() {
            let storage = Arc::clone(&self.engine.storage);
            let query_vec: Vec<f64> = query_vector.to_vec();
            let label = entity_type.to_string();
            Some(tokio::task::spawn_blocking(
                move || -> Result<Vec<HVector>> {
                    let txn = storage.graph_env.read_txn()?;
                    match storage.vectors.search::<fn(&HVector, &RoTxn) -> bool>(
                        &txn,
                        &query_vec,
                        limit * 2,
                        &label,
                        None,
                        false,
                    ) {
                        Ok(results) => Ok(results),
                        Err(VectorError::EntryPointNotFound) => Ok(Vec::new()),
                        Err(err) => Err(StorageError::Graph(err.into())),
                    }
                },
            ))
        } else {
            None
        };

        let bm25_results = if let Some(handle) = bm25_handle {
            handle
                .await
                .map_err(|e| StorageError::SyncError(format!("BM25 search task failed: {}", e)))??
        } else {
            Vec::new()
        };

        let vector_results = if let Some(handle) = vector_handle {
            handle.await.map_err(|e| {
                StorageError::SyncError(format!("Vector search task failed: {}", e))
            })??
        } else {
            Vec::new()
        };

        let mut combined_scores: HashMap<u128, f32> = HashMap::new();
        for (doc_id, score) in bm25_results {
            combined_scores
                .entry(doc_id)
                .and_modify(|existing| *existing = existing.max(alpha * score))
                .or_insert(alpha * score);
        }

        for vector in &vector_results {
            let label_matches = vector
                .get_label()
                .map(|value| value.inner_stringify() == entity_type)
                .unwrap_or(true);
            if !label_matches {
                continue;
            }
            let similarity = (1.0 / (1.0 + vector.distance.unwrap_or(0.0))) as f32;
            combined_scores
                .entry(vector.id)
                .and_modify(|existing| *existing += (1.0 - alpha) * similarity)
                .or_insert((1.0 - alpha) * similarity);
        }

        let mut entries: Vec<(u128, f32)> = combined_scores.into_iter().collect();
        entries.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        entries.truncate(limit);

        let txn = self.engine.storage.graph_env.read_txn()?;
        let mut hits = Vec::with_capacity(entries.len());
        for (doc_id, score) in entries {
            match self.engine.storage.get_node(&txn, &doc_id) {
                Ok(node) if node.label == entity_type => {
                    hits.push(HybridSearchHit {
                        score,
                        node: Some(Self::node_to_map(node)),
                        vector: None,
                    });
                }
                Ok(_) => continue,
                Err(GraphError::NodeNotFound) => {
                    match self
                        .engine
                        .storage
                        .vectors
                        .get_vector(&txn, doc_id, 0, true)
                    {
                        Ok(vector)
                            if vector
                                .get_label()
                                .map(|value| value.inner_stringify() == entity_type)
                                .unwrap_or(true) =>
                        {
                            hits.push(HybridSearchHit {
                                score,
                                node: None,
                                vector: Some(Self::vector_to_map(vector)),
                            });
                        }
                        Ok(_) => continue,
                        Err(err) => return Err(StorageError::Graph(err.into())),
                    }
                }
                Err(err) => return Err(StorageError::from(err)),
            }
        }

        Ok(hits)
    }

    pub async fn search_hybrid_multi(
        &self,
        entity_types: &[String],
        query_text: &str,
        query_vector: &[f64],
        alpha: f32,
        limit: usize,
    ) -> Result<Vec<MultiEntitySearchHit>> {
        if entity_types.is_empty() || (query_text.trim().is_empty() && query_vector.is_empty()) {
            return Ok(Vec::new());
        }

        let trimmed = query_text.trim();
        let mut aggregate: Vec<MultiEntitySearchHit> = Vec::new();

        for entity_type in entity_types {
            let hits = self
                .search_hybrid(entity_type, trimmed, query_vector, alpha, limit)
                .await?;

            aggregate.extend(hits.into_iter().map(|hit| {
                MultiEntitySearchHit {
                    entity_type: entity_type.clone(),
                    score: hit.score,
                    summary: hit
                        .node
                        .as_ref()
                        .and_then(|node| {
                            Self::extract_text_field(
                                node,
                                &["title", "name", "label", "signature", "path"],
                            )
                        })
                        .or_else(|| {
                            hit.vector.as_ref().and_then(|vector| {
                                Self::extract_text_field(
                                    vector,
                                    &["text", "body", "summary", "content", "preview"],
                                )
                            })
                        }),
                    node: hit.node,
                    vector: hit.vector,
                }
            }));
        }

        aggregate.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        aggregate.truncate(limit.max(1));

        Ok(aggregate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use deltalake::arrow::array::{Int32Array, Int64Array, StringArray, UInt64Array};
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    use deltalake::datafusion::execution::context::SessionContext;
    use helix_db::helix_engine::traversal_core::{HelixGraphEngine, HelixGraphEngineOpts};
    use std::sync::Arc;
    use tempfile::tempdir;

    async fn create_lake(config: &StorageConfig) -> Lake {
        tokio::fs::create_dir_all(&config.engine_path)
            .await
            .unwrap();
        let engine_opts = HelixGraphEngineOpts {
            path: config
                .engine_path
                .to_str()
                .expect("Engine path not valid UTF-8")
                .to_string(),
            ..Default::default()
        };
        let engine = Arc::new(HelixGraphEngine::new(engine_opts).unwrap());
        Lake::new(config.clone(), engine).await.unwrap()
    }

    #[tokio::test]
    async fn test_write_and_read_delta_table() {
        let dir = tempdir().unwrap();
        let config = StorageConfig::new(dir.path());
        let lake = create_lake(&config).await;
        let table_name = "test_table";

        // 定义Schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // 创建RecordBatch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        // 写入数据
        let result = lake.write_batches(table_name, vec![batch], None).await;
        assert!(result.is_ok());

        // 验证表是否存在
        let table_uri = Url::from_file_path(config.lake_path.join(table_name)).unwrap();
        let table = deltalake::open_table(table_uri).await.unwrap();
        assert_eq!(table.version(), Some(0));
        assert_eq!(table.get_file_uris().into_iter().count(), 1);
    }

    #[tokio::test]
    async fn test_write_batches_upsert_by_primary_key() {
        let dir = tempdir().unwrap();
        let config = StorageConfig::new(dir.path());
        let lake = create_lake(&config).await;
        let table_name = "silver/entities/projects";

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let initial_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["alpha"])),
            ],
        )
        .unwrap();

        lake.write_batches(
            table_name,
            vec![initial_batch.clone()],
            Some(vec!["id".to_string()]),
        )
        .await
        .unwrap();

        // Update the row with the same primary key but different value.
        let updated_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["beta"])),
            ],
        )
        .unwrap();

        lake.write_batches(
            table_name,
            vec![updated_batch.clone()],
            Some(vec!["id".to_string()]),
        )
        .await
        .unwrap();

        let table_path = config.lake_path.join(table_name);
        assert!(table_path.exists(), "Table path should exist after writes");
        assert!(
            table_path.join("_delta_log").exists(),
            "Delta log directory should be created"
        );
        let table_uri = lake.path_to_url(&table_path).unwrap();
        let final_table = deltalake::open_table(table_uri).await.unwrap();

        let ctx = Lake::single_partition_session();
        ctx.register_table("projects", Arc::new(final_table.clone()))
            .unwrap();

        let df = ctx
            .sql("SELECT id, name FROM projects ORDER BY id")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(name_array.value(0), "beta");

        // Ensure total row count is 1.
        let count_df = ctx
            .sql("SELECT count(*) as cnt FROM projects")
            .await
            .unwrap();
        let count_batches = count_df.collect().await.unwrap();
        let count_column = count_batches[0].column(0);
        let count_value = if let Some(arr) = count_column.as_any().downcast_ref::<UInt64Array>() {
            arr.value(0)
        } else {
            let arr = count_column.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(0) as u64
        };
        assert_eq!(count_value, 1);
    }

    #[tokio::test]
    async fn test_read_changes_since() {
        let dir = tempdir().unwrap();
        let config = StorageConfig::new(dir.path());
        let lake = create_lake(&config).await;
        let table_name = "silver/entities/nodes";

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch_v0 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["alpha"])),
            ],
        )
        .unwrap();

        lake.write_batches(
            table_name,
            vec![batch_v0.clone()],
            Some(vec!["id".to_string()]),
        )
        .await
        .unwrap();

        let (changes_v0, latest) = lake.read_changes_since(table_name, -1).await.unwrap();
        assert_eq!(latest, 0);
        assert_eq!(changes_v0.len(), 1);
        assert_eq!(changes_v0[0].0, 0);
        assert_eq!(changes_v0[0].1.len(), 1);
        assert_eq!(changes_v0[0].1[0].num_rows(), 1);

        let batch_v1 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["beta"])),
            ],
        )
        .unwrap();

        lake.write_batches(table_name, vec![batch_v1], Some(vec!["id".to_string()]))
            .await
            .unwrap();

        let (changes_v1, latest_after) = lake.read_changes_since(table_name, 0).await.unwrap();
        assert_eq!(latest_after, 1);
        assert_eq!(changes_v1.len(), 1);
        assert_eq!(changes_v1[0].0, 1);
        assert_eq!(changes_v1[0].1.len(), 1);
        assert_eq!(changes_v1[0].1[0].num_rows(), 1);
    }
}
