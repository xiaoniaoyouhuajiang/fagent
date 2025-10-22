use std::{collections::HashMap, path::PathBuf, sync::Arc};

use anyhow::Context;
use deltalake::{
    arrow::{
        array::{
            Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
            StringArray, TimestampMicrosecondArray,
        },
        datatypes::DataType,
        record_batch::RecordBatch,
    },
    datafusion::{datasource::TableProvider, execution::context::SessionContext},
    open_table,
};
use fstorage::{
    fetch::{Fetchable, Fetcher},
    models::{EntityIdentifier, SyncBudget, SyncContext},
    schemas::generated_schemas::{Commit, HasVersion, IsCommit, Project, ReadmeChunk, Version},
    sync::DataSynchronizer,
    utils::id::stable_node_id_u128,
};
use helix_db::helix_engine::storage_core::{
    graph_visualization::GraphVisualization, storage_methods::StorageMethods,
};
use helix_db::protocol::value::Value as HelixValue;
use serde_json::{Map as JsonMap, Value, json};
use url::Url;
use uuid::Uuid;

mod common;
mod support;

use support::fixture::FixtureFetcher;

/// End-to-end test that replays the gitfetcher fixture through the sync pipeline.
#[tokio::test]
async fn sync_from_fixture_populates_hot_and_cold_layers() -> anyhow::Result<()> {
    let ctx = common::init_test_context().await?;
    let fixture_dir =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/gitfetcher/tinykv");

    let fixture_fetcher = Arc::new(FixtureFetcher::new());
    fixture_fetcher
        .load_and_register("tinykv", &fixture_dir)
        .await?;

    let fetcher_arc: Arc<dyn Fetcher> = fixture_fetcher.clone();
    ctx.synchronizer.register_fetcher(fetcher_arc);

    let params = json!({"fixture_key": "tinykv"});
    let context = SyncContext {
        triggering_query: Some("fixture-test".to_string()),
        target_entities: vec![EntityIdentifier {
            uri: "talent-plan/tinykv".to_string(),
            entity_type: Project::ENTITY_TYPE.to_string(),
            fetcher_name: Some("fixture_fetcher".to_string()),
            params: Some(params.clone()),
            anchor_key: None,
        }],
    };

    ctx.synchronizer
        .sync(
            "fixture_fetcher",
            params,
            context,
            SyncBudget::ByRequestCount(10),
        )
        .await?;

    let offsets = ctx.catalog.list_ingestion_offsets()?;
    let expect_tables = [
        Project::table_name(),
        Version::table_name(),
        Commit::table_name(),
        "silver/edges/hasversion".to_string(),
        "silver/edges/iscommit".to_string(),
        ReadmeChunk::table_name(),
    ];
    for table in &expect_tables {
        assert!(
            offsets
                .iter()
                .any(|offset| offset.table_path.ends_with(table)),
            "expected offset for table {table}"
        );
    }

    let project_table = Url::from_file_path(ctx.config.lake_path.join(Project::table_name()))
        .map_err(|_| anyhow::anyhow!("project table path not valid UTF-8"))?;
    let project_delta = open_table(project_table).await?;
    assert!(
        project_delta.version().is_some(),
        "project table initialized"
    );

    let vector_table = Url::from_file_path(ctx.config.lake_path.join(ReadmeChunk::table_name()))
        .map_err(|_| anyhow::anyhow!("vector table path not valid UTF-8"))?;
    let vector_delta = open_table(vector_table).await?;
    assert!(vector_delta.version().is_some(), "vector table initialized");

    let project = ctx
        .lake
        .get_node_by_keys(
            Project::ENTITY_TYPE,
            &[("url", "https://github.com/talent-plan/tinykv")],
        )
        .await?
        .expect("project hydrated in hot layer");

    let url = project
        .get("url")
        .and_then(|value| value.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            project
                .get("properties")
                .and_then(|value| value.as_object())
                .and_then(|props| props.get("url"))
                .and_then(|value| value.as_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_default();
    assert_eq!(url, "https://github.com/talent-plan/tinykv");

    // Validate hot/cold consistency via row counts.
    let project_rows = delta_row_count(ctx.config.lake_path.join(Project::table_name()))
        .await
        .context("count project rows")?;
    let version_rows = delta_row_count(ctx.config.lake_path.join(Version::table_name()))
        .await
        .context("count version rows")?;
    let commit_rows = delta_row_count(ctx.config.lake_path.join(Commit::table_name()))
        .await
        .context("count commit rows")?;
    let has_version_rows = delta_row_count(ctx.config.lake_path.join("silver/edges/hasversion"))
        .await
        .context("count hasversion rows")?;
    let is_commit_rows = delta_row_count(ctx.config.lake_path.join("silver/edges/iscommit"))
        .await
        .context("count iscommit rows")?;
    let readme_rows = delta_row_count(ctx.config.lake_path.join(ReadmeChunk::table_name()))
        .await
        .context("count readmechunk rows")?;

    let (helix_nodes, helix_edges, helix_vectors) = helix_counts(&ctx)?;
    let project_entries = read_delta_rows(ctx.config.lake_path.join(Project::table_name())).await?;
    let version_entries = read_delta_rows(ctx.config.lake_path.join(Version::table_name())).await?;
    let commit_entries = read_delta_rows(ctx.config.lake_path.join(Commit::table_name())).await?;

    verify_nodes_against_helix(
        &ctx,
        Project::ENTITY_TYPE,
        &Project::primary_keys(),
        &project_entries,
    )
    .await?;
    verify_nodes_against_helix(
        &ctx,
        Version::ENTITY_TYPE,
        &Version::primary_keys(),
        &version_entries,
    )
    .await?;
    verify_nodes_against_helix(
        &ctx,
        Commit::ENTITY_TYPE,
        &Commit::primary_keys(),
        &commit_entries,
    )
    .await?;

    let has_version_entries =
        read_delta_rows(ctx.config.lake_path.join("silver/edges/hasversion")).await?;
    let is_commit_entries =
        read_delta_rows(ctx.config.lake_path.join("silver/edges/iscommit")).await?;

    verify_edges_against_helix(&ctx, &has_version_entries, HasVersion::ENTITY_TYPE)?;
    verify_edges_against_helix(&ctx, &is_commit_entries, IsCommit::ENTITY_TYPE)?;

    assert_eq!(
        project_rows + version_rows + commit_rows,
        helix_nodes,
        "node counts differ between Delta and Helix"
    );
    assert_eq!(
        has_version_rows + is_commit_rows,
        helix_edges,
        "edge counts differ between Delta and Helix"
    );
    if readme_rows == 0 {
        assert_eq!(
            helix_vectors, 0,
            "Helix should have zero vectors when Delta contains none"
        );
    } else {
        // Fixtures produced with a null embedding provider persist vectors to Delta but the
        // hot path skips empty embeddings. Accept either 0 or full parity and surface mismatch.
        assert!(
            helix_vectors == readme_rows || helix_vectors == 0,
            "vector counts differ between Delta ({readme_rows}) and Helix ({helix_vectors})"
        );
    }

    Ok(())
}

async fn delta_row_count(path: PathBuf) -> anyhow::Result<usize> {
    let url =
        Url::from_file_path(&path).map_err(|_| anyhow::anyhow!("non-UTF8 path {:?}", path))?;
    let table = open_table(url).await?;
    let ctx = SessionContext::new();
    let table_provider: Arc<dyn TableProvider> = Arc::new(table);
    let df = ctx.read_table(table_provider)?;
    let batches = df.collect().await?;

    Ok(batches.iter().map(|batch| batch.num_rows()).sum())
}

fn helix_counts(ctx: &common::TestContext) -> anyhow::Result<(usize, usize, usize)> {
    let txn = ctx.engine.storage.graph_env.read_txn()?;
    let stats_json = ctx
        .engine
        .storage
        .get_db_stats_json(&txn)
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    let stats: Value = serde_json::from_str(&stats_json)?;
    let nodes = stats
        .get("num_nodes")
        .and_then(|v| v.as_u64())
        .unwrap_or_default() as usize;
    let edges = stats
        .get("num_edges")
        .and_then(|v| v.as_u64())
        .unwrap_or_default() as usize;
    let vectors = stats
        .get("num_vectors")
        .and_then(|v| v.as_u64())
        .unwrap_or_default() as usize;
    Ok((nodes, edges, vectors))
}

async fn read_delta_rows(path: PathBuf) -> anyhow::Result<Vec<HashMap<String, serde_json::Value>>> {
    let url =
        Url::from_file_path(&path).map_err(|_| anyhow::anyhow!("non-UTF8 path {:?}", path))?;
    let table = open_table(url).await?;
    let ctx = SessionContext::new();
    let provider: Arc<dyn TableProvider> = Arc::new(table);
    let df = ctx.read_table(provider)?;
    let batches = df.collect().await?;

    let mut rows = Vec::new();
    for batch in batches {
        rows.extend(record_batch_to_maps(&batch)?);
    }
    Ok(rows)
}

fn record_batch_to_maps(
    batch: &RecordBatch,
) -> anyhow::Result<Vec<HashMap<String, serde_json::Value>>> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    let schema = batch.schema();
    for row in 0..batch.num_rows() {
        let mut map = HashMap::new();
        for (idx, field) in schema.fields().iter().enumerate() {
            let column = batch.column(idx);
            let value = arrow_cell_to_json(column, row)?;
            map.insert(field.name().clone(), value);
        }
        rows.push(map);
    }
    Ok(rows)
}

fn arrow_cell_to_json(array: &ArrayRef, row: usize) -> anyhow::Result<serde_json::Value> {
    use serde_json::Value as JsonValue;

    if array.is_null(row) {
        return Ok(JsonValue::Null);
    }

    match array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let value = array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string();
            Ok(JsonValue::String(value))
        }
        DataType::Int32 => {
            let value = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row);
            Ok(JsonValue::Number(value.into()))
        }
        DataType::Int64 => {
            let value = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row);
            Ok(JsonValue::Number(value.into()))
        }
        DataType::Float32 => {
            let value = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row);
            serde_json::Number::from_f64(value as f64)
                .map(JsonValue::Number)
                .ok_or_else(|| anyhow::anyhow!("invalid f32 value"))
        }
        DataType::Float64 => {
            let value = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            serde_json::Number::from_f64(value)
                .map(JsonValue::Number)
                .ok_or_else(|| anyhow::anyhow!("invalid f64 value"))
        }
        DataType::Boolean => {
            let value = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row);
            Ok(JsonValue::Bool(value))
        }
        DataType::Timestamp(time_unit, _) => match time_unit {
            deltalake::arrow::datatypes::TimeUnit::Microsecond => {
                let value = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .value(row);
                let secs = value / 1_000_000;
                let nanos = ((value % 1_000_000) * 1_000) as u32;
                let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
                    .ok_or_else(|| anyhow::anyhow!("invalid timestamp value: {}", value))?;
                Ok(JsonValue::String(datetime.to_rfc3339()))
            }
            _ => Err(anyhow::anyhow!(
                "unsupported timestamp unit: {:?}",
                time_unit
            )),
        },
        other => Err(anyhow::anyhow!("unsupported Arrow type: {:?}", other)),
    }
}

async fn verify_nodes_against_helix(
    ctx: &common::TestContext,
    entity_type: &str,
    primary_keys: &[&str],
    rows: &[HashMap<String, serde_json::Value>],
) -> anyhow::Result<()> {
    for row in rows {
        let mut pk_pairs: Vec<(&str, String)> = Vec::new();
        for key in primary_keys {
            let value = row
                .get(*key)
                .ok_or_else(|| anyhow::anyhow!("row missing primary key '{}'", key))?;
            let value_str = json_value_to_string(value)
                .with_context(|| format!("primary key '{}' is not string-like", key))?;
            pk_pairs.push((key, value_str));
        }

        let owned_pairs: Vec<(&str, String)> =
            pk_pairs.iter().map(|(k, v)| (*k, v.clone())).collect();
        let id = stable_node_id_u128(entity_type, &owned_pairs);
        let id_str = Uuid::from_u128(id).to_string();

        let node = ctx
            .lake
            .get_node_by_id(&id_str, Some(entity_type))
            .await?
            .ok_or_else(|| anyhow::anyhow!("Helix node '{}' not found", id_str))?;

        let label = node
            .get("label")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("node '{}' missing label", id_str))?;
        assert_eq!(
            label, entity_type,
            "node '{}' label mismatch (expected '{}')",
            id_str, entity_type
        );

        let properties = node
            .get("properties")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow::anyhow!("node '{}' missing properties map", id_str))?;

        for (column, expected_value) in row {
            let actual_value = properties.get(column).cloned().unwrap_or(Value::Null);
            assert_eq!(
                actual_value, *expected_value,
                "Mismatch for node '{}' column '{}'",
                id_str, column
            );
        }
    }
    Ok(())
}

fn verify_edges_against_helix(
    ctx: &common::TestContext,
    rows: &[HashMap<String, serde_json::Value>],
    entity_type: &str,
) -> anyhow::Result<()> {
    let txn = ctx.engine.storage.graph_env.read_txn()?;

    for row in rows {
        let id_str = row
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("edge row missing 'id' column"))?;
        let edge_id = Uuid::parse_str(id_str)
            .map_err(|err| anyhow::anyhow!("invalid edge id '{}': {}", id_str, err))?;
        let edge = ctx
            .engine
            .storage
            .get_edge(&txn, &edge_id.as_u128())
            .map_err(|err| anyhow::anyhow!("helix edge lookup failed: {}", err))?;

        assert_eq!(
            edge.label, entity_type,
            "edge '{}' label mismatch (expected '{}')",
            id_str, entity_type
        );

        let from_node_str = Uuid::from_u128(edge.from_node).to_string();
        let to_node_str = Uuid::from_u128(edge.to_node).to_string();

        if let Some(expected_from) = row.get("from_node_id").and_then(Value::as_str) {
            assert_eq!(
                expected_from, from_node_str,
                "edge '{}' from_node_id mismatch",
                id_str
            );
        }
        if let Some(expected_to) = row.get("to_node_id").and_then(Value::as_str) {
            assert_eq!(
                expected_to, to_node_str,
                "edge '{}' to_node_id mismatch",
                id_str
            );
        }

        let props: HashMap<String, Value> = edge
            .properties
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, helix_value_to_json(&v)))
            .collect();

        for (column, expected_value) in row {
            match column.as_str() {
                "id" | "from_node_id" | "to_node_id" => continue,
                _ => {
                    let actual_value = props.get(column).cloned().unwrap_or(Value::Null);
                    assert_eq!(
                        actual_value, *expected_value,
                        "Mismatch for edge '{}' column '{}'",
                        id_str, column
                    );
                }
            }
        }
    }

    Ok(())
}

fn json_value_to_string(value: &Value) -> anyhow::Result<String> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Number(num) => Ok(num.to_string()),
        other => Err(anyhow::anyhow!("unsupported JSON value: {}", other)),
    }
}

fn helix_value_to_json(value: &HelixValue) -> Value {
    match value {
        HelixValue::String(s) => Value::String(s.clone()),
        HelixValue::Boolean(b) => Value::Bool(*b),
        HelixValue::I32(v) => Value::Number((*v).into()),
        HelixValue::I64(v) => Value::Number((*v).into()),
        HelixValue::U32(v) => Value::Number((*v).into()),
        HelixValue::U64(v) => Value::Number((*v).into()),
        HelixValue::F32(v) => serde_json::Number::from_f64(*v as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        HelixValue::F64(v) => serde_json::Number::from_f64(*v)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        HelixValue::Array(items) => Value::Array(items.iter().map(helix_value_to_json).collect()),
        HelixValue::Object(map) => {
            let mut obj = JsonMap::new();
            for (k, v) in map {
                obj.insert(k.clone(), helix_value_to_json(v));
            }
            Value::Object(obj)
        }
        HelixValue::Date(date) => Value::String(date.to_string()),
        HelixValue::Empty => Value::Null,
        HelixValue::Id(id) => Value::String(id.stringify()),
        HelixValue::U8(v) => Value::Number((*v).into()),
        HelixValue::U16(v) => Value::Number((*v).into()),
        HelixValue::U128(v) => Value::String(v.to_string()),
        HelixValue::I8(v) => Value::Number((*v).into()),
        HelixValue::I16(v) => Value::Number((*v).into()),
    }
}
