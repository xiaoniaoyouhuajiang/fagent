use chrono::Utc;
use deltalake::{
    datafusion::{datasource::TableProvider, execution::context::SessionContext},
    open_table,
};
use fstorage::{
    fetch::{EntityCategory, Fetchable},
    schema_registry,
    schemas::generated_schemas::{CodeChunk, Function, Project, ReadmeChunk},
    sync::DataSynchronizer,
    utils,
};
use heed3::RoTxn;
use helix_db::{
    helix_engine::storage_core::graph_visualization::GraphVisualization,
    helix_engine::traversal_core::ops::out::out::OutAdapter,
    helix_engine::traversal_core::ops::source::{add_e::EdgeType, n_from_id::NFromIdAdapter},
    helix_engine::traversal_core::ops::vectors::search::SearchVAdapter,
    helix_engine::{
        traversal_core::ops::g::G, traversal_core::traversal_value::Traversable,
        vector_core::vector::HVector,
    },
};
use serde_json::Value;
use std::{path::PathBuf, sync::Arc};
use url::Url;
use uuid::Uuid;

mod common;

#[tokio::test]
async fn readme_vectors_are_idempotent() -> anyhow::Result<()> {
    let ctx = common::init_test_context().await?;

    let project_url = "https://github.com/example/repo-idempotent".to_string();
    let project = Project {
        url: Some(project_url.clone()),
        name: Some("example".to_string()),
        description: Some("Vector idempotency test".to_string()),
        language: Some("Rust".to_string()),
        stars: Some(1),
        forks: Some(0),
    };

    let embedding_values = vec![0.4_f32, 0.5_f32, 0.6_f32];
    let embedding_id = "readme-vector-idem".to_string();
    let readme_chunk = ReadmeChunk {
        id: None,
        project_url: Some(project_url.clone()),
        revision_sha: Some("abc456".to_string()),
        source_file: Some("README.md".to_string()),
        start_line: Some(1),
        end_line: Some(20),
        text: Some("# Example".to_string()),
        embedding: Some(embedding_values.clone()),
        embedding_model: Some("fixture-model".to_string()),
        embedding_id: Some(embedding_id.clone()),
        token_count: Some(24),
        chunk_order: Some(0),
        created_at: Some(Utc::now()),
        updated_at: None,
    };

    let mut first_graph = fstorage::fetch::GraphData::new();
    first_graph.add_entities(vec![project.clone()]);
    first_graph.add_entities(vec![readme_chunk.clone()]);
    ctx.synchronizer.process_graph_data(first_graph).await?;

    let vector_table = ctx.config.lake_path.join(ReadmeChunk::table_name());
    assert_eq!(
        delta_row_count(vector_table.clone()).await?,
        1,
        "Delta table should contain a single vector row after first sync"
    );
    let index_meta = schema_registry::vector_index(ReadmeChunk::ENTITY_TYPE)
        .expect("vector index metadata for readme chunk");
    let index_table = ctx.config.lake_path.join(index_meta.index_table);
    assert_eq!(
        delta_row_count(index_table.clone()).await?,
        1,
        "Vector index table records single entry after first sync"
    );

    let first_uuid = vector_uuid_for_embedding(&ctx, &embedding_id).await?;
    assert!(
        first_uuid.is_some(),
        "vector UUID should be registered in index after first sync"
    );
    let first_vector_count = helix_vector_count(&ctx)?;
    assert!(
        first_vector_count >= 1,
        "Helix should contain at least one vector after first sync"
    );

    let mut second_graph = fstorage::fetch::GraphData::new();
    second_graph.add_entities(vec![project]);
    let mut updated_chunk = readme_chunk;
    updated_chunk.text = Some("# Example Updated".to_string());
    second_graph.add_entities(vec![updated_chunk]);
    ctx.synchronizer.process_graph_data(second_graph).await?;

    assert_eq!(
        delta_row_count(vector_table.clone()).await?,
        1,
        "Delta table should still report a single vector row after second sync"
    );
    assert_eq!(
        delta_row_count(index_table.clone()).await?,
        1,
        "Vector index table should remain a single row after second sync"
    );
    let second_vector_count = helix_vector_count(&ctx)?;
    assert_eq!(
        second_vector_count, first_vector_count,
        "Helix vector count should remain stable across repeated syncs"
    );

    let second_uuid = vector_uuid_for_embedding(&ctx, &embedding_id).await?;
    assert_eq!(
        first_uuid, second_uuid,
        "Vector UUID should be stable across repeated syncs"
    );

    Ok(())
}

#[tokio::test]
async fn vector_pipeline_persists_to_lake_and_engine() -> anyhow::Result<()> {
    let ctx = common::init_test_context().await?;

    let project_url = "https://github.com/example/repo".to_string();
    let project = Project {
        url: Some(project_url.clone()),
        name: Some("example".to_string()),
        description: Some("Vector pipeline test".to_string()),
        language: Some("Rust".to_string()),
        stars: Some(1),
        forks: Some(0),
    };

    let embedding_values = vec![0.1_f32, 0.2_f32, 0.3_f32];
    let embedding_id = "readme-chunk-1".to_string();
    let readme_chunk = ReadmeChunk {
        id: None,
        project_url: Some(project_url.clone()),
        revision_sha: Some("abc123".to_string()),
        source_file: Some("README.md".to_string()),
        start_line: Some(1),
        end_line: Some(20),
        text: Some("# Example".to_string()),
        embedding: Some(embedding_values.clone()),
        embedding_model: Some("fixture-model".to_string()),
        embedding_id: Some(embedding_id.clone()),
        token_count: Some(12),
        chunk_order: Some(0),
        created_at: Some(Utc::now()),
        updated_at: None,
    };

    let mut graph_data = fstorage::fetch::GraphData::new();
    graph_data.add_entities(vec![project]);
    graph_data.add_entities(vec![readme_chunk]);

    ctx.synchronizer.process_graph_data(graph_data).await?;

    let project_table = ctx.config.lake_path.join(Project::table_name());
    let project_table_url = Url::from_file_path(&project_table)
        .map_err(|_| anyhow::anyhow!("non-UTF8 project table path"))?;
    let project_delta = open_table(project_table_url).await?;
    assert_eq!(project_delta.version(), Some(0));

    let vector_table = ctx.config.lake_path.join(ReadmeChunk::table_name());
    let vector_table_url = Url::from_file_path(&vector_table)
        .map_err(|_| anyhow::anyhow!("non-UTF8 vector table path"))?;
    let vector_delta = open_table(vector_table_url).await?;
    assert_eq!(vector_delta.version(), Some(0));

    let offsets = ctx.catalog.list_ingestion_offsets()?;
    let vector_offset = offsets
        .iter()
        .find(|offset| offset.table_path.ends_with("readmechunk"))
        .expect("vector offset registered");
    assert_eq!(vector_offset.category, EntityCategory::Vector);
    assert_eq!(vector_offset.primary_keys, vec!["id".to_string()]);

    let contains_content_offset = offsets
        .iter()
        .find(|offset| offset.table_path.ends_with("containscontent"));
    assert!(
        contains_content_offset.is_some(),
        "contains_content offset registered"
    );

    let txn = ctx.engine.storage.graph_env.read_txn()?;
    let query: Vec<f64> = embedding_values.iter().map(|v| *v as f64).collect();
    let vector_value = G::new(ctx.engine.storage.clone(), &txn)
        .search_v::<fn(&HVector, &RoTxn) -> bool, _>(&query, 10, ReadmeChunk::ENTITY_TYPE, None)
        .collect_to_obj();
    let vector_uuid = vector_value.uuid();
    let project_id_u128 = fstorage::utils::id::stable_node_id_u128(
        Project::ENTITY_TYPE,
        &[("url", project_url.clone())],
    );

    let neighbors = G::new(ctx.engine.storage.clone(), &txn)
        .n_from_id(&project_id_u128)
        .out("edge_containscontent", &EdgeType::Vec)
        .collect_to::<Vec<_>>();
    assert!(
        neighbors
            .into_iter()
            .any(|value| value.uuid() == vector_uuid),
        "project connects to vector via contains_content edge"
    );

    Ok(())
}

#[tokio::test]
async fn embeds_edges_are_idempotent() -> anyhow::Result<()> {
    let ctx = common::init_test_context().await?;

    let version_sha = "deadbeef".to_string();
    let file_path = "src/lib.rs".to_string();
    let function_name = "handle_request".to_string();
    let function = Function {
        version_sha: Some(version_sha.clone()),
        file_path: Some(file_path.clone()),
        name: Some(function_name.clone()),
        signature: Some("fn handle_request()".to_string()),
        start_line: Some(5),
        end_line: Some(20),
        is_component: Some(false),
    };
    let function_id_u128 = utils::id::stable_node_id_u128(
        Function::ENTITY_TYPE,
        &[
            ("version_sha", version_sha.clone()),
            ("file_path", file_path.clone()),
            ("name", function_name.clone()),
        ],
    );
    let function_id = Uuid::from_u128(function_id_u128).to_string();

    let project_url = "https://github.com/example/repo-code".to_string();
    let source_node_key = format!(
        "{}::{}::{}::{}",
        Function::ENTITY_TYPE,
        version_sha,
        file_path,
        function_name
    );

    let mut graph = fstorage::fetch::GraphData::new();
    graph.add_entities(vec![function.clone()]);
    graph.add_entities(vec![build_code_chunk(
        &function_id,
        &source_node_key,
        &project_url,
        &version_sha,
        &file_path,
        "fn handle_request() {}".to_string(),
        vec![0.2, 0.3, 0.5],
        0,
    )]);

    ctx.synchronizer.process_graph_data(graph).await?;
    assert_eq!(
        embeds_neighbor_count(&ctx, function_id_u128)?,
        1,
        "initial embeds edge count"
    );

    let mut second_graph = fstorage::fetch::GraphData::new();
    second_graph.add_entities(vec![function]);
    second_graph.add_entities(vec![build_code_chunk(
        &function_id,
        &source_node_key,
        &project_url,
        &version_sha,
        &file_path,
        "fn handle_request() { println!(\"updated\"); }".to_string(),
        vec![0.9, 0.1, 0.4],
        0,
    )]);

    ctx.synchronizer.process_graph_data(second_graph).await?;
    assert_eq!(
        embeds_neighbor_count(&ctx, function_id_u128)?,
        1,
        "embeds edges remain single after re-sync"
    );

    let embeds_path = ctx.config.lake_path.join("silver/edges/embeds");
    assert_eq!(
        delta_row_count(embeds_path).await?,
        1,
        "delta table keeps single embeds row"
    );

    Ok(())
}

fn build_code_chunk(
    function_id: &str,
    source_node_key: &str,
    project_url: &str,
    version_sha: &str,
    file_path: &str,
    text: String,
    embedding: Vec<f32>,
    chunk_order: i32,
) -> CodeChunk {
    let chunk_id = Uuid::from_u128(utils::id::stable_node_id_u128(
        CodeChunk::ENTITY_TYPE,
        &[
            ("source_node_id", function_id.to_string()),
            ("chunk_order", chunk_order.to_string()),
        ],
    ))
    .to_string();

    CodeChunk {
        id: Some(chunk_id),
        project_url: Some(project_url.to_string()),
        revision_sha: Some(version_sha.to_string()),
        source_file: Some(file_path.to_string()),
        source_node_key: Some(source_node_key.to_string()),
        source_node_id: Some(function_id.to_string()),
        language: Some("rust".to_string()),
        text: Some(text),
        embedding: Some(embedding),
        embedding_model: Some("fixture-code".to_string()),
        embedding_id: Some(format!("{source_node_key}::{}", chunk_order)),
        token_count: Some(16),
        chunk_order: Some(chunk_order),
        created_at: Some(Utc::now()),
        updated_at: None,
    }
}

fn embeds_neighbor_count(ctx: &common::TestContext, function_id: u128) -> anyhow::Result<usize> {
    let txn = ctx.engine.storage.graph_env.read_txn()?;
    let neighbors = G::new(ctx.engine.storage.clone(), &txn)
        .n_from_id(&function_id)
        .out("edge_embeds", &EdgeType::Vec)
        .collect_to::<Vec<_>>();
    Ok(neighbors.len())
}

async fn delta_row_count(path: PathBuf) -> anyhow::Result<usize> {
    let url =
        Url::from_file_path(&path).map_err(|_| anyhow::anyhow!("non-UTF8 path {:?}", path))?;
    let table = open_table(url).await?;
    let ctx = SessionContext::new();
    let provider: Arc<dyn TableProvider> = Arc::new(table);
    let df = ctx.read_table(provider)?;
    let batches = df.collect().await?;
    Ok(batches.iter().map(|batch| batch.num_rows()).sum())
}

async fn vector_uuid_for_embedding(
    ctx: &common::TestContext,
    embedding_id: &str,
) -> anyhow::Result<Option<String>> {
    let meta = schema_registry::vector_index(ReadmeChunk::ENTITY_TYPE)
        .expect("vector index metadata for readme chunk");
    let map = ctx
        .lake
        .load_vector_index_map(
            meta.index_table,
            meta.id_column,
            &[embedding_id.to_string()],
        )
        .await?;
    Ok(map.get(embedding_id).cloned())
}

fn helix_vector_count(ctx: &common::TestContext) -> anyhow::Result<usize> {
    let txn = ctx.engine.storage.graph_env.read_txn()?;
    let stats_json = ctx
        .engine
        .storage
        .get_db_stats_json(&txn)
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    let stats: Value = serde_json::from_str(&stats_json)?;
    Ok(stats
        .get("num_vectors")
        .and_then(|v| v.as_u64())
        .unwrap_or_default() as usize)
}
