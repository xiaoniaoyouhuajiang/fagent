use chrono::Utc;
use deltalake::open_table;
use fstorage::{
    fetch::{EntityCategory, Fetchable},
    schemas::generated_schemas::{Project, ReadmeChunk},
    sync::DataSynchronizer,
};
use heed3::RoTxn;
use helix_db::{
    helix_engine::traversal_core::ops::vectors::search::SearchVAdapter,
    helix_engine::{traversal_core::ops::g::G, vector_core::vector::HVector},
};

mod common;

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
    let project_table_uri = project_table
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("non-UTF8 project table path"))?
        .to_string();
    let project_delta = open_table(project_table_uri).await?;
    assert_eq!(project_delta.version(), 0);

    let vector_table = ctx.config.lake_path.join(ReadmeChunk::table_name());
    let vector_table_uri = vector_table
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("non-UTF8 vector table path"))?
        .to_string();
    let vector_delta = open_table(vector_table_uri).await?;
    assert_eq!(vector_delta.version(), 0);

    let offsets = ctx.catalog.list_ingestion_offsets()?;
    let vector_offset = offsets
        .iter()
        .find(|offset| offset.table_path.ends_with("readmechunk"))
        .expect("vector offset registered");
    assert_eq!(vector_offset.category, EntityCategory::Vector);
    assert_eq!(vector_offset.primary_keys, vec!["embedding_id".to_string()]);

    let txn = ctx.engine.storage.graph_env.read_txn()?;
    let query: Vec<f64> = embedding_values.iter().map(|v| *v as f64).collect();
    let results = G::new(ctx.engine.storage.clone(), &txn)
        .search_v::<fn(&HVector, &RoTxn) -> bool, _>(&query, 10, ReadmeChunk::ENTITY_TYPE, None)
        .collect_to::<Vec<_>>();

    assert!(
        !results.is_empty(),
        "vector search returns at least one result"
    );

    Ok(())
}
