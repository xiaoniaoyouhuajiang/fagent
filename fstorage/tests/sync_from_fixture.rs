use std::{path::PathBuf, sync::Arc};

use anyhow::Context;
use deltalake::datafusion::{datasource::TableProvider, execution::context::SessionContext};
use deltalake::open_table;
use fstorage::{
    fetch::{Fetchable, Fetcher},
    models::{EntityIdentifier, SyncBudget, SyncContext},
    schemas::generated_schemas::{Commit, Project, ReadmeChunk, Version},
    sync::DataSynchronizer,
};
use helix_db::helix_engine::storage_core::graph_visualization::GraphVisualization;
use serde_json::{Value, json};
use url::Url;

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
