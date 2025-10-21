use deltalake::{
    arrow::array::StringArray, datafusion::execution::context::SessionContext, open_table,
};
use fstorage::{
    fetch::Fetchable, schemas::generated_schemas::Project, sync::DataSynchronizer, utils,
};
use helix_db::{
    helix_engine::storage_core::storage_methods::StorageMethods, protocol::value::Value,
};
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

mod common;

#[tokio::test]
async fn graph_pipeline_updates_delta_and_helix() -> anyhow::Result<()> {
    let ctx = common::init_test_context().await?;

    let mut graph_data = fstorage::fetch::GraphData::new();
    graph_data.add_entities(vec![Project {
        url: Some("https://github.com/example/repo".to_string()),
        name: Some("example".to_string()),
        description: Some("Graph pipeline smoke test".to_string()),
        language: Some("Rust".to_string()),
        stars: Some(42),
        forks: Some(7),
    }]);

    ctx.synchronizer.process_graph_data(graph_data).await?;

    let table_path = ctx.config.lake_path.join(Project::table_name());
    let table_url =
        Url::from_file_path(&table_path).map_err(|_| anyhow::anyhow!("non-UTF8 table path"))?;
    let table = open_table(table_url).await?;
    assert_eq!(table.version(), Some(0));
    assert_eq!(table.get_file_uris()?.count(), 1);

    let node_id = utils::id::stable_node_id_u128(
        Project::ENTITY_TYPE,
        &[("url", "https://github.com/example/repo".to_string())],
    );
    let node_uuid = Uuid::from_u128(node_id);

    let txn = ctx.engine.storage.graph_env.read_txn()?;
    let node = ctx.engine.storage.get_node(&txn, &node_id)?;
    assert_eq!(node.label, Project::ENTITY_TYPE);
    assert!(matches!(
        node.properties.as_ref().and_then(|props| props.get("name")),
        Some(Value::String(value)) if value == "example"
    ));

    assert!(
        ctx.engine.storage.get_node(&txn, &node_id).is_ok(),
        "node {} should exist in Helix",
        node_uuid
    );

    let index_table_path = ctx
        .config
        .lake_path
        .join(format!("silver/index/{}", Project::ENTITY_TYPE));
    assert!(
        index_table_path.exists(),
        "index table directory should exist"
    );
    let index_url = Url::from_file_path(&index_table_path)
        .map_err(|_| anyhow::anyhow!("non-UTF8 index path"))?;
    let index_table = open_table(index_url).await?;
    assert_eq!(index_table.version(), Some(0));

    let ctx_df = SessionContext::new();
    ctx_df.register_table("index_table", Arc::new(index_table))?;
    let df = ctx_df.sql("SELECT id, url FROM index_table").await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let id_column = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("id column should be utf8");
    assert_eq!(id_column.value(0), node_uuid.to_string());

    let url_column = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("url column should be utf8");
    assert_eq!(url_column.value(0), "https://github.com/example/repo");

    Ok(())
}
