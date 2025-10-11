use deltalake::open_table;
use fstorage::{
    fetch::Fetchable,
    schemas::generated_schemas::Project,
    sync::DataSynchronizer,
    utils,
};
use helix_db::{
    helix_engine::storage_core::storage_methods::StorageMethods,
    protocol::value::Value,
};
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

    let table_uri = ctx
        .config
        .lake_path
        .join(Project::table_name())
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("non-UTF8 table path"))?
        .to_string();
    let table = open_table(table_uri).await?;
    assert_eq!(table.version(), 0);
    assert_eq!(table.get_file_uris().into_iter().count(), 1);

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

    Ok(())
}
