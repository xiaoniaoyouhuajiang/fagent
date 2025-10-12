use fstorage::{
    fetch::{Fetchable, GraphData},
    schemas::generated_schemas::Project,
    sync::DataSynchronizer,
    utils,
};
use uuid::Uuid;

mod common;

#[tokio::test]
async fn node_index_allows_cold_path_lookup_by_id() -> anyhow::Result<()> {
    let ctx = common::init_test_context().await?;

    let mut graph_data = GraphData::new();
    graph_data.add_entities(vec![
        Project {
            url: Some("https://example.com/repo-a".to_string()),
            name: Some("repo-a".to_string()),
            description: None,
            language: Some("Rust".to_string()),
            stars: Some(10),
            forks: None,
        },
        Project {
            url: Some("https://example.com/repo-b".to_string()),
            name: Some("repo-b".to_string()),
            description: None,
            language: Some("Go".to_string()),
            stars: Some(5),
            forks: None,
        },
    ]);

    ctx.synchronizer.process_graph_data(graph_data).await?;

    let node_id = utils::id::stable_node_id_u128(
        Project::ENTITY_TYPE,
        &[("url", "https://example.com/repo-a".to_string())],
    );

    {
        let mut txn = ctx.engine.storage.graph_env.write_txn()?;
        ctx.engine.storage.nodes_db.delete(&mut txn, &node_id)?;
        txn.commit()?;
    }

    let node = ctx
        .lake
        .get_node_by_id(&Uuid::from_u128(node_id).to_string(), None)
        .await?
        .expect("node should be retrievable via index table");

    assert_eq!(
        node.get("url").and_then(|value| value.as_str()),
        Some("https://example.com/repo-a")
    );
    assert_eq!(
        node.get("name").and_then(|value| value.as_str()),
        Some("repo-a")
    );

    Ok(())
}
