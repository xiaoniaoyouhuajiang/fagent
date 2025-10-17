use chrono::Utc;
use fstorage::{
    fetch::{Fetchable, GraphData},
    schemas::generated_schemas::{Calls, Function},
    sync::DataSynchronizer,
    utils,
};
use uuid::Uuid;

mod common;

#[tokio::test]
async fn helix_and_delta_edge_queries_both_succeed() -> anyhow::Result<()> {
    let ctx = common::init_test_context().await?;

    // Prepare two function nodes that will be connected via Helix-managed edges.
    let from_name = "function::main";
    let to_name = "function::helper";
    let version_sha = "version-sha-2";
    let from_file_path = "src/main.rs";
    let to_file_path = "src/helper.rs";
    let from_node_id = utils::id::stable_node_id_u128(
        Function::ENTITY_TYPE,
        &[
            ("version_sha", version_sha.to_string()),
            ("file_path", from_file_path.to_string()),
            ("name", from_name.to_string()),
        ],
    );
    let to_node_id = utils::id::stable_node_id_u128(
        Function::ENTITY_TYPE,
        &[
            ("version_sha", version_sha.to_string()),
            ("file_path", to_file_path.to_string()),
            ("name", to_name.to_string()),
        ],
    );

    let mut node_data = GraphData::new();
    node_data.add_entities(vec![
        Function {
            version_sha: Some(version_sha.to_string()),
            file_path: Some(from_file_path.to_string()),
            name: Some(from_name.to_string()),
            signature: Some("fn main()".to_string()),
            start_line: Some(1),
            end_line: Some(10),
            is_component: Some(true),
        },
        Function {
            version_sha: Some(version_sha.to_string()),
            file_path: Some(to_file_path.to_string()),
            name: Some(to_name.to_string()),
            signature: Some("fn helper()".to_string()),
            start_line: Some(11),
            end_line: Some(20),
            is_component: Some(false),
        },
    ]);
    ctx.synchronizer.process_graph_data(node_data).await?;

    // Insert a Helix-managed edge with stable UUIDs.
    let edge_uuid = utils::id::stable_edge_id_u128(
        Calls::ENTITY_TYPE,
        &Uuid::from_u128(from_node_id).to_string(),
        &Uuid::from_u128(to_node_id).to_string(),
    );
    let mut helix_edges = GraphData::new();
    helix_edges.add_entities(vec![Calls {
        id: Some(Uuid::from_u128(edge_uuid).to_string()),
        from_node_id: Some(Uuid::from_u128(from_node_id).to_string()),
        to_node_id: Some(Uuid::from_u128(to_node_id).to_string()),
        from_node_type: Some("FUNCTION".to_string()),
        to_node_type: Some("FUNCTION".to_string()),
        created_at: Some(Utc::now()),
        updated_at: Some(Utc::now()),
    }]);
    ctx.synchronizer.process_graph_data(helix_edges).await?;

    // Helix path: the node ID is a UUID, so Helix should answer directly.
    let to_uuid_string = Uuid::from_u128(to_node_id).to_string();

    let helix_results = ctx
        .lake
        .get_out_edges(
            &Uuid::from_u128(from_node_id).to_string(),
            Some(Calls::ENTITY_TYPE),
        )
        .await?;
    assert_eq!(helix_results.len(), 1);
    assert_eq!(
        helix_results[0]
            .get("to_node_id")
            .and_then(|value| value.as_str()),
        Some(to_uuid_string.as_str())
    );

    // Delta fallback path: use non-UUID identifiers that force the query to read from Delta Lake.
    let legacy_edges = vec![Calls {
        id: Some("edge-calls-legacy".to_string()),
        from_node_id: Some("function-main".to_string()),
        to_node_id: Some("function-helper".to_string()),
        from_node_type: Some("FUNCTION".to_string()),
        to_node_type: Some("FUNCTION".to_string()),
        created_at: Some(Utc::now()),
        updated_at: Some(Utc::now()),
    }];
    ctx.lake.write_edges("calls", legacy_edges).await?;

    let fallback_results = ctx
        .lake
        .get_out_edges("function-main", Some("calls"))
        .await?;
    assert_eq!(fallback_results.len(), 1);
    assert_eq!(
        fallback_results[0]
            .get("to_node_id")
            .and_then(|value| value.as_str()),
        Some("function-helper")
    );

    {
        let mut txn = ctx.engine.storage.graph_env.write_txn()?;
        ctx.engine.storage.nodes_db.delete(&mut txn, &to_node_id)?;
        txn.commit()?;
    }

    let node_from_cold = ctx
        .lake
        .get_node_by_id(&to_uuid_string, Some(Function::ENTITY_TYPE))
        .await?
        .expect("node should be retrievable via index");
    assert_eq!(
        node_from_cold.get("name").and_then(|value| value.as_str()),
        Some(to_name)
    );

    let stats = ctx.lake.get_edge_statistics().await?;
    assert!(
        stats.contains_key("calls"),
        "expected statistics to contain entry for 'calls'"
    );

    Ok(())
}
