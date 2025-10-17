use chrono::Utc;
use fstorage::{
    fetch::{Fetchable, GraphData},
    lake::{NeighborDirection, NeighborEdgeOrientation},
    schemas::generated_schemas::{Calls, Function},
    sync::DataSynchronizer,
    utils,
};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use uuid::Uuid;

mod common;

fn get_scalar<'a>(map: &'a HashMap<String, JsonValue>, key: &str) -> Option<&'a str> {
    map.get(key).and_then(|value| value.as_str()).or_else(|| {
        map.get("properties")
            .and_then(|value| value.as_object())
            .and_then(|props| props.get(key))
            .and_then(|value| value.as_str())
    })
}

#[tokio::test]
async fn query_api_covers_hot_and_cold_paths() -> anyhow::Result<()> {
    let ctx = common::init_test_context().await?;

    let function_a = "function::root";
    let function_b = "function::child";
    let function_c = "function::leaf";
    let version_sha = "version-sha-1";
    let file_path_a = "src/root.rs";
    let file_path_b = "src/child.rs";
    let file_path_c = "src/leaf.rs";

    let node_a_id = utils::id::stable_node_id_u128(
        Function::ENTITY_TYPE,
        &[
            ("version_sha", version_sha.to_string()),
            ("file_path", file_path_a.to_string()),
            ("name", function_a.to_string()),
        ],
    );
    let node_b_id = utils::id::stable_node_id_u128(
        Function::ENTITY_TYPE,
        &[
            ("version_sha", version_sha.to_string()),
            ("file_path", file_path_b.to_string()),
            ("name", function_b.to_string()),
        ],
    );
    let node_c_id = utils::id::stable_node_id_u128(
        Function::ENTITY_TYPE,
        &[
            ("version_sha", version_sha.to_string()),
            ("file_path", file_path_c.to_string()),
            ("name", function_c.to_string()),
        ],
    );

    let mut node_data = GraphData::new();
    node_data.add_entities(vec![
        Function {
            version_sha: Some(version_sha.to_string()),
            file_path: Some(file_path_a.to_string()),
            name: Some(function_a.to_string()),
            signature: Some("fn root()".to_string()),
            start_line: Some(1),
            end_line: Some(10),
            is_component: Some(true),
        },
        Function {
            version_sha: Some(version_sha.to_string()),
            file_path: Some(file_path_b.to_string()),
            name: Some(function_b.to_string()),
            signature: Some("fn child()".to_string()),
            start_line: Some(11),
            end_line: Some(20),
            is_component: Some(false),
        },
        Function {
            version_sha: Some(version_sha.to_string()),
            file_path: Some(file_path_c.to_string()),
            name: Some(function_c.to_string()),
            signature: Some("fn leaf()".to_string()),
            start_line: Some(21),
            end_line: Some(30),
            is_component: Some(false),
        },
    ]);
    ctx.synchronizer.process_graph_data(node_data).await?;

    let edge_ab_id = utils::id::stable_edge_id_u128(
        Calls::ENTITY_TYPE,
        &Uuid::from_u128(node_a_id).to_string(),
        &Uuid::from_u128(node_b_id).to_string(),
    );
    let edge_bc_id = utils::id::stable_edge_id_u128(
        Calls::ENTITY_TYPE,
        &Uuid::from_u128(node_b_id).to_string(),
        &Uuid::from_u128(node_c_id).to_string(),
    );

    let mut edge_data = GraphData::new();
    edge_data.add_entities(vec![
        Calls {
            id: Some(Uuid::from_u128(edge_ab_id).to_string()),
            from_node_id: Some(Uuid::from_u128(node_a_id).to_string()),
            to_node_id: Some(Uuid::from_u128(node_b_id).to_string()),
            from_node_type: Some("FUNCTION".to_string()),
            to_node_type: Some("FUNCTION".to_string()),
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
        },
        Calls {
            id: Some(Uuid::from_u128(edge_bc_id).to_string()),
            from_node_id: Some(Uuid::from_u128(node_b_id).to_string()),
            to_node_id: Some(Uuid::from_u128(node_c_id).to_string()),
            from_node_type: Some("FUNCTION".to_string()),
            to_node_type: Some("FUNCTION".to_string()),
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
        },
    ]);
    ctx.synchronizer.process_graph_data(edge_data).await?;

    let node_a_uuid = Uuid::from_u128(node_a_id).to_string();
    let node_b_uuid = Uuid::from_u128(node_b_id).to_string();

    let hot_node = ctx
        .lake
        .get_node_by_id(&node_a_uuid, Some(Function::ENTITY_TYPE))
        .await?
        .expect("hot node should exist");
    assert_eq!(get_scalar(&hot_node, "name"), Some(function_a));

    let node_from_keys = ctx
        .lake
        .get_node_by_keys(
            Function::ENTITY_TYPE,
            &[
                ("version_sha", version_sha),
                ("file_path", file_path_b),
                ("name", function_b),
            ],
        )
        .await?
        .expect("node fetched by keys");
    assert_eq!(get_scalar(&node_from_keys, "name"), Some(function_b));

    let edge_types = [Calls::ENTITY_TYPE];
    let outgoing = ctx
        .lake
        .neighbors(
            &node_a_uuid,
            Some(&edge_types),
            NeighborDirection::Outgoing,
            10,
        )
        .await?;
    assert_eq!(outgoing.len(), 1);
    assert_eq!(outgoing[0].orientation, NeighborEdgeOrientation::Outgoing);
    assert_eq!(outgoing[0].node_id, Uuid::from_u128(node_b_id).to_string());
    assert_eq!(
        outgoing[0]
            .node
            .as_ref()
            .and_then(|node| get_scalar(node, "name")),
        Some(function_b)
    );

    let incoming = ctx
        .lake
        .neighbors(
            &node_b_uuid,
            Some(&edge_types),
            NeighborDirection::Incoming,
            10,
        )
        .await?;
    assert_eq!(incoming.len(), 1);
    assert_eq!(incoming[0].orientation, NeighborEdgeOrientation::Incoming);
    assert_eq!(incoming[0].node_id, Uuid::from_u128(node_a_id).to_string());
    assert_eq!(
        incoming[0]
            .node
            .as_ref()
            .and_then(|node| get_scalar(node, "name")),
        Some(function_a),
        "incoming = {:?}",
        incoming
    );

    let limited = ctx
        .lake
        .neighbors(&node_b_uuid, Some(&edge_types), NeighborDirection::Both, 1)
        .await?;
    assert_eq!(limited.len(), 1);

    let subgraph = ctx
        .lake
        .subgraph_bfs(&node_a_uuid, Some(&edge_types), 2, 0, 0)
        .await?;
    let node_ids: Vec<String> = subgraph
        .nodes
        .iter()
        .filter_map(|node| {
            node.get("id")
                .and_then(|value| value.as_str())
                .map(|s| s.to_string())
        })
        .collect();
    assert!(node_ids.contains(&node_a_uuid));
    assert!(node_ids.contains(&node_b_uuid));
    assert!(node_ids.contains(&Uuid::from_u128(node_c_id).to_string()));
    assert!(
        subgraph.edges.len() >= 2,
        "expected BFS to include both edges"
    );

    let constrained = ctx
        .lake
        .subgraph_bfs(&node_a_uuid, Some(&edge_types), 2, 2, 1)
        .await?;
    assert!(
        constrained.nodes.len() <= 2,
        "node limit should constrain BFS result"
    );
    assert!(
        constrained.edges.len() <= 1,
        "edge limit should constrain BFS result"
    );

    let table_name = Function::table_name();
    let table_rows = ctx
        .lake
        .query_table(&table_name, Some(&[("name", function_c)]), Some(1))
        .await?;
    assert_eq!(table_rows.len(), 1);
    assert_eq!(get_scalar(&table_rows[0], "name"), Some(function_c));

    let sql_rows = ctx
        .lake
        .table_sql(
            &table_name,
            "SELECT name FROM {{table}} WHERE name LIKE 'function::%' ORDER BY name",
        )
        .await?;
    assert_eq!(sql_rows.len(), 3);

    {
        let mut txn = ctx.engine.storage.graph_env.write_txn()?;
        ctx.engine.storage.nodes_db.delete(&mut txn, &node_b_id)?;
        txn.commit()?;
    }

    let cold_node = ctx
        .lake
        .get_node_by_id(&node_b_uuid, Some(Function::ENTITY_TYPE))
        .await?
        .expect("cold node should be resolved via index");
    assert_eq!(get_scalar(&cold_node, "name"), Some(function_b));

    let node_from_keys_cold = ctx
        .lake
        .get_node_by_keys(
            Function::ENTITY_TYPE,
            &[
                ("version_sha", version_sha),
                ("file_path", file_path_b),
                ("name", function_b),
            ],
        )
        .await?
        .expect("node by keys should work via lake");
    assert_eq!(get_scalar(&node_from_keys_cold, "name"), Some(function_b));

    Ok(())
}
