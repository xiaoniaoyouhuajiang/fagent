use fstorage::{
    fetch::{Fetchable, GraphData},
    schemas::generated_schemas::{Commit, HasVersion, IsCommit, Project, Version},
    sync::DataSynchronizer,
    utils,
};
use uuid::Uuid;

mod common;

#[tokio::test]
async fn shortest_path_traverses_linked_nodes() -> anyhow::Result<()> {
    let ctx = common::init_test_context().await?;

    let project_url = "https://example.com/repo";
    let version_sha = "v1.0.0";
    let commit_sha = "c0ffee";

    let project_id =
        utils::id::stable_node_id_u128(Project::ENTITY_TYPE, &[("url", project_url.to_string())]);
    let version_id =
        utils::id::stable_node_id_u128(Version::ENTITY_TYPE, &[("sha", version_sha.to_string())]);
    let commit_id =
        utils::id::stable_node_id_u128(Commit::ENTITY_TYPE, &[("sha", commit_sha.to_string())]);

    let project_uuid = Uuid::from_u128(project_id).to_string();
    let version_uuid = Uuid::from_u128(version_id).to_string();
    let commit_uuid = Uuid::from_u128(commit_id).to_string();

    let mut graph = GraphData::new();
    graph.add_entities(vec![Project {
        url: Some(project_url.to_string()),
        name: Some("demo-repo".to_string()),
        description: None,
        language: Some("Rust".to_string()),
        stars: Some(42),
        forks: Some(7),
    }]);
    graph.add_entities(vec![Version {
        sha: Some(version_sha.to_string()),
        tag: Some("v1.0.0".to_string()),
        is_head: Some(true),
        created_at: None,
    }]);
    graph.add_entities(vec![Commit {
        sha: Some(commit_sha.to_string()),
        message: Some("Initial commit".to_string()),
        committed_at: None,
    }]);

    let has_version_id =
        utils::id::stable_edge_id_u128(HasVersion::ENTITY_TYPE, &project_uuid, &version_uuid);
    let is_commit_id =
        utils::id::stable_edge_id_u128(IsCommit::ENTITY_TYPE, &version_uuid, &commit_uuid);

    graph.add_entities(vec![HasVersion {
        id: Some(Uuid::from_u128(has_version_id).to_string()),
        from_node_id: Some(project_uuid.clone()),
        to_node_id: Some(version_uuid.clone()),
        from_node_type: Some("project".to_string()),
        to_node_type: Some("version".to_string()),
        created_at: None,
        updated_at: None,
    }]);
    graph.add_entities(vec![IsCommit {
        id: Some(Uuid::from_u128(is_commit_id).to_string()),
        from_node_id: Some(version_uuid.clone()),
        to_node_id: Some(commit_uuid.clone()),
        from_node_type: Some("version".to_string()),
        to_node_type: Some("commit".to_string()),
        created_at: None,
        updated_at: None,
    }]);

    ctx.synchronizer.process_graph_data(graph).await?;

    let path = ctx
        .lake
        .shortest_path(&project_uuid, &commit_uuid, None)
        .await?
        .expect("expected path between project and commit");

    assert_eq!(path.length, 2);
    assert_eq!(path.nodes.len(), 3);
    assert_eq!(path.edges.len(), 2);
    assert_eq!(
        path.nodes
            .first()
            .and_then(|node| node.get("id"))
            .and_then(|value| value.as_str()),
        Some(project_uuid.as_str())
    );
    assert_eq!(
        path.nodes
            .last()
            .and_then(|node| node.get("id"))
            .and_then(|value| value.as_str()),
        Some(commit_uuid.as_str())
    );

    let constrained = ctx
        .lake
        .shortest_path(&project_uuid, &commit_uuid, Some("edge_containscontent"))
        .await?;
    assert!(
        constrained.is_none(),
        "unexpected path when filtering by unrelated edge label"
    );

    Ok(())
}
