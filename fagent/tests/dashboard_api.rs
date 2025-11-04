use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use fagent::{build_router, AppState};
use fstorage::{
    config::StorageConfig,
    fetch::{Fetchable, GraphData},
    schemas::generated_schemas::{Commit, HasVersion, IsCommit, Project, Version},
    sync::DataSynchronizer,
    utils, FStorage,
};
use serde_json::Value;
use tempfile::tempdir;
use tower::util::ServiceExt;
use uuid::Uuid;

const BODY_LIMIT: usize = 1 << 20;

async fn test_app() -> anyhow::Result<(axum::Router, tempfile::TempDir)> {
    let dir = tempdir()?;
    let config = StorageConfig::new(dir.path());
    let storage = Arc::new(FStorage::new(config).await?);
    let app = build_router(AppState::new(storage));
    Ok((app, dir))
}

#[tokio::test]
async fn fetchers_endpoint_returns_empty_list() -> anyhow::Result<()> {
    let (app, _dir) = test_app().await?;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/fetchers")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), BODY_LIMIT).await?;
    let value: Value = serde_json::from_slice(&body)?;
    assert!(value.as_array().map(|arr| arr.is_empty()).unwrap_or(false));
    Ok(())
}

#[tokio::test]
async fn status_endpoint_reports_counts() -> anyhow::Result<()> {
    let (app, _dir) = test_app().await?;
    let response = app
        .oneshot(Request::builder().uri("/api/status").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), BODY_LIMIT).await?;
    let value: Value = serde_json::from_slice(&body)?;
    assert!(value.get("db_stats").is_some());
    let count = value
        .get("entity_count")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    assert_eq!(count, 0);
    Ok(())
}

#[tokio::test]
async fn readiness_endpoint_accepts_empty_payload() -> anyhow::Result<()> {
    let (app, _dir) = test_app().await?;
    let request = Request::builder()
        .method("POST")
        .uri("/api/readiness")
        .header("content-type", "application/json")
        .body(Body::from("[]"))?;
    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), BODY_LIMIT).await?;
    let value: Value = serde_json::from_slice(&body)?;
    assert!(value.as_object().map(|o| o.is_empty()).unwrap_or(false));
    Ok(())
}

#[tokio::test]
async fn shortest_path_endpoint_reports_paths() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let config = StorageConfig::new(dir.path());
    let storage = Arc::new(FStorage::new(config).await?);

    let project_url = "https://example.com/api";
    let version_sha = "v2.0.0";
    let commit_sha = "deadbeef";

    let project_id =
        utils::id::stable_node_id_u128(Project::ENTITY_TYPE, &[("url", project_url.to_string())]);
    let version_id =
        utils::id::stable_node_id_u128(Version::ENTITY_TYPE, &[("sha", version_sha.to_string())]);
    let commit_id =
        utils::id::stable_node_id_u128(Commit::ENTITY_TYPE, &[("sha", commit_sha.to_string())]);

    let project_uuid = Uuid::from_u128(project_id).to_string();
    let version_uuid = Uuid::from_u128(version_id).to_string();
    let commit_uuid = Uuid::from_u128(commit_id).to_string();

    let has_version_id =
        utils::id::stable_edge_id_u128(HasVersion::ENTITY_TYPE, &project_uuid, &version_uuid);
    let is_commit_id =
        utils::id::stable_edge_id_u128(IsCommit::ENTITY_TYPE, &version_uuid, &commit_uuid);

    let mut graph = GraphData::new();
    graph.add_entities(vec![Project {
        url: Some(project_url.to_string()),
        name: Some("api-service".to_string()),
        description: None,
        language: Some("Rust".to_string()),
        stars: Some(101),
        forks: Some(12),
    }]);
    graph.add_entities(vec![Version {
        sha: Some(version_sha.to_string()),
        tag: Some("v2.0.0".to_string()),
        is_head: Some(false),
        created_at: None,
    }]);
    graph.add_entities(vec![Commit {
        sha: Some(commit_sha.to_string()),
        message: Some("Add shortest path endpoint".to_string()),
        committed_at: None,
    }]);
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

    storage.synchronizer.process_graph_data(graph).await?;

    let router = build_router(AppState::new(storage.clone()));
    let request = Request::builder()
        .uri(format!(
            "/api/graph/shortest_path?from_id={}&to_id={}",
            project_uuid, commit_uuid
        ))
        .body(Body::empty())?;
    let response = router.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), BODY_LIMIT).await?;
    let value: Value = serde_json::from_slice(&body)?;
    assert!(value.get("found").and_then(Value::as_bool).unwrap_or(false));
    assert_eq!(
        value
            .get("length")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        2
    );
    assert_eq!(
        value
            .get("nodes")
            .and_then(|node| node.as_array())
            .map(|arr| arr.len())
            .unwrap_or_default(),
        3
    );
    assert_eq!(
        value
            .get("edges")
            .and_then(|edge| edge.as_array())
            .map(|arr| arr.len())
            .unwrap_or_default(),
        2
    );

    let router = build_router(AppState::new(storage));
    let filtered_request = Request::builder()
        .uri(format!(
            "/api/graph/shortest_path?from_id={}&to_id={}&edge_label=edge_containscontent",
            project_uuid, commit_uuid
        ))
        .body(Body::empty())?;
    let filtered_response = router.oneshot(filtered_request).await?;
    assert_eq!(filtered_response.status(), StatusCode::OK);
    let filtered_body = to_bytes(filtered_response.into_body(), BODY_LIMIT).await?;
    let filtered_value: Value = serde_json::from_slice(&filtered_body)?;
    assert!(
        !filtered_value
            .get("found")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        "expected no path when filtering by unrelated edge label"
    );

    Ok(())
}
