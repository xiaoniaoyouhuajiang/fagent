use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use fagent::{build_router, AppState};
use fstorage::{config::StorageConfig, FStorage};
use serde_json::Value;
use tempfile::tempdir;
use tower::util::ServiceExt;

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
