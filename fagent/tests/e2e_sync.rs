//! End-to-end sync skeleton test.
//!
//! This test is intentionally ignored by default because it requires network
//! access plus real GitHub credentials. It provides the scaffolding for a full
//! pipeline verification:
//!   1. Spin up an ephemeral fstorage instance behind the fagent HTTP server.
//!   2. Invoke the public `/api/sync` endpoint with real gitfetcher params.
//!   3. Assert (TODO) that freshly written Delta + Helix layers contain the
//!      expected entities and remain consistent.
//! Follow-up work:
//!   * Introduce helpers that cross-check hot/cold row counts for each entity.
//!   * Promote GitHub + proxy environment discovery into reusable fixtures.
//!   * Extend sync verification to inspect vector + edge materialization.

use std::{io::ErrorKind, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use dotenvy::dotenv;
use fagent::{build_router, AppState};
use fstorage::{config::StorageConfig, fetch::Fetcher, FStorage};
use gitfetcher::GitFetcher;
use serde_json::json;
use tempfile::tempdir;
use tokio::{net::TcpListener, sync::oneshot, time::sleep};

struct TestServer {
    base_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    _tmp_dir: tempfile::TempDir,
    storage: Arc<FStorage>,
}

impl TestServer {
    async fn spawn() -> Result<Self> {
        let tmp_dir = tempdir().context("failed to create tempdir for fstorage")?;
        let config = StorageConfig::new(tmp_dir.path());
        let storage = Arc::new(
            FStorage::new(config)
                .await
                .context("failed to init fstorage")?,
        );
        let github_token = std::env::var("GITHUB_TOKEN")
            .context("GITHUB_TOKEN must be set before registering gitfetcher")?;
        let fetcher = GitFetcher::with_default_client(Some(github_token))
            .context("failed to initialize gitfetcher")?;
        storage.register_fetcher(Arc::new(fetcher) as Arc<dyn Fetcher>);
        let app = build_router(AppState::new(storage.clone()));

        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .context("failed to bind test listener")?;
        let addr = listener.local_addr().context("missing local addr")?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let server = axum::serve(listener, app).with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            });
            if let Err(err) = server.await {
                tracing::error!("test server error: {err:?}");
            }
        });

        Ok(Self {
            base_addr: addr,
            shutdown_tx: Some(shutdown_tx),
            _tmp_dir: tmp_dir,
            storage,
        })
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.base_addr)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

async fn wait_for_server(addr: SocketAddr) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("failed to build readiness client")?;
    let url = format!("http://{addr}/api/status");
    for _ in 0..20 {
        if let Ok(response) = client.get(&url).send().await {
            if response.status().is_success() {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(250)).await;
    }
    anyhow::bail!("server did not become ready on {addr}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires network access, GitHub token, and long-running sync pipeline"]
async fn end_to_end_repo_snapshot_sync() -> Result<()> {
    if let Err(err) = dotenv() {
        if !matches!(err, dotenvy::Error::Io(ref io_err) if io_err.kind() == ErrorKind::NotFound) {
            anyhow::bail!("failed to load .env file: {err}");
        }
    }

    let github_token = std::env::var("GITHUB_TOKEN")
        .context("GITHUB_TOKEN must be provided to run the e2e sync test")?;

    if let Ok(http_proxy) = std::env::var("http_proxy") {
        std::env::set_var("http_proxy", http_proxy);
    }
    if let Ok(https_proxy) = std::env::var("https_proxy") {
        std::env::set_var("https_proxy", https_proxy);
    }
    std::env::set_var("GITHUB_TOKEN", github_token);

    let server = TestServer::spawn().await?;
    wait_for_server(server.base_addr).await?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(600))
        .build()
        .context("failed to build reqwest client")?;

    let request_body = json!({
        "fetcher": "gitfetcher",
        "params": {
            "mode": "repo_snapshot",
            "repo": "talent-plan/tinykv",
            "include_code": true
        }
    });

    let response = client
        .post(format!("{}/api/sync", server.base_url()))
        .json(&request_body)
        .send()
        .await
        .context("failed to invoke sync endpoint")?;

    let status = response.status();
    let body_text = response.text().await.unwrap_or_default();
    anyhow::ensure!(
        status.is_success(),
        "sync endpoint returned {status}, body: {body_text}"
    );

    sleep(Duration::from_secs(5)).await;

    // Placeholder: verify cold (Delta) and hot (Helix) layers converge.
    // TODO: add concrete assertions comparing lake and engine state once helper APIs are available.
    assert!(
        server
            .storage
            .list_known_entities()
            .expect("list entities succeeds")
            .len()
            >= 1,
        "expected at least one entity to be registered after sync"
    );

    drop(server);
    Ok(())
}
