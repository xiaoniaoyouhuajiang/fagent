use std::sync::Arc;

use fstorage::{
    fetch::Fetchable,
    fetch::{FetchResponse, Fetcher, FetcherCapability, GraphData, ProbeReport, ProducedDataset},
    models::{EntityIdentifier, SyncBudget, SyncContext},
    schemas::generated_schemas::Function,
    sync::DataSynchronizer,
};
use serde_json::json;
use tokio::sync::Mutex;

mod common;

struct MockFetcher {
    remote_anchor: Mutex<String>,
}

impl MockFetcher {
    const NAME: &'static str = "mock_fetcher";

    fn new(initial_anchor: &str) -> Self {
        Self {
            remote_anchor: Mutex::new(initial_anchor.to_string()),
        }
    }

    async fn set_remote_anchor(&self, value: &str) {
        let mut guard = self.remote_anchor.lock().await;
        *guard = value.to_string();
    }

    async fn current_anchor(&self) -> String {
        self.remote_anchor.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl Fetcher for MockFetcher {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn capability(&self) -> FetcherCapability {
        FetcherCapability {
            name: Self::NAME,
            description: "Mock fetcher for readiness probe tests",
            param_schema: json!({"type": "object"}),
            produces: vec![ProducedDataset {
                kind: "node",
                name: Function::ENTITY_TYPE.to_string(),
                table_path: Function::table_name(),
                primary_keys: vec!["name".to_string()],
            }],
            default_ttl_secs: Some(3600),
            examples: vec![json!({"repo": "example/repo"})],
        }
    }

    async fn probe(&self, _params: serde_json::Value) -> fstorage::errors::Result<ProbeReport> {
        let anchor = self.current_anchor().await;
        Ok(ProbeReport {
            fresh: None,
            remote_anchor: Some(anchor),
            local_anchor: None,
            anchor_key: Some("head".to_string()),
            estimated_missing: Some(1),
            rate_limit_left: Some(100),
            reason: None,
        })
    }

    async fn fetch(
        &self,
        _params: serde_json::Value,
        _embedding_provider: Arc<dyn fstorage::embedding::EmbeddingProvider>,
    ) -> fstorage::errors::Result<FetchResponse> {
        let mut graph_data = GraphData::new();
        graph_data.add_entities(vec![Function {
            name: Some("function::mock".to_string()),
            signature: Some("fn mock()".to_string()),
            start_line: Some(1),
            end_line: Some(5),
            is_component: Some(false),
        }]);
        Ok(FetchResponse::GraphData(graph_data))
    }
}

#[tokio::test]
async fn readiness_probe_tracks_anchor_freshness() -> anyhow::Result<()> {
    let mut ctx = common::init_test_context().await?;
    let fetcher = Arc::new(MockFetcher::new("sha-initial"));
    ctx.synchronizer
        .register_fetcher(Arc::clone(&fetcher) as Arc<dyn Fetcher>);

    let entity = EntityIdentifier {
        uri: "repo::example".to_string(),
        entity_type: Function::ENTITY_TYPE.to_string(),
        fetcher_name: Some(MockFetcher::NAME.to_string()),
        params: Some(json!({"repo": "example"})),
        anchor_key: Some("head".to_string()),
    };

    let first = ctx.synchronizer.check_readiness(&[entity.clone()]).await?;
    let initial_report = first.get(&entity.uri).expect("readiness missing");
    assert!(
        !initial_report.is_fresh,
        "expected initial probe to mark entity stale"
    );
    assert_eq!(
        initial_report
            .probe_report
            .as_ref()
            .and_then(|r| r.remote_anchor.clone()),
        Some("sha-initial".to_string())
    );

    ctx.synchronizer
        .sync(
            MockFetcher::NAME,
            json!({"repo": "example"}),
            SyncContext {
                triggering_query: None,
                target_entities: vec![entity.clone()],
            },
            SyncBudget::ByRequestCount(1),
        )
        .await?;

    let second = ctx.synchronizer.check_readiness(&[entity.clone()]).await?;
    let post_sync = second.get(&entity.uri).expect("readiness missing");
    assert!(
        post_sync.is_fresh,
        "entity should become fresh after sync updates anchor"
    );

    fetcher.set_remote_anchor("sha-next").await;

    let third = ctx.synchronizer.check_readiness(&[entity.clone()]).await?;
    let after_anchor_change = third.get(&entity.uri).expect("readiness missing");
    assert!(
        !after_anchor_change.is_fresh,
        "changing remote anchor should mark entity stale again"
    );

    Ok(())
}
