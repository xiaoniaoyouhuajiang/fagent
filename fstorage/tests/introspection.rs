use std::sync::Arc;

use fstorage::{
    config::StorageConfig,
    fetch::{
        FetchResponse, Fetchable, Fetcher, FetcherCapability, GraphData, ProbeReport,
        ProducedDataset,
    },
    models::{EntityIdentifier, SyncBudget, SyncContext},
    schemas::generated_schemas::Function,
    sync::DataSynchronizer,
    FStorage,
};
use serde_json::json;
use tempfile::tempdir;
use tokio::sync::Mutex;

struct MockFetcher {
    remote_anchor: Mutex<String>,
}

impl MockFetcher {
    const NAME: &'static str = "introspection_mock";

    fn new(initial_anchor: &str) -> Self {
        Self {
            remote_anchor: Mutex::new(initial_anchor.to_string()),
        }
    }

    async fn set_anchor(&self, value: &str) {
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
            description: "Mock fetcher used for introspection tests",
            param_schema: json!({"type": "object"}),
            produces: vec![ProducedDataset {
                kind: "node",
                name: Function::ENTITY_TYPE.to_string(),
                table_path: Function::table_name(),
                primary_keys: vec!["name".to_string()],
            }],
            default_ttl_secs: Some(900),
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
            estimated_missing: None,
            rate_limit_left: Some(42),
            reason: None,
        })
    }

    async fn fetch(
        &self,
        _params: serde_json::Value,
        _embedding_provider: Arc<dyn fstorage::embedding::EmbeddingProvider>,
    ) -> fstorage::errors::Result<FetchResponse> {
        let mut graph = GraphData::new();
        graph.add_entities(vec![Function {
            name: Some("function::introspect".to_string()),
            signature: Some("fn introspect()".to_string()),
            start_line: Some(1),
            end_line: Some(10),
            is_component: Some(true),
        }]);
        Ok(FetchResponse::GraphData(graph))
    }
}

#[tokio::test]
async fn storage_introspection_reports_capabilities_and_tables() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let config = StorageConfig::new(dir.path());
    let storage = FStorage::new(config.clone()).await?;

    let fetcher = Arc::new(MockFetcher::new("sha-alpha"));
    storage.register_fetcher(Arc::clone(&fetcher) as Arc<dyn Fetcher>);

    // Introspection should include the registered fetcher capability.
    let capabilities = storage.list_fetchers_capability();
    assert!(
        capabilities.iter().any(|cap| cap.name == MockFetcher::NAME),
        "capabilities should include the mock fetcher"
    );

    // Exercise the data path to create tables and ingestion offsets.
    let mut graph = GraphData::new();
    graph.add_entities(vec![Function {
        name: Some("function::a".to_string()),
        signature: Some("fn a()".to_string()),
        start_line: Some(1),
        end_line: Some(5),
        is_component: Some(false),
    }]);
    storage.synchronizer.process_graph_data(graph).await?;

    // Known entities should now include the Function dataset.
    let entities = storage.list_known_entities()?;
    assert!(
        entities
            .iter()
            .any(|entity| entity.table_path.contains(Function::ENTITY_TYPE)),
        "known entities should include Function ingestion metadata"
    );

    // Table listing should pick up both entity table and index table.
    let entity_tables = storage.list_tables("silver/entities").await?;
    assert!(
        entity_tables
            .iter()
            .any(|table| table.table_path.ends_with(Function::ENTITY_TYPE)),
        "entity tables should include Function"
    );

    let index_tables = storage.list_tables("silver/index").await?;
    assert!(
        index_tables
            .iter()
            .any(|table| table.table_path.ends_with(Function::ENTITY_TYPE)),
        "index tables should include Function"
    );

    // Readiness introspection should run the probe.
    let readiness = storage
        .get_readiness(&[EntityIdentifier {
            uri: "repo://example".to_string(),
            entity_type: Function::ENTITY_TYPE.to_string(),
            fetcher_name: Some(MockFetcher::NAME.to_string()),
            params: Some(json!({"repo": "example"})),
            anchor_key: Some("head".to_string()),
        }])
        .await?;
    let report = readiness
        .get("repo://example")
        .expect("readiness report is missing");
    assert!(
        !report.is_fresh,
        "initial readiness should be stale because anchors diverge"
    );

    // After sync, readiness should become fresh.
    storage
        .synchronizer
        .sync(
            MockFetcher::NAME,
            json!({"repo": "example"}),
            SyncContext {
                triggering_query: None,
                target_entities: vec![EntityIdentifier {
                    uri: "repo://example".to_string(),
                    entity_type: Function::ENTITY_TYPE.to_string(),
                    fetcher_name: Some(MockFetcher::NAME.to_string()),
                    params: Some(json!({"repo": "example"})),
                    anchor_key: Some("head".to_string()),
                }],
            },
            SyncBudget::ByRequestCount(1),
        )
        .await?;

    fetcher.set_anchor("sha-beta").await;

    let readiness_after = storage
        .get_readiness(&[EntityIdentifier {
            uri: "repo://example".to_string(),
            entity_type: Function::ENTITY_TYPE.to_string(),
            fetcher_name: Some(MockFetcher::NAME.to_string()),
            params: Some(json!({"repo": "example"})),
            anchor_key: Some("head".to_string()),
        }])
        .await?;
    let updated = readiness_after
        .get("repo://example")
        .expect("readiness report is missing after sync");
    assert!(
        !updated.is_fresh,
        "readiness should flip back to stale after remote anchor advances"
    );

    Ok(())
}
