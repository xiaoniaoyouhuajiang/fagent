use std::sync::Arc;

use async_trait::async_trait;
use fstorage::{
    embedding::EmbeddingProvider,
    errors::{Result as StorageResult, StorageError},
    fetch::{FetchResponse, Fetcher, FetcherCapability, Fetchable, ProducedDataset, ProbeReport},
};
use serde_json::json;

use crate::{
    client::{GitHubService, OctocrabService},
    mapper,
    models::RepoSnapshot,
    params::{FetcherParams, RepoSnapshotParams, SearchRepoParams},
};

fn edge_table_path(entity_type: &str) -> String {
    let suffix = entity_type
        .strip_prefix("edge_")
        .unwrap_or(entity_type)
        .to_lowercase();
    format!("silver/edges/{suffix}")
}

pub struct GitFetcher {
    client: Arc<dyn GitHubService>,
}

impl GitFetcher {
    pub fn new(client: Arc<dyn GitHubService>) -> Self {
        Self { client }
    }

    pub fn with_default_client(token: Option<String>) -> StorageResult<Self> {
        let client = OctocrabService::new(token).map_err(|err| {
            StorageError::Initialization(format!("failed to create Octocrab client: {err}"))
        })?;
        Ok(Self {
            client: Arc::new(client),
        })
    }

    fn capability_descriptor() -> FetcherCapability {
        let produces = vec![
            ProducedDataset {
                kind: "node",
                name: fstorage::schemas::generated_schemas::Project::ENTITY_TYPE.to_string(),
                table_path: fstorage::schemas::generated_schemas::Project::table_name(),
                primary_keys: fstorage::schemas::generated_schemas::Project::primary_keys()
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
            ProducedDataset {
                kind: "node",
                name: fstorage::schemas::generated_schemas::Version::ENTITY_TYPE.to_string(),
                table_path: fstorage::schemas::generated_schemas::Version::table_name(),
                primary_keys: fstorage::schemas::generated_schemas::Version::primary_keys()
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
            ProducedDataset {
                kind: "node",
                name: fstorage::schemas::generated_schemas::Commit::ENTITY_TYPE.to_string(),
                table_path: fstorage::schemas::generated_schemas::Commit::table_name(),
                primary_keys: fstorage::schemas::generated_schemas::Commit::primary_keys()
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
            ProducedDataset {
                kind: "edge",
                name: fstorage::schemas::generated_schemas::HasVersion::ENTITY_TYPE.to_string(),
                table_path: edge_table_path(
                    fstorage::schemas::generated_schemas::HasVersion::ENTITY_TYPE,
                ),
                primary_keys: fstorage::schemas::generated_schemas::HasVersion::primary_keys()
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
            ProducedDataset {
                kind: "edge",
                name: fstorage::schemas::generated_schemas::IsCommit::ENTITY_TYPE.to_string(),
                table_path: edge_table_path(
                    fstorage::schemas::generated_schemas::IsCommit::ENTITY_TYPE,
                ),
                primary_keys: fstorage::schemas::generated_schemas::IsCommit::primary_keys()
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
            ProducedDataset {
                kind: "panel",
                name: "github_search".to_string(),
                table_path: "silver/panel/github_search".to_string(),
                primary_keys: vec!["full_name".to_string()],
            },
        ];

        FetcherCapability {
            name: "gitfetcher",
            description: "Fetches GitHub repository snapshots and search panels",
            param_schema: json!({
                "type": "object",
                "required": ["mode"],
                "properties": {
                    "mode": { "enum": ["repo_snapshot", "search_repo"] },
                    "repo": { "type": "string", "description": "Repository in <owner>/<name> format" },
                    "rev": { "type": "string", "description": "Branch, tag, or commit SHA" },
                    "include_code": { "type": "boolean" },
                    "include_readme": { "type": "boolean" },
                    "query": { "type": "string" },
                    "language": { "type": "string" },
                    "min_stars": { "type": "integer" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 100 }
                },
                "oneOf": [
                    { "required": ["repo"] },
                    { "required": ["query"] }
                ]
            }),
            produces,
            default_ttl_secs: Some(6 * 3600),
            examples: vec![
                json!({"mode": "repo_snapshot", "repo": "rust-lang/rust", "include_code": false}),
                json!({"mode": "search_repo", "query": "language:rust compiler", "min_stars": 5000})
            ],
        }
    }

    fn parse_params(value: serde_json::Value) -> StorageResult<FetcherParams> {
        serde_json::from_value::<FetcherParams>(value)
            .map_err(|err| StorageError::InvalidArg(format!("invalid fetch params: {err}")))
    }

    async fn fetch_repo_snapshot(
        &self,
        params: RepoSnapshotParams,
        embedding_provider: Arc<dyn EmbeddingProvider>,
    ) -> StorageResult<FetchResponse> {
        let (owner, repo) = params.coordinates().map_err(|err| {
            StorageError::InvalidArg(format!("invalid repo coordinates: {err}"))
        })?;

        let snapshot: RepoSnapshot = self
            .client
            .fetch_repo_snapshot(&owner, &repo, &params)
            .await
            .map_err(|err| StorageError::SyncError(err.to_string()))?;

        let graph =
            mapper::build_repo_snapshot_graph(&snapshot, &params, embedding_provider)?;

        Ok(FetchResponse::GraphData(graph))
    }

    async fn fetch_search_repo(
        &self,
        params: SearchRepoParams,
    ) -> StorageResult<FetchResponse> {
        let results = self
            .client
            .search_repositories(&params)
            .await
            .map_err(|err| StorageError::SyncError(err.to_string()))?;

        let limited: Vec<_> = if let Some(limit) = params.limit {
            results.into_iter().take(limit).collect()
        } else {
            results
        };

        let batch = mapper::build_search_panel(&limited)?;

        Ok(FetchResponse::PanelData {
            table_name: "silver/panel/github_search".to_string(),
            batch,
        })
    }

    async fn probe_repo_snapshot(&self, params: RepoSnapshotParams) -> StorageResult<ProbeReport> {
        let (owner, repo) = params.coordinates().map_err(|err| {
            StorageError::InvalidArg(format!("invalid repo coordinates: {err}"))
        })?;

        let metadata = self
            .client
            .probe_repo_snapshot(&owner, &repo, params.rev.as_deref())
            .await
            .map_err(|err| StorageError::SyncError(err.to_string()))?;

        Ok(ProbeReport {
            fresh: None,
            remote_anchor: Some(metadata.remote_anchor),
            local_anchor: None,
            anchor_key: Some(metadata.anchor_key),
            estimated_missing: None,
            rate_limit_left: metadata.rate_limit_left,
            reason: metadata.reason,
        })
    }
}

#[async_trait]
impl Fetcher for GitFetcher {
    fn name(&self) -> &'static str {
        "gitfetcher"
    }

    fn capability(&self) -> FetcherCapability {
        Self::capability_descriptor()
    }

    async fn probe(&self, params: serde_json::Value) -> StorageResult<ProbeReport> {
        match Self::parse_params(params)? {
            FetcherParams::RepoSnapshot(params) => self.probe_repo_snapshot(params).await,
            FetcherParams::SearchRepo(_) => Ok(ProbeReport {
                fresh: Some(true),
                remote_anchor: None,
                local_anchor: None,
                anchor_key: None,
                estimated_missing: None,
                rate_limit_left: None,
                reason: None,
            }),
        }
    }

    async fn fetch(
        &self,
        params: serde_json::Value,
        embedding_provider: Arc<dyn EmbeddingProvider>,
    ) -> StorageResult<FetchResponse> {
        match Self::parse_params(params)? {
            FetcherParams::RepoSnapshot(params) => {
                self.fetch_repo_snapshot(params, embedding_provider).await
            }
            FetcherParams::SearchRepo(params) => self.fetch_search_repo(params).await,
        }
    }
}
