use std::sync::Arc;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use fstorage::{
    embedding::NullEmbeddingProvider,
    fetch::{FetchResponse, Fetchable, Fetcher},
    schemas::generated_schemas::{Commit, HasVersion, IsCommit, Project, Version},
};
use gitfetcher::{
    client::{GitHubService, ProbeMetadata},
    models::{
        CommitInfo, ReadmeContent, RepoSnapshot, RepositoryInfo, ResolvedRevision, SearchRepository,
    },
    params::{RepoSnapshotParams, SearchRepoParams},
    GitFetcher,
};
use serde_json::json;

struct MockGitHubService {
    snapshot: RepoSnapshot,
    search_results: Vec<SearchRepository>,
    probe: ProbeMetadata,
}

#[async_trait]
impl GitHubService for MockGitHubService {
    async fn fetch_repo_snapshot(
        &self,
        _owner: &str,
        _repo: &str,
        _params: &RepoSnapshotParams,
    ) -> gitfetcher::error::Result<RepoSnapshot> {
        Ok(self.snapshot.clone())
    }

    async fn probe_repo_snapshot(
        &self,
        _owner: &str,
        _repo: &str,
        _rev: Option<&str>,
    ) -> gitfetcher::error::Result<ProbeMetadata> {
        Ok(self.probe.clone())
    }

    async fn search_repositories(
        &self,
        _params: &SearchRepoParams,
    ) -> gitfetcher::error::Result<Vec<SearchRepository>> {
        Ok(self.search_results.clone())
    }
}

fn sample_snapshot() -> RepoSnapshot {
    let repository = RepositoryInfo {
        owner: "octocat".into(),
        name: "hello-world".into(),
        full_name: "octocat/hello-world".into(),
        html_url: "https://github.com/octocat/hello-world".into(),
        description: Some("Demo repository".into()),
        language: Some("Rust".into()),
        stargazers: 42,
        forks: 7,
        default_branch: Some("main".into()),
    };

    let revision = ResolvedRevision {
        reference: Some("main".into()),
        sha: "abc123".into(),
        is_head: true,
    };

    let commit = CommitInfo {
        sha: "abc123".into(),
        message: "Initial commit".into(),
        author: Some("octocat".into()),
        authored_at: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
    };

    let readme = ReadmeContent {
        text: "# Hello".into(),
        source_file: "README.md".into(),
    };

    RepoSnapshot {
        repository,
        revision,
        commit,
        readme: Some(readme),
    }
}

fn sample_search_results() -> Vec<SearchRepository> {
    vec![SearchRepository {
        full_name: "octocat/hello-world".into(),
        html_url: "https://github.com/octocat/hello-world".into(),
        description: Some("Demo".into()),
        language: Some("Rust".into()),
        stargazers: 99,
        updated_at: None,
    }]
}

fn sample_probe() -> ProbeMetadata {
    ProbeMetadata {
        remote_anchor: "abc123".into(),
        anchor_key: "main".into(),
        rate_limit_left: Some(1000),
        reason: None,
    }
}

#[tokio::test]
async fn repo_snapshot_fetch_builds_graph() {
    let service = Arc::new(MockGitHubService {
        snapshot: sample_snapshot(),
        search_results: sample_search_results(),
        probe: sample_probe(),
    });
    let fetcher = GitFetcher::new(service);

    let response = fetcher
        .fetch(
            json!({ "mode": "repo_snapshot", "repo": "octocat/hello-world" }),
            Arc::new(NullEmbeddingProvider),
        )
        .await
        .expect("fetch should succeed");

    match response {
        FetchResponse::GraphData(graph) => {
            let mut entity_types: Vec<_> = graph
                .entities
                .iter()
                .map(|entity| entity.entity_type_any())
                .collect();
            entity_types.sort();
            assert_eq!(
                entity_types,
                vec![
                    Commit::ENTITY_TYPE,
                    HasVersion::ENTITY_TYPE,
                    IsCommit::ENTITY_TYPE,
                    Project::ENTITY_TYPE,
                    Version::ENTITY_TYPE,
                ]
            );
        }
        _ => panic!("unexpected response"),
    }
}

#[tokio::test]
async fn search_repo_fetch_returns_panel() {
    let service = Arc::new(MockGitHubService {
        snapshot: sample_snapshot(),
        search_results: sample_search_results(),
        probe: sample_probe(),
    });
    let fetcher = GitFetcher::new(service);

    let response = fetcher
        .fetch(
            json!({ "mode": "search_repo", "query": "rust" }),
            Arc::new(NullEmbeddingProvider),
        )
        .await
        .expect("fetch should succeed");

    match response {
        FetchResponse::PanelData { batch, .. } => {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 6);
        }
        _ => panic!("unexpected response"),
    }
}

#[tokio::test]
async fn probe_returns_anchor_metadata() {
    let service = Arc::new(MockGitHubService {
        snapshot: sample_snapshot(),
        search_results: sample_search_results(),
        probe: sample_probe(),
    });
    let fetcher = GitFetcher::new(service);

    let probe = fetcher
        .probe(json!({ "mode": "repo_snapshot", "repo": "octocat/hello-world" }))
        .await
        .expect("probe should succeed");

    assert_eq!(probe.remote_anchor, Some("abc123".into()));
    assert_eq!(probe.anchor_key, Some("main".into()));
    assert_eq!(probe.rate_limit_left, Some(1000));
}
