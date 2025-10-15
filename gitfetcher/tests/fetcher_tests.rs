use std::{fs, sync::Arc};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use fstorage::{
    embedding::NullEmbeddingProvider,
    fetch::{FetchResponse, Fetchable, Fetcher},
    schemas::generated_schemas::{Commit, HasVersion, IsCommit, Project, ReadmeChunk, Version},
};
use git2::{Repository, Signature};
use gitfetcher::{
    client::{GitHubService, ProbeMetadata},
    models::{
        CommitInfo, ReadmeContent, RepoSnapshot, RepositoryInfo, ResolvedRevision, SearchRepository,
    },
    params::{RepoSnapshotParams, SearchRepoParams},
    GitFetcher,
};
use serde_json::json;
use tempfile::TempDir;

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

fn snapshot_with_local_repo() -> (TempDir, RepoSnapshot) {
    let temp_dir = TempDir::new().expect("temp dir");
    let repo_path = temp_dir.path().join("repo");
    fs::create_dir_all(repo_path.join("src")).expect("create src directory");

    let repo = Repository::init(&repo_path).expect("init repo");
    fs::write(
        repo_path.join("src").join("lib.rs"),
        "pub fn greet(name: &str) -> String { format!(\"Hello {name}\") }",
    )
    .expect("write lib.rs");
    fs::write(
        repo_path.join("Cargo.toml"),
        "[package]\nname = \"sample\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    )
    .expect("write cargo manifest");

    let mut index = repo.index().expect("repo index");
    index
        .add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None)
        .expect("add files");
    index.write().expect("write index");
    let tree_id = index.write_tree().expect("tree id");
    let tree = repo.find_tree(tree_id).expect("tree");
    let signature = Signature::now("Tester", "tester@example.com").expect("signature");
    let oid = repo
        .commit(
            Some("HEAD"),
            &signature,
            &signature,
            "Initial revision",
            &tree,
            &[],
        )
        .expect("commit");

    let repository = RepositoryInfo {
        owner: "local".into(),
        name: "sample".into(),
        full_name: "local/sample".into(),
        html_url: repo_path.to_string_lossy().to_string(),
        description: Some("Local sample repo".into()),
        language: Some("Rust".into()),
        stargazers: 0,
        forks: 0,
        default_branch: Some("master".into()),
    };

    let revision = ResolvedRevision {
        reference: Some("master".into()),
        sha: oid.to_string(),
        is_head: true,
    };

    let commit = CommitInfo {
        sha: oid.to_string(),
        message: "Initial revision".into(),
        author: Some("Tester".into()),
        authored_at: Utc::now(),
    };

    let snapshot = RepoSnapshot {
        repository,
        revision,
        commit,
        readme: None,
    };

    (temp_dir, snapshot)
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
            assert_eq!(entity_types.len(), 6);
            assert!(entity_types.contains(&Commit::ENTITY_TYPE));
            assert!(entity_types.contains(&HasVersion::ENTITY_TYPE));
            assert!(entity_types.contains(&IsCommit::ENTITY_TYPE));
            assert!(entity_types.contains(&Project::ENTITY_TYPE));
            assert!(entity_types.contains(&Version::ENTITY_TYPE));
            assert!(entity_types.contains(&ReadmeChunk::ENTITY_TYPE));

            let readme_batches = graph
                .entities
                .iter()
                .filter(|entity| entity.entity_type_any() == ReadmeChunk::ENTITY_TYPE)
                .collect::<Vec<_>>();
            assert_eq!(readme_batches.len(), 1);
            let readme_batch = readme_batches[0]
                .to_record_batch_any()
                .expect("readme batch convertible");
            assert_eq!(readme_batch.num_rows(), 1);
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

#[tokio::test]
async fn repo_snapshot_fetch_with_code_succeeds() {
    let (temp_dir, snapshot) = snapshot_with_local_repo();

    let service = Arc::new(MockGitHubService {
        snapshot: snapshot.clone(),
        search_results: sample_search_results(),
        probe: sample_probe(),
    });
    let fetcher = GitFetcher::new(service);

    let response = fetcher
        .fetch(
            json!({
                "mode": "repo_snapshot",
                "repo": snapshot.repository.full_name,
                "include_code": true
            }),
            Arc::new(NullEmbeddingProvider),
        )
        .await
        .expect("fetch should succeed");

    match response {
        FetchResponse::GraphData(graph) => {
            let entity_types: Vec<_> = graph
                .entities
                .iter()
                .map(|entity| entity.entity_type_any())
                .collect();
            assert!(entity_types.contains(&Project::ENTITY_TYPE));
        }
        _ => panic!("unexpected response"),
    }

    drop(temp_dir);
}
