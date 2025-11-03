use std::{fs, sync::Arc};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use fstorage::{
    embedding::NullEmbeddingProvider,
    fetch::{FetchResponse, Fetchable, Fetcher},
    schemas::generated_schemas::{
        Commit, HasIssue, HasPr, HasVersion, IsCommit, Issue, IssueDoc, Label, OpenedIssue,
        OpenedPr, PrDoc, Project, PullRequest, ReadmeChunk, RelatesTo, Version,
    },
};
use git2::{Repository, Signature};
use gitfetcher::{
    client::{GitHubService, ProbeMetadata},
    models::{
        CommentInfo, CommentKind, CommitInfo, DeveloperProfile, IssueInfo, IssueRelation,
        LabelInfo, PullRequestInfo, ReactionSummary, ReadmeContent, RepoSnapshot, RepositoryInfo,
        ResolvedRevision, SearchRepository,
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

    let developer = DeveloperProfile {
        platform: "github".into(),
        account_id: "1".into(),
        login: "octocat".into(),
        name: Some("The Octocat".into()),
        company: Some("GitHub".into()),
        followers: Some(42),
        following: Some(7),
        location: Some("Internet".into()),
        email: None,
        created_at: None,
        updated_at: None,
    };

    let issue_created_at = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
    let issue_comment = CommentInfo {
        id: 101,
        body: "Looks good to me".into(),
        body_text: "Looks good to me".into(),
        author_login: Some("octocat".into()),
        author_id: Some("1".into()),
        author_association: Some("OWNER".into()),
        created_at: issue_created_at,
        updated_at: Some(issue_created_at),
        reactions: ReactionSummary::default(),
        is_bot: false,
        kind: CommentKind::Issue,
        in_reply_to_id: None,
        review_state: None,
        path: None,
        position: None,
    };

    let issues = vec![IssueInfo {
        project_url: repository.html_url.clone(),
        number: 1,
        title: "Sample issue".into(),
        body: Some("There is a small bug in the README".into()),
        state: "open".into(),
        author_login: Some("octocat".into()),
        author_id: Some("1".into()),
        created_at: issue_created_at,
        updated_at: Some(issue_created_at),
        closed_at: None,
        comments_count: 1,
        is_locked: false,
        milestone: None,
        assignees: vec!["octocat".into()],
        labels: vec![LabelInfo {
            name: "bug".into(),
            color: Some("ff0000".into()),
            description: None,
        }],
        reactions: ReactionSummary::default(),
        comments: vec![issue_comment],
        representative_comment_ids: vec![101],
        representative_digest_text: Some("Looks good to me".into()),
    }];

    let pr_created_at = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();
    let pr_issue_comment = CommentInfo {
        id: 201,
        body: "Implements the requested change".into(),
        body_text: "Implements the requested change".into(),
        author_login: Some("octocat".into()),
        author_id: Some("1".into()),
        author_association: Some("OWNER".into()),
        created_at: pr_created_at,
        updated_at: Some(pr_created_at),
        reactions: ReactionSummary::default(),
        is_bot: false,
        kind: CommentKind::Issue,
        in_reply_to_id: None,
        review_state: None,
        path: None,
        position: None,
    };

    let pull_requests = vec![PullRequestInfo {
        project_url: repository.html_url.clone(),
        number: 42,
        title: "Add greeting function".into(),
        body: Some("This PR adds a greeting function and closes #1".into()),
        state: "open".into(),
        draft: false,
        author_login: Some("octocat".into()),
        author_id: Some("1".into()),
        created_at: pr_created_at,
        updated_at: Some(pr_created_at),
        closed_at: None,
        merged: false,
        merged_at: None,
        merged_by: None,
        additions: Some(10),
        deletions: Some(2),
        changed_files: Some(3),
        commits: Some(1),
        base_ref: Some("main".into()),
        head_ref: Some("feature".into()),
        base_sha: Some("abc123".into()),
        head_sha: Some("def456".into()),
        is_cross_repo: false,
        comments_count: 1,
        review_comments_count: 0,
        labels: vec![LabelInfo {
            name: "enhancement".into(),
            color: Some("10b981".into()),
            description: None,
        }],
        assignees: vec!["octocat".into()],
        reactions: ReactionSummary::default(),
        issue_comments: vec![pr_issue_comment],
        review_comments: Vec::new(),
        representative_comment_ids: vec![201],
        representative_digest_text: Some("Implements the requested change".into()),
        related_issues: vec![IssueRelation {
            owner: repository.owner.clone(),
            repo: repository.name.clone(),
            number: 1,
            link_type: "closes".into(),
            strength: 3,
            origin: "pr_body".into(),
            cross_repo: false,
        }],
    }];

    RepoSnapshot {
        repository,
        revision,
        commit,
        readme: Some(readme),
        developers: vec![developer],
        issues,
        pull_requests,
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
        developers: Vec::new(),
        issues: Vec::new(),
        pull_requests: Vec::new(),
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
            let entity_types: std::collections::HashSet<_> = graph
                .entities
                .iter()
                .map(|entity| entity.entity_type_any())
                .collect();
            assert!(entity_types.contains(Commit::ENTITY_TYPE));
            assert!(entity_types.contains(Project::ENTITY_TYPE));
            assert!(entity_types.contains(Version::ENTITY_TYPE));
            assert!(entity_types.contains(ReadmeChunk::ENTITY_TYPE));
            assert!(entity_types.contains(Issue::ENTITY_TYPE));
            assert!(entity_types.contains(PullRequest::ENTITY_TYPE));
            assert!(entity_types.contains(IssueDoc::ENTITY_TYPE));
            assert!(entity_types.contains(PrDoc::ENTITY_TYPE));
            assert!(entity_types.contains(Label::ENTITY_TYPE));
            assert!(entity_types.contains(HasVersion::ENTITY_TYPE));
            assert!(entity_types.contains(IsCommit::ENTITY_TYPE));
            assert!(entity_types.contains(HasIssue::ENTITY_TYPE));
            assert!(entity_types.contains(HasPr::ENTITY_TYPE));
            assert!(entity_types.contains(OpenedIssue::ENTITY_TYPE));
            assert!(entity_types.contains(OpenedPr::ENTITY_TYPE));
            assert!(entity_types.contains(RelatesTo::ENTITY_TYPE));

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

            let issue_doc_count = graph
                .entities
                .iter()
                .filter(|entity| entity.entity_type_any() == IssueDoc::ENTITY_TYPE)
                .count();
            assert_eq!(issue_doc_count, 1);

            let pr_doc_count = graph
                .entities
                .iter()
                .filter(|entity| entity.entity_type_any() == PrDoc::ENTITY_TYPE)
                .count();
            assert_eq!(pr_doc_count, 1);
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
