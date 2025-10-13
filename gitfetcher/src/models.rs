use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct RepositoryInfo {
    pub owner: String,
    pub name: String,
    pub full_name: String,
    pub html_url: String,
    pub description: Option<String>,
    pub language: Option<String>,
    pub stargazers: u64,
    pub forks: u64,
    pub default_branch: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CommitInfo {
    pub sha: String,
    pub message: String,
    pub author: Option<String>,
    pub authored_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ResolvedRevision {
    pub reference: Option<String>,
    pub sha: String,
    pub is_head: bool,
}

#[derive(Debug, Clone)]
pub struct ReadmeContent {
    pub text: String,
    pub source_file: String,
}

#[derive(Debug, Clone)]
pub struct RepoSnapshot {
    pub repository: RepositoryInfo,
    pub revision: ResolvedRevision,
    pub commit: CommitInfo,
    pub readme: Option<ReadmeContent>,
}

#[derive(Debug, Clone)]
pub struct SearchRepository {
    pub full_name: String,
    pub html_url: String,
    pub description: Option<String>,
    pub language: Option<String>,
    pub stargazers: u64,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeDirection {
    Outgoing,
    Incoming,
}

#[derive(Debug, Deserialize)]
pub struct RepoCoordinates {
    pub owner: String,
    pub name: String,
}
