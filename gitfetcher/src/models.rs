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
pub struct DeveloperProfile {
    pub platform: String,
    pub account_id: String,
    pub login: String,
    pub name: Option<String>,
    pub company: Option<String>,
    pub followers: Option<u64>,
    pub following: Option<u64>,
    pub location: Option<String>,
    pub email: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct LabelInfo {
    pub name: String,
    pub color: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ReactionSummary {
    pub plus_one: u64,
    pub heart: u64,
    pub hooray: u64,
    pub eyes: u64,
    pub rocket: u64,
    pub confused: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommentKind {
    Issue,
    Review,
}

#[derive(Debug, Clone)]
pub struct CommentInfo {
    pub id: i64,
    pub body: String,
    pub body_text: String,
    pub author_login: Option<String>,
    pub author_id: Option<String>,
    pub author_association: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub reactions: ReactionSummary,
    pub is_bot: bool,
    pub kind: CommentKind,
    pub in_reply_to_id: Option<i64>,
    pub review_state: Option<String>,
    pub path: Option<String>,
    pub position: Option<i64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct IssueRelation {
    pub owner: String,
    pub repo: String,
    pub number: i64,
    pub link_type: String,
    pub strength: u8,
    pub origin: String,
    pub cross_repo: bool,
}

#[derive(Debug, Clone)]
pub struct IssueInfo {
    pub project_url: String,
    pub number: i64,
    pub title: String,
    pub body: Option<String>,
    pub state: String,
    pub author_login: Option<String>,
    pub author_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub closed_at: Option<DateTime<Utc>>,
    pub comments_count: u64,
    pub is_locked: bool,
    pub milestone: Option<String>,
    pub assignees: Vec<String>,
    pub labels: Vec<LabelInfo>,
    pub reactions: ReactionSummary,
    pub comments: Vec<CommentInfo>,
    pub representative_comment_ids: Vec<i64>,
    pub representative_digest_text: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PullRequestInfo {
    pub project_url: String,
    pub number: i64,
    pub title: String,
    pub body: Option<String>,
    pub state: String,
    pub draft: bool,
    pub author_login: Option<String>,
    pub author_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub closed_at: Option<DateTime<Utc>>,
    pub merged: bool,
    pub merged_at: Option<DateTime<Utc>>,
    pub merged_by: Option<String>,
    pub additions: Option<u64>,
    pub deletions: Option<u64>,
    pub changed_files: Option<u64>,
    pub commits: Option<u64>,
    pub base_ref: Option<String>,
    pub head_ref: Option<String>,
    pub base_sha: Option<String>,
    pub head_sha: Option<String>,
    pub is_cross_repo: bool,
    pub comments_count: u64,
    pub review_comments_count: u64,
    pub labels: Vec<LabelInfo>,
    pub assignees: Vec<String>,
    pub reactions: ReactionSummary,
    pub issue_comments: Vec<CommentInfo>,
    pub review_comments: Vec<CommentInfo>,
    pub representative_comment_ids: Vec<i64>,
    pub representative_digest_text: Option<String>,
    pub related_issues: Vec<IssueRelation>,
}

#[derive(Debug, Clone)]
pub struct RepoSnapshot {
    pub repository: RepositoryInfo,
    pub revision: ResolvedRevision,
    pub commit: CommitInfo,
    pub readme: Option<ReadmeContent>,
    pub developers: Vec<DeveloperProfile>,
    pub issues: Vec<IssueInfo>,
    pub pull_requests: Vec<PullRequestInfo>,
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
