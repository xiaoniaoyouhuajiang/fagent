use serde::Deserialize;

use crate::error::{GitFetcherError, Result};

#[derive(Debug, Deserialize, Clone)]
pub struct RepoSnapshotParams {
    pub repo: String,
    #[serde(default)]
    pub rev: Option<String>,
    #[serde(default)]
    pub include_code: bool,
    #[serde(default = "default_include_readme")]
    pub include_readme: bool,
    #[serde(default = "default_include_issues")]
    pub include_issues: bool,
    #[serde(default = "default_include_pulls")]
    pub include_pulls: bool,
    #[serde(default = "default_include_developers")]
    pub include_developers: bool,
    #[serde(default = "default_doc_level_only")]
    pub doc_level_only: bool,
    #[serde(default)]
    pub touches_mode: TouchesMode,
    #[serde(default)]
    pub representative_comment_limit: Option<usize>,
}

fn default_include_readme() -> bool {
    true
}

fn default_include_issues() -> bool {
    true
}

fn default_include_pulls() -> bool {
    true
}

fn default_include_developers() -> bool {
    true
}

fn default_doc_level_only() -> bool {
    true
}

impl RepoSnapshotParams {
    pub fn coordinates(&self) -> Result<(String, String)> {
        let mut parts = self.repo.split('/');
        match (parts.next(), parts.next(), parts.next()) {
            (Some(owner), Some(repo), None) => Ok((owner.to_string(), repo.to_string())),
            _ => Err(GitFetcherError::InvalidParam(format!(
                "repo must be <owner>/<name>, got '{}'",
                self.repo
            ))),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct SearchRepoParams {
    pub query: String,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub min_stars: Option<u64>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum FetcherParams {
    RepoSnapshot(RepoSnapshotParams),
    SearchRepo(SearchRepoParams),
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TouchesMode {
    None,
    DirTopk,
    HotTopk,
}

impl Default for TouchesMode {
    fn default() -> Self {
        TouchesMode::None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchMode {
    RepoSnapshot,
    SearchRepo,
}

impl FetcherParams {
    pub fn mode(&self) -> FetchMode {
        match self {
            FetcherParams::RepoSnapshot(_) => FetchMode::RepoSnapshot,
            FetcherParams::SearchRepo(_) => FetchMode::SearchRepo,
        }
    }
}
