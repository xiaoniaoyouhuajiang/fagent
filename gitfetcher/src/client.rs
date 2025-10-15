use async_trait::async_trait;
use chrono::Utc;
use octocrab::{
    models::{
        repos::{Object, RepoCommit},
        Repository,
    },
    params::repos::Reference,
    Octocrab,
};

use crate::{
    error::{GitFetcherError, Result},
    models::{
        CommitInfo, ReadmeContent, RepoSnapshot, RepositoryInfo, ResolvedRevision, SearchRepository,
    },
    params::{RepoSnapshotParams, SearchRepoParams},
};

#[derive(Debug, Clone)]
pub struct ProbeMetadata {
    pub remote_anchor: String,
    pub anchor_key: String,
    pub rate_limit_left: Option<u32>,
    pub reason: Option<String>,
}

#[async_trait]
pub trait GitHubService: Send + Sync {
    async fn fetch_repo_snapshot(
        &self,
        owner: &str,
        repo: &str,
        params: &RepoSnapshotParams,
    ) -> Result<RepoSnapshot>;

    async fn probe_repo_snapshot(
        &self,
        owner: &str,
        repo: &str,
        rev: Option<&str>,
    ) -> Result<ProbeMetadata>;

    async fn search_repositories(&self, params: &SearchRepoParams)
        -> Result<Vec<SearchRepository>>;
}

pub struct OctocrabService {
    client: Octocrab,
}

impl OctocrabService {
    pub fn new(token: Option<String>) -> octocrab::Result<Self> {
        let mut builder = Octocrab::builder();
        if let Some(token) = token {
            builder = builder.personal_token(token);
        }
        let client = builder.build()?;
        Ok(Self { client })
    }

    async fn load_repository(&self, owner: &str, repo: &str) -> Result<RepositoryInfo> {
        let handle = self.client.repos(owner, repo);
        let repository = handle.get().await?;
        Ok(Self::map_repository(repository, owner))
    }

    fn map_repository(repo: Repository, owner_hint: &str) -> RepositoryInfo {
        use serde_json::Value;

        let Repository {
            name,
            full_name,
            owner,
            html_url,
            description,
            language,
            stargazers_count,
            forks_count,
            default_branch,
            ..
        } = repo;

        let owner_login = owner
            .as_ref()
            .map(|owner| owner.login.clone())
            .unwrap_or_else(|| owner_hint.to_string());
        let full_name = full_name.unwrap_or_else(|| format!("{owner_login}/{name}"));
        let html_url = html_url
            .map(|url| url.to_string())
            .unwrap_or_else(|| format!("https://github.com/{full_name}"));
        let language = language.and_then(|value| match value {
            Value::String(value) => Some(value),
            _ => None,
        });

        RepositoryInfo {
            owner: owner_login,
            name,
            full_name,
            html_url,
            description,
            language,
            stargazers: stargazers_count.unwrap_or(0) as u64,
            forks: forks_count.unwrap_or(0) as u64,
            default_branch,
        }
    }

    async fn resolve_revision(
        &self,
        owner: &str,
        repo: &str,
        repo_info: &RepositoryInfo,
        rev: Option<&str>,
    ) -> Result<ResolvedRevision> {
        let repo_handle = self.client.repos(owner, repo);

        if let Some(reference) = rev {
            // Try branch first
            if let Ok(reference_ref) = repo_handle
                .get_ref(&Reference::Branch(reference.to_string()))
                .await
            {
                if let Object::Commit { sha, .. } = reference_ref.object {
                    return Ok(ResolvedRevision {
                        reference: Some(reference.to_string()),
                        sha,
                        is_head: repo_info
                            .default_branch
                            .as_deref()
                            .map(|branch| branch == reference)
                            .unwrap_or(false),
                    });
                }
            }

            // Try tag reference
            if let Ok(reference_ref) = repo_handle
                .get_ref(&Reference::Tag(reference.to_string()))
                .await
            {
                if let Object::Commit { sha, .. } = reference_ref.object {
                    return Ok(ResolvedRevision {
                        reference: Some(reference.to_string()),
                        sha,
                        is_head: false,
                    });
                }
            }

            // Fallback to treat as specific commit
            let commit = self.fetch_commit_object(owner, repo, reference).await?;
            return Ok(ResolvedRevision {
                reference: Some(reference.to_string()),
                sha: commit.sha.clone(),
                is_head: repo_info
                    .default_branch
                    .as_deref()
                    .map(|branch| branch == reference)
                    .unwrap_or(false),
            });
        }

        let branch = repo_info
            .default_branch
            .clone()
            .unwrap_or_else(|| "main".to_string());
        let reference = repo_handle
            .get_ref(&Reference::Branch(branch.clone()))
            .await?;
        let sha = match reference.object {
            Object::Commit { sha, .. } => sha,
            Object::Tag { sha, .. } => sha,
            _ => {
                return Err(GitFetcherError::InvalidParam(
                    "reference did not resolve to a commit".to_string(),
                ))
            }
        };

        Ok(ResolvedRevision {
            reference: Some(branch),
            sha,
            is_head: true,
        })
    }

    async fn fetch_commit_object(
        &self,
        owner: &str,
        repo: &str,
        reference: &str,
    ) -> Result<RepoCommit> {
        let page = self
            .client
            .repos(owner, repo)
            .list_commits()
            .sha(reference)
            .per_page(1)
            .send()
            .await?;

        page.items
            .into_iter()
            .next()
            .ok_or_else(|| GitFetcherError::NotFound(format!("commit '{reference}'")))
    }

    async fn load_commit(&self, owner: &str, repo: &str, sha: &str) -> Result<CommitInfo> {
        let commit = self.fetch_commit_object(owner, repo, sha).await?;
        let message = commit.commit.message.clone();
        let author_login = commit.author.as_ref().map(|author| author.login.clone());
        let authored_at = commit
            .commit
            .author
            .and_then(|author| author.date)
            .or_else(|| commit.commit.committer.and_then(|committer| committer.date))
            .unwrap_or_else(Utc::now);

        Ok(CommitInfo {
            sha: commit.sha,
            message,
            author: author_login,
            authored_at,
        })
    }

    async fn load_readme(
        &self,
        owner: &str,
        repo: &str,
        reference: Option<&str>,
    ) -> Result<Option<ReadmeContent>> {
        let handle = self.client.repos(owner, repo);
        let builder = if let Some(reference) = reference {
            handle.get_readme().r#ref(reference.to_string())
        } else {
            handle.get_readme()
        };

        match builder.send().await {
            Ok(content) => {
                let text = content.decoded_content().unwrap_or_else(|| String::new());
                Ok(Some(ReadmeContent {
                    text,
                    source_file: content.path.clone(),
                }))
            }
            Err(octocrab::Error::GitHub { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}

#[async_trait]
impl GitHubService for OctocrabService {
    async fn fetch_repo_snapshot(
        &self,
        owner: &str,
        repo: &str,
        params: &RepoSnapshotParams,
    ) -> Result<RepoSnapshot> {
        let repository = self.load_repository(owner, repo).await?;
        let revision = self
            .resolve_revision(owner, repo, &repository, params.rev.as_deref())
            .await?;
        let commit = self.load_commit(owner, repo, &revision.sha).await?;

        let readme = if params.include_readme {
            self.load_readme(
                owner,
                repo,
                params
                    .rev
                    .as_deref()
                    .or(revision.reference.as_deref())
                    .or(Some(&revision.sha)),
            )
            .await?
        } else {
            None
        };

        Ok(RepoSnapshot {
            repository,
            revision,
            commit,
            readme,
        })
    }

    async fn probe_repo_snapshot(
        &self,
        owner: &str,
        repo: &str,
        rev: Option<&str>,
    ) -> Result<ProbeMetadata> {
        let repository = self.load_repository(owner, repo).await?;
        let revision = self.resolve_revision(owner, repo, &repository, rev).await?;

        let anchor_key = rev
            .map(|rev| rev.to_string())
            .or_else(|| revision.reference.clone())
            .unwrap_or_else(|| "head".to_string());

        Ok(ProbeMetadata {
            remote_anchor: revision.sha,
            anchor_key,
            rate_limit_left: None,
            reason: None,
        })
    }

    async fn search_repositories(
        &self,
        params: &SearchRepoParams,
    ) -> Result<Vec<SearchRepository>> {
        let mut query = params.query.clone();
        if let Some(language) = &params.language {
            query.push_str(" language:");
            query.push_str(language);
        }
        if let Some(min_stars) = params.min_stars {
            query.push_str(&format!(" stars:>={}", min_stars));
        }

        let per_page = params.limit.map(|limit| limit.min(100) as u8).unwrap_or(30);

        let mut page = self
            .client
            .search()
            .repositories(&query)
            .per_page(per_page)
            .send()
            .await?;

        Ok(page
            .take_items()
            .into_iter()
            .map(|repo| {
                use serde_json::Value;

                let Repository {
                    name,
                    full_name,
                    owner,
                    html_url,
                    description,
                    language,
                    stargazers_count,
                    updated_at,
                    ..
                } = repo;

                let owner_login = owner
                    .as_ref()
                    .map(|owner| owner.login.clone())
                    .unwrap_or_default();
                let full_name = full_name.unwrap_or_else(|| format!("{owner_login}/{name}"));
                let html_url = html_url
                    .map(|url| url.to_string())
                    .unwrap_or_else(|| format!("https://github.com/{full_name}"));
                let language = language.and_then(|value| match value {
                    Value::String(value) => Some(value),
                    _ => None,
                });

                SearchRepository {
                    full_name,
                    html_url,
                    description,
                    language,
                    stargazers: stargazers_count.unwrap_or(0) as u64,
                    updated_at,
                }
            })
            .collect())
    }
}
