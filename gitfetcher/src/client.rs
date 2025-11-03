use async_trait::async_trait;
use chrono::Utc;
use octocrab::{
    models::{
        repos::{Object, RepoCommit},
        Repository,
    },
    params::repos::Reference,
    params::{self, Direction},
    Octocrab,
};
use std::collections::HashMap;

use crate::{
    error::{GitFetcherError, Result},
    models::{
        CommentInfo, CommentKind, CommitInfo, DeveloperProfile, IssueInfo, IssueRelation,
        LabelInfo, PullRequestInfo, ReactionSummary, ReadmeContent, RepoSnapshot, RepositoryInfo,
        ResolvedRevision, SearchRepository,
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
        log::info!("Loading repository metadata for {owner}/{repo}");
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
        log::info!("Resolving revision for {owner}/{repo} (hint: {:?})", rev);
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
        log::info!("Fetching commit object for {owner}/{repo}@{reference} via list_commits");
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
        log::info!("Loading commit metadata for {owner}/{repo}@{sha}");
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
        log::info!(
            "Loading README content for {owner}/{repo} (reference {:?})",
            reference
        );
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

    async fn load_issues(
        &self,
        owner: &str,
        repo: &str,
        project_url: &str,
        params: &RepoSnapshotParams,
        developers: &mut HashMap<String, DeveloperProfile>,
    ) -> Result<Vec<IssueInfo>> {
        log::info!("Loading issues for {owner}/{repo}");
        let mut collected = Vec::new();
        let mut page = self
            .client
            .issues(owner, repo)
            .list()
            .state(params::State::All)
            .sort(params::issues::Sort::Updated)
            .direction(Direction::Descending)
            .per_page(100)
            .send()
            .await?;
        let mut issues = page.take_items();
        while let Some(next) = self.client.get_page(&page.next).await? {
            page = next;
            issues.extend(page.take_items());
        }

        let comment_limit = params.representative_comment_limit.unwrap_or(16) * 4;

        for issue in issues.into_iter() {
            if issue.pull_request.is_some() {
                continue;
            }

            Self::ensure_developer(developers, &issue.user);
            let author_login = Some(issue.user.login.clone());
            let author_id = issue.user.id.0.to_string();

            let assignees = issue
                .assignees
                .iter()
                .map(|author| {
                    Self::ensure_developer(developers, author);
                    author.login.clone()
                })
                .collect::<Vec<_>>();

            let labels = issue.labels.iter().map(Self::map_label).collect::<Vec<_>>();

            let milestone = issue
                .milestone
                .as_ref()
                .map(|milestone| milestone.title.clone());

            let comments = self
                .load_issue_comments(owner, repo, issue.number, developers, comment_limit)
                .await?;
            let (representative_ids, digest_text) = Self::select_representative_comments(
                &comments,
                params.representative_comment_limit.unwrap_or(8),
            );

            collected.push(IssueInfo {
                project_url: project_url.to_string(),
                number: issue.number as i64,
                title: issue.title.clone(),
                body: issue.body.clone(),
                state: format!("{:?}", issue.state),
                author_login,
                author_id: Some(author_id),
                created_at: issue.created_at,
                updated_at: Some(issue.updated_at),
                closed_at: issue.closed_at,
                comments_count: issue.comments as u64,
                is_locked: issue.locked,
                milestone,
                assignees,
                labels,
                reactions: ReactionSummary::default(),
                comments,
                representative_comment_ids: representative_ids,
                representative_digest_text: digest_text,
            });
        }

        Ok(collected)
    }

    async fn load_pull_requests(
        &self,
        owner: &str,
        repo: &str,
        project_url: &str,
        params: &RepoSnapshotParams,
        developers: &mut HashMap<String, DeveloperProfile>,
    ) -> Result<Vec<PullRequestInfo>> {
        log::info!("Loading pull requests for {owner}/{repo}");
        let mut collected = Vec::new();
        let mut page = self
            .client
            .pulls(owner, repo)
            .list()
            .state(params::State::All)
            .sort(params::pulls::Sort::Updated)
            .direction(Direction::Descending)
            .per_page(100)
            .send()
            .await?;
        let mut pulls = page.take_items();
        while let Some(next) = self.client.get_page(&page.next).await? {
            page = next;
            pulls.extend(page.take_items());
        }

        let comment_limit = params.representative_comment_limit.unwrap_or(16) * 4;

        for pr in pulls.into_iter() {
            if let Some(user) = pr.user.as_deref() {
                Self::ensure_developer(developers, user);
            }
            if let Some(merged_by) = pr.merged_by.as_deref() {
                Self::ensure_developer(developers, merged_by);
            }

            let author_login = pr.user.as_ref().map(|u| u.login.clone());
            let author_id = pr.user.as_ref().map(|u| u.id.0.to_string());

            let assignees = pr
                .assignees
                .as_ref()
                .map(|list| {
                    list.iter()
                        .map(|author| {
                            Self::ensure_developer(developers, author);
                            author.login.clone()
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            let labels = pr
                .labels
                .as_ref()
                .map(|list| list.iter().map(Self::map_label).collect::<Vec<_>>())
                .unwrap_or_default();

            let issue_comments = self
                .load_issue_comments(owner, repo, pr.number, developers, comment_limit)
                .await?;
            let review_comments = self
                .load_review_comments(owner, repo, pr.number, developers, comment_limit)
                .await?;
            let mut all_comments = issue_comments.clone();
            all_comments.extend(review_comments.clone());
            let (representative_ids, digest_text) = Self::select_representative_comments(
                &all_comments,
                params.representative_comment_limit.unwrap_or(8),
            );

            let mut relations = Vec::new();
            if let Some(title) = pr.title.as_ref() {
                relations.extend(Self::extract_issue_links(title, owner, repo, "pr_title"));
            }
            if let Some(body) = pr.body.as_ref() {
                relations.extend(Self::extract_issue_links(body, owner, repo, "pr_body"));
            }
            let mut relation_map: HashMap<(String, String, i64), IssueRelation> = HashMap::new();
            for relation in relations.into_iter() {
                let key = (
                    relation.owner.clone(),
                    relation.repo.clone(),
                    relation.number,
                );
                relation_map
                    .entry(key)
                    .and_modify(|existing| {
                        if relation.strength > existing.strength {
                            *existing = relation.clone();
                        }
                    })
                    .or_insert(relation);
            }
            let mut related_issues: Vec<IssueRelation> = relation_map.into_values().collect();
            related_issues.sort_by(|a, b| b.strength.cmp(&a.strength));
            if related_issues.len() > 50 {
                related_issues.truncate(50);
            }

            let base_sha = pr.base.sha.clone();
            let head_sha = pr.head.sha.clone();

            let is_cross_repo = pr
                .head
                .repo
                .as_ref()
                .and_then(|repo_info| repo_info.full_name.clone())
                .map(|full_name| !full_name.eq_ignore_ascii_case(&format!("{owner}/{repo}")))
                .unwrap_or(false);

            collected.push(PullRequestInfo {
                project_url: project_url.to_string(),
                number: pr.number as i64,
                title: pr.title.clone().unwrap_or_default(),
                body: pr.body.clone(),
                state: pr
                    .state
                    .map(|state| format!("{state:?}"))
                    .unwrap_or_else(|| "UNKNOWN".to_string()),
                draft: pr.draft.unwrap_or(false),
                author_login,
                author_id,
                created_at: pr.created_at.unwrap_or_else(Utc::now),
                updated_at: pr.updated_at,
                closed_at: pr.closed_at,
                merged: pr.merged.unwrap_or(false),
                merged_at: pr.merged_at,
                merged_by: pr.merged_by.as_ref().map(|user| user.login.clone()),
                additions: pr.additions,
                deletions: pr.deletions,
                changed_files: pr.changed_files,
                commits: pr.commits,
                base_ref: Some(pr.base.ref_field.clone()),
                head_ref: Some(pr.head.ref_field.clone()),
                base_sha: Some(base_sha),
                head_sha: Some(head_sha),
                is_cross_repo,
                comments_count: pr.comments.unwrap_or(0),
                review_comments_count: pr.review_comments.unwrap_or(0),
                labels,
                assignees,
                reactions: ReactionSummary::default(),
                issue_comments,
                review_comments,
                representative_comment_ids: representative_ids,
                representative_digest_text: digest_text,
                related_issues,
            });
        }

        Ok(collected)
    }

    async fn load_issue_comments(
        &self,
        owner: &str,
        repo: &str,
        number: u64,
        developers: &mut HashMap<String, DeveloperProfile>,
        max_entries: usize,
    ) -> Result<Vec<CommentInfo>> {
        log::info!(
            "Loading issue comments for {owner}/{repo} issue #{number} (limit {max_entries})"
        );
        let mut page = self
            .client
            .issues(owner, repo)
            .list_comments(number)
            .per_page(100)
            .send()
            .await?;
        let mut comments = page.take_items();
        while comments.len() < max_entries {
            if let Some(next) = self.client.get_page(&page.next).await? {
                page = next;
                if page.items.is_empty() {
                    break;
                }
                comments.extend(page.take_items());
            } else {
                break;
            }
        }
        comments.truncate(max_entries);

        Ok(comments
            .into_iter()
            .map(|comment| {
                Self::ensure_developer(developers, &comment.user);
                CommentInfo {
                    id: comment.id.0 as i64,
                    body: comment.body.clone().unwrap_or_default(),
                    body_text: comment
                        .body_text
                        .clone()
                        .or_else(|| comment.body.clone())
                        .unwrap_or_default(),
                    author_login: Some(comment.user.login.clone()),
                    author_id: Some(comment.user.id.0.to_string()),
                    author_association: Some(Self::association_to_string(
                        &comment.author_association,
                    )),
                    created_at: comment.created_at,
                    updated_at: comment.updated_at,
                    reactions: ReactionSummary::default(),
                    is_bot: comment.user.r#type == "Bot"
                        || comment.user.login.to_ascii_lowercase().ends_with("[bot]"),
                    kind: CommentKind::Issue,
                    in_reply_to_id: None,
                    review_state: None,
                    path: None,
                    position: None,
                }
            })
            .collect())
    }

    async fn load_review_comments(
        &self,
        owner: &str,
        repo: &str,
        number: u64,
        developers: &mut HashMap<String, DeveloperProfile>,
        max_entries: usize,
    ) -> Result<Vec<CommentInfo>> {
        log::info!("Loading review comments for {owner}/{repo} PR #{number} (limit {max_entries})");
        let mut page = self
            .client
            .pulls(owner, repo)
            .list_comments(Some(number))
            .per_page(100)
            .send()
            .await?;
        let mut comments = page.take_items();
        while comments.len() < max_entries {
            if let Some(next) = self.client.get_page(&page.next).await? {
                page = next;
                if page.items.is_empty() {
                    break;
                }
                comments.extend(page.take_items());
            } else {
                break;
            }
        }
        comments.truncate(max_entries);

        Ok(comments
            .into_iter()
            .map(|comment| {
                if let Some(user) = comment.user.as_ref() {
                    Self::ensure_developer(developers, user);
                }
                CommentInfo {
                    id: comment.id.0 as i64,
                    body: comment.body.clone(),
                    body_text: comment.body.clone(),
                    author_login: comment.user.as_ref().map(|user| user.login.clone()),
                    author_id: comment.user.as_ref().map(|user| user.id.0.to_string()),
                    author_association: Some(Self::association_to_string(
                        &comment.author_association,
                    )),
                    created_at: comment.created_at,
                    updated_at: Some(comment.updated_at),
                    reactions: ReactionSummary::default(),
                    is_bot: comment
                        .user
                        .as_ref()
                        .map(|user| {
                            user.r#type == "Bot"
                                || user.login.to_ascii_lowercase().ends_with("[bot]")
                        })
                        .unwrap_or(false),
                    kind: CommentKind::Review,
                    in_reply_to_id: comment.in_reply_to_id.map(|id| id.0 as i64),
                    review_state: comment.pull_request_review_id.map(|id| id.0.to_string()),
                    path: Some(comment.path.clone()),
                    position: comment.position.map(|pos| pos as i64),
                }
            })
            .collect())
    }

    fn select_representative_comments(
        comments: &[CommentInfo],
        limit_hint: usize,
    ) -> (Vec<i64>, Option<String>) {
        let mut filtered: Vec<&CommentInfo> = comments
            .iter()
            .filter(|comment| {
                !comment.is_bot && !comment.body_text.trim().is_empty() && comment.id != 0
            })
            .collect();

        if filtered.is_empty() {
            return (Vec::new(), None);
        }

        filtered.sort_by_key(|comment| comment.created_at);
        let total = filtered.len();
        let mut seen = std::collections::HashSet::new();
        let mut scored: Vec<(i64, f32, String, CommentKind)> = Vec::new();

        for (idx, comment) in filtered.iter().enumerate() {
            let normalized = Self::normalize_comment_text(&comment.body_text);
            if normalized.is_empty() {
                continue;
            }
            if !seen.insert(normalized.clone()) {
                continue;
            }

            let reaction_score = Self::reaction_score(&comment.reactions);
            let author_weight =
                Self::author_weight(comment.author_association.as_deref().unwrap_or("NONE"));
            let position_weight = Self::position_weight(idx, total, comment);
            let recency_weight = Self::recency_weight(idx, total);
            let content_quality = Self::content_quality(&normalized);

            let score = 0.45 * reaction_score
                + 0.20 * author_weight
                + 0.15 * position_weight
                + 0.10 * recency_weight
                + 0.10 * content_quality;

            scored.push((comment.id, score, comment.body_text.clone(), comment.kind));
        }

        if scored.is_empty() {
            return (Vec::new(), None);
        }

        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        let desired = limit_hint.clamp(3, 8);
        let threshold = 0.42;
        let mut selected: Vec<(i64, f32, String, CommentKind)> = scored
            .iter()
            .cloned()
            .filter(|(_, score, _, _)| *score >= threshold)
            .take(desired)
            .collect();

        if selected.len() < 3 {
            selected = scored.iter().cloned().take(desired).collect();
        }

        let has_review_candidate = scored
            .iter()
            .any(|(_, _, _, kind)| matches!(kind, CommentKind::Review));
        let has_review_selected = selected
            .iter()
            .any(|(_, _, _, kind)| matches!(kind, CommentKind::Review));
        if has_review_candidate && !has_review_selected {
            if let Some(best_review) = scored
                .iter()
                .filter(|(_, _, _, kind)| matches!(kind, CommentKind::Review))
                .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            {
                if !selected.iter().any(|(id, _, _, _)| id == &best_review.0) {
                    selected.push(best_review.clone());
                    selected.sort_by(|a, b| {
                        b.1.partial_cmp(&a.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| a.0.cmp(&b.0))
                    });
                    selected.truncate(desired);
                }
            }
        }

        let ids: Vec<i64> = selected.iter().map(|(id, _, _, _)| *id).collect();

        let mut digest = String::new();
        for (_, _, text, _) in &selected {
            if digest.len() >= 4000 {
                break;
            }

            if !digest.is_empty() {
                digest.push_str("\n\n");
            }

            if digest.len() + text.len() > 4000 {
                let remaining = 4000 - digest.len();
                let mut truncated = text.chars().take(remaining).collect::<String>();
                truncated.push('…');
                digest.push_str(&truncated);
            } else {
                digest.push_str(text);
            }
        }

        let digest = if digest.is_empty() {
            None
        } else {
            Some(digest)
        };

        (ids, digest)
    }

    fn normalize_comment_text(text: &str) -> String {
        text.lines()
            .map(|line| line.trim())
            .filter(|line| !line.starts_with('>'))
            .flat_map(|line| line.split_whitespace())
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase()
    }

    fn reaction_score(summary: &ReactionSummary) -> f32 {
        let positive =
            summary.plus_one + summary.heart + summary.hooray + summary.eyes + summary.rocket;
        let negative = summary.confused;

        let positive_score = if positive == 0 {
            0.0
        } else {
            (positive as f32).ln_1p() / (20f32).ln_1p()
        };

        let negative_penalty = if negative == 0 {
            0.0
        } else {
            0.5 * ((negative as f32).ln_1p() / (5f32).ln_1p())
        };

        (positive_score - negative_penalty).clamp(-1.0, 1.0)
    }

    fn author_weight(association: &str) -> f32 {
        match association {
            "OWNER" => 1.0,
            "MEMBER" => 0.9,
            "COLLABORATOR" => 0.8,
            "CONTRIBUTOR" => 0.7,
            "FIRST_TIMER" => 0.6,
            "FIRST_TIME_CONTRIBUTOR" => 0.6,
            "NONE" => 0.5,
            other if !other.is_empty() => 0.6,
            _ => 0.5,
        }
    }

    fn position_weight(index: usize, _total: usize, comment: &CommentInfo) -> f32 {
        let mut weight: f32 = if index == 0 { 0.6 } else { 0.3 };
        let text = comment.body_text.to_lowercase();
        let keywords = [
            "fix",
            "fixed",
            "resolve",
            "resolved",
            "workaround",
            "duplicate",
        ];
        if keywords.iter().any(|kw| text.contains(kw)) {
            weight += 0.3;
        }
        weight.clamp(0.0, 1.0)
    }

    fn recency_weight(index: usize, total: usize) -> f32 {
        if total <= 1 {
            1.0
        } else {
            (index as f32) / ((total - 1) as f32)
        }
    }

    fn content_quality(normalized: &str) -> f32 {
        let length = normalized.len();
        if length <= 80 {
            (length as f32) / 80.0
        } else if length >= 1200 {
            1200.0 / (length as f32)
        } else {
            1.0
        }
        .clamp(0.0, 1.0)
    }

    fn extract_issue_links(
        text: &str,
        default_owner: &str,
        default_repo: &str,
        origin: &str,
    ) -> Vec<IssueRelation> {
        let mut results = Vec::new();
        let text_lower = text.to_lowercase();
        let mut offset = 0;

        while let Some(pos) = text[offset..].find('#') {
            let absolute = offset + pos;
            if absolute + 1 >= text.len() {
                break;
            }

            if absolute > 0 {
                let prev = text[..absolute].chars().rev().next().unwrap_or(' ');
                if prev.is_ascii_alphanumeric() || prev == '/' || prev == '-' {
                    offset = absolute + 1;
                    continue;
                }
            }

            let mut end = absolute + 1;
            while end < text.len() && text.as_bytes()[end].is_ascii_digit() {
                end += 1;
            }
            if end == absolute + 1 {
                offset = absolute + 1;
                continue;
            }

            let number_str = &text[absolute + 1..end];
            if number_str.len() > 7 {
                offset = end;
                continue;
            }

            if let Ok(number) = number_str.parse::<i64>() {
                let (owner, repo) =
                    Self::parse_issue_prefix(text, absolute, default_owner, default_repo);
                let (link_type, strength) =
                    Self::classify_reference(&text_lower, absolute, number_str.len());

                results.push(IssueRelation {
                    owner,
                    repo,
                    number,
                    link_type: link_type.to_string(),
                    strength,
                    origin: origin.to_string(),
                    cross_repo: false,
                });
            }

            offset = end;
        }

        for relation in &mut results {
            relation.cross_repo = !(relation.owner.eq_ignore_ascii_case(default_owner)
                && relation.repo.eq_ignore_ascii_case(default_repo));
        }

        results
    }

    fn parse_issue_prefix(
        text: &str,
        hash_index: usize,
        default_owner: &str,
        default_repo: &str,
    ) -> (String, String) {
        let mut start = hash_index;
        while start > 0 {
            let ch = text.as_bytes()[start - 1] as char;
            if ch.is_ascii_whitespace() || matches!(ch, '(' | '[' | '{' | ':' | ',') {
                break;
            }
            start -= 1;
        }

        let prefix = text[start..hash_index]
            .trim_matches(|c: char| matches!(c, '(' | '[' | '{' | ':' | ',' | ';'));

        if prefix.is_empty() || prefix.contains("://") {
            return (default_owner.to_string(), default_repo.to_string());
        }

        if let Some((owner, repo)) = prefix.rsplit_once('/') {
            if !owner.is_empty() && !repo.is_empty() {
                return (owner.to_string(), repo.to_string());
            }
        } else if prefix
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return (default_owner.to_string(), prefix.to_string());
        }

        (default_owner.to_string(), default_repo.to_string())
    }

    fn classify_reference(
        text_lower: &str,
        hash_index: usize,
        number_len: usize,
    ) -> (&'static str, u8) {
        let window_start = hash_index.saturating_sub(40);
        let window_end = (hash_index + 1 + number_len + 40).min(text_lower.len());
        let before = &text_lower[window_start..hash_index];
        let after = &text_lower[hash_index + 1 + number_len..window_end];

        let closing_keywords = [
            "close", "closes", "closed", "fix", "fixes", "fixed", "resolve", "resolves", "resolved",
        ];
        if closing_keywords
            .iter()
            .any(|kw| before.contains(kw) || after.contains(kw))
        {
            return ("closes", 3);
        }

        if before.contains("duplicate") || after.contains("duplicate") {
            return ("duplicate", 3);
        }

        let reference_keywords = ["reference", "references", "ref", "refs", "see", "related"];
        if reference_keywords
            .iter()
            .any(|kw| before.contains(kw) || after.contains(kw))
        {
            return ("references", 2);
        }

        ("mentions", 1)
    }

    fn ensure_developer(
        developers: &mut HashMap<String, DeveloperProfile>,
        author: &octocrab::models::Author,
    ) {
        let account_id = author.id.0.to_string();
        developers
            .entry(account_id.clone())
            .or_insert_with(|| DeveloperProfile {
                platform: "github".to_string(),
                account_id,
                login: author.login.clone(),
                name: None,
                company: None,
                followers: None,
                following: None,
                location: None,
                email: None,
                created_at: None,
                updated_at: None,
            });
    }

    fn map_label(label: &octocrab::models::Label) -> LabelInfo {
        LabelInfo {
            name: label.name.clone(),
            color: Some(label.color.clone()),
            description: label.description.clone(),
        }
    }

    fn association_to_string(value: &octocrab::models::AuthorAssociation) -> String {
        match value {
            octocrab::models::AuthorAssociation::Other(other) => other.clone(),
            other => format!("{other:?}"),
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

        let project_url = repository.html_url.clone();
        let mut developers_map = HashMap::new();

        let issues = if params.include_issues {
            self.load_issues(owner, repo, &project_url, params, &mut developers_map)
                .await?
        } else {
            Vec::new()
        };

        let pull_requests = if params.include_pulls {
            self.load_pull_requests(owner, repo, &project_url, params, &mut developers_map)
                .await?
        } else {
            Vec::new()
        };

        let developers = developers_map.into_values().collect();

        Ok(RepoSnapshot {
            repository,
            revision,
            commit,
            readme,
            developers,
            issues,
            pull_requests,
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
