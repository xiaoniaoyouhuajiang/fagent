use crate::client::Fetcher;
use crate::error::Result;
use crate::models::{Commit, DetailLevel, Issue, PullRequest, RepoData};

/// Fetches issues for a given repository.
pub async fn fetch_issues(
    fetcher: &Fetcher,
    owner: &str,
    repo: &str,
    level: DetailLevel,
) -> Result<Vec<Issue>> {
    let mut issues = Vec::new();
    let mut page = fetcher
        .octocrab
        .issues(owner, repo)
        .list()
        .state(octocrab::params::State::All)
        .per_page(100)
        .send()
        .await?;

    loop {
        for issue in &page.items {
            issues.push(Issue {
                id: issue.id.0,
                number: issue.number,
                title: issue.title.clone(),
                user: issue.user.login.clone(),
                state: format!("{:?}", issue.state),
                created_at: issue.created_at.to_rfc3339(),
                updated_at: issue.updated_at.to_rfc3339(),
                body: if let DetailLevel::Full = level {
                    issue.body.clone()
                } else {
                    None
                },
            });
        }

        let next_page = fetcher.octocrab.get_page(&page.next).await?;
        match next_page {
            Some(new_page) => page = new_page,
            None => break,
        }
    }

    Ok(issues)
}

/// Fetches pull requests for a given repository.
pub async fn fetch_pull_requests(
    fetcher: &Fetcher,
    owner: &str,
    repo: &str,
    level: DetailLevel,
) -> Result<Vec<PullRequest>> {
    let mut pull_requests = Vec::new();
    let mut page = fetcher
        .octocrab
        .pulls(owner, repo)
        .list()
        .state(octocrab::params::State::All)
        .per_page(100)
        .send()
        .await?;

    loop {
        for pr in &page.items {
            pull_requests.push(PullRequest {
                id: pr.id.0,
                number: pr.number,
                title: pr.title.clone().unwrap_or_default(),
                user: pr
                    .user
                    .as_ref()
                    .map(|u| u.login.clone())
                    .unwrap_or_default(),
                state: format!("{:?}", pr.state),
                created_at: pr.created_at.unwrap_or_default().to_rfc3339(),
                updated_at: pr.updated_at.unwrap_or_default().to_rfc3339(),
                merged_at: pr.merged_at.as_ref().map(|date| date.to_rfc3339()),
                body: if let DetailLevel::Full = level {
                    pr.body.clone()
                } else {
                    None
                },
            });
        }

        let next_page = fetcher.octocrab.get_page(&page.next).await?;
        match next_page {
            Some(new_page) => page = new_page,
            None => break,
        }
    }

    Ok(pull_requests)
}

/// Fetches commits for a given repository.
pub async fn fetch_commits(
    fetcher: &Fetcher,
    owner: &str,
    repo: &str,
    _level: DetailLevel,
) -> Result<Vec<Commit>> {
    let mut commits = Vec::new();
    let mut page = fetcher
        .octocrab
        .repos(owner, repo)
        .list_commits()
        .per_page(100)
        .send()
        .await?;

    loop {
        for commit in &page.items {
            let author = if let Some(ref author_data) = commit.author {
                author_data.login.clone()
            } else {
                "unknown".to_string()
            };

            commits.push(Commit {
                sha: commit.sha.clone(),
                author,
                message: commit.commit.message.clone(),
                date: commit
                    .commit
                    .author
                    .as_ref()
                    .unwrap()
                    .date
                    .unwrap_or_default()
                    .to_rfc3339(),
            });
        }

        let next_page = fetcher.octocrab.get_page(&page.next).await?;
        match next_page {
            Some(new_page) => page = new_page,
            None => break,
        }
    }

    Ok(commits)
}

/// Fetches all data types for a given repository.
pub async fn fetch_all(
    fetcher: &Fetcher,
    owner: &str,
    repo: &str,
    level: DetailLevel,
) -> Result<RepoData> {
    let issues = fetch_issues(fetcher, owner, repo, level).await?;
    let pull_requests = fetch_pull_requests(fetcher, owner, repo, level).await?;
    let commits = fetch_commits(fetcher, owner, repo, level).await?;

    Ok(RepoData {
        issues,
        pull_requests,
        commits,
    })
}
