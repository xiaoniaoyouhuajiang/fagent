pub mod analysis;
pub mod client;
pub mod error;
pub mod export;
pub mod fetch;
pub mod models;
pub mod network;

pub use analysis::{
    analyze_collaboration_network, analyze_repo_fork_network, analyze_user_follow_network,
    find_most_active_collaborators, find_most_forked_repos, find_top_influential_users,
    generate_network_summary, NetworkMetrics,
};
pub use models::{
    DetailLevel, NetworkData, OutputFormat, RepoData, RepoDependency, RepoFork, UserCollaboration,
    UserFollowRelation,
};

use crate::client::Fetcher;
use crate::error::Result;
use std::path::Path;

/// Fetches all data for a repository and saves it to files in the specified format.
///
/// This is the main entry point function for the library.
pub async fn run(
    owner: &str,
    repo: &str,
    token: Option<String>,
    format: OutputFormat,
    level: DetailLevel,
    output_dir: &str,
) -> Result<()> {
    // Ensure output directory exists
    std::fs::create_dir_all(output_dir)?;

    let data = fetch_to_memory(owner, repo, token, level).await?;

    let extension = match format {
        OutputFormat::Json => "json",
        OutputFormat::Csv => "csv",
    };

    if !data.issues.is_empty() {
        let path = Path::new(output_dir).join(format!("issues.{}", extension));
        save_data(&data.issues, &path.to_string_lossy(), format)?;
    }

    if !data.pull_requests.is_empty() {
        let path = Path::new(output_dir).join(format!("pull_requests.{}", extension));
        save_data(&data.pull_requests, &path.to_string_lossy(), format)?;
    }

    if !data.commits.is_empty() {
        let path = Path::new(output_dir).join(format!("commits.{}", extension));
        save_data(&data.commits, &path.to_string_lossy(), format)?;
    }

    Ok(())
}

/// Fetches all data for a repository and returns it as an in-memory `RepoData` struct.
pub async fn fetch_to_memory(
    owner: &str,
    repo: &str,
    token: Option<String>,
    level: DetailLevel,
) -> Result<RepoData> {
    let fetcher = Fetcher::new(token)?;
    fetch::fetch_all(&fetcher, owner, repo, level).await
}

/// Fetches network data for a repository and returns it as an in-memory `NetworkData` struct.
pub async fn fetch_network_to_memory(
    owner: &str,
    repo: &str,
    token: Option<String>,
    user_depth: usize,
    fork_depth: usize,
    max_items: Option<usize>,
) -> Result<NetworkData> {
    let fetcher = Fetcher::new(token)?;
    network::fetch_all_network_data(&fetcher, owner, repo, user_depth, fork_depth, max_items).await
}

/// Fetches user follow network data.
pub async fn fetch_user_follow_network(
    username: &str,
    token: Option<String>,
    depth: usize,
    max_users: Option<usize>,
) -> Result<Vec<UserFollowRelation>> {
    let fetcher = Fetcher::new(token)?;
    network::fetch_user_follow_network(&fetcher, username, depth, max_users).await
}

/// Fetches repository fork network data.
pub async fn fetch_repo_fork_network(
    owner: &str,
    repo: &str,
    token: Option<String>,
    depth: usize,
    max_forks: Option<usize>,
) -> Result<Vec<RepoFork>> {
    let fetcher = Fetcher::new(token)?;
    network::fetch_repo_fork_network(&fetcher, owner, repo, depth, max_forks).await
}

/// Fetches user collaboration network data.
pub async fn fetch_user_collaboration_network(
    owner: &str,
    repo: &str,
    token: Option<String>,
    max_collaborations: Option<usize>,
) -> Result<Vec<UserCollaboration>> {
    let fetcher = Fetcher::new(token)?;
    network::fetch_user_collaboration_network(&fetcher, owner, repo, max_collaborations).await
}

/// Fetches network data and saves it to files in the specified format.
pub async fn run_network_fetch(
    owner: &str,
    repo: &str,
    token: Option<String>,
    user_depth: usize,
    fork_depth: usize,
    max_items: Option<usize>,
    format: &str,
    output_dir: &str,
) -> Result<()> {
    std::fs::create_dir_all(output_dir)?;

    let network_data =
        fetch_network_to_memory(owner, repo, token, user_depth, fork_depth, max_items).await?;

    export::save_network_data(&network_data, output_dir, format)?;

    Ok(())
}

/// Helper function to save data based on the selected format.
fn save_data<T: serde::Serialize>(data: &[T], path: &str, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => export::save_to_json(data, path),
        OutputFormat::Csv => export::save_to_csv(data, path),
    }
}

// The existing tests in lib.rs can be removed as we have module-specific tests.
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
