use crate::client::Fetcher;
use crate::error::Result;
use crate::models::{
    UserFollowRelation, RepoDependency, RepoFork, UserCollaboration, NetworkData,
};
use chrono;

/// Fetches user follow network for a given user with specified depth
pub async fn fetch_user_follow_network(
    fetcher: &Fetcher,
    username: &str,
    depth: usize,
    max_users: Option<usize>,
) -> Result<Vec<UserFollowRelation>> {
    let mut relations = Vec::new();
    let mut processed_users = std::collections::HashSet::new();
    let mut queue = std::collections::VecDeque::new();
    
    // Start with the target user - skip user details for now to avoid API issues
    // We'll start processing with just the username
    queue.push_back((0, username.to_string(), 0)); // Use 0 as placeholder ID
    
    while let Some((current_user_id, current_user_login, current_depth)) = queue.pop_front() {
        if current_depth >= depth {
            continue;
        }
        
        // Rate limiting protection
        if processed_users.len() % 100 == 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        }
        
        // Fetch followers
        let mut followers_page = fetcher
            .octocrab
            .users(&current_user_login)
            .followers()
            .per_page(100)
            .send()
            .await?;
        
        let mut follower_count = 0;
        loop {
            for follower in &followers_page.items {
                if max_users.map_or(false, |max| processed_users.len() >= max) {
                    break;
                }
                
                relations.push(UserFollowRelation {
                    follower_id: follower.id.0,
                    follower_login: follower.login.clone(),
                    followed_id: current_user_id,
                    followed_login: current_user_login.clone(),
                    created_at: chrono::Utc::now().to_rfc3339(), // Follower doesn't have created_at
                });
                
                if !processed_users.contains(&follower.id.0) && current_depth + 1 < depth {
                    processed_users.insert(follower.id.0);
                    queue.push_back((follower.id.0, follower.login.clone(), current_depth + 1));
                }
                
                follower_count += 1;
            }
            
            let next_page = fetcher.octocrab.get_page(&followers_page.next).await?;
            match next_page {
                Some(new_page) => followers_page = new_page,
                None => break,
            }
        }
        
        // Fetch following
        let mut following_page = fetcher
            .octocrab
            .users(&current_user_login)
            .following()
            .per_page(100)
            .send()
            .await?;
        
        let mut following_count = 0;
        loop {
            for followed in &following_page.items {
                if max_users.map_or(false, |max| processed_users.len() >= max) {
                    break;
                }
                
                relations.push(UserFollowRelation {
                    follower_id: current_user_id,
                    follower_login: current_user_login.clone(),
                    followed_id: followed.id.0,
                    followed_login: followed.login.clone(),
                    created_at: chrono::Utc::now().to_rfc3339(), // Followed doesn't have created_at
                });
                
                if !processed_users.contains(&followed.id.0) && current_depth + 1 < depth {
                    processed_users.insert(followed.id.0);
                    queue.push_back((followed.id.0, followed.login.clone(), current_depth + 1));
                }
                
                following_count += 1;
            }
            
            let next_page = fetcher.octocrab.get_page(&following_page.next).await?;
            match next_page {
                Some(new_page) => following_page = new_page,
                None => break,
            }
        }
        
        println!("Processed user {} (depth {}): {} followers, {} following", 
                current_user_login, current_depth, follower_count, following_count);
    }
    
    Ok(relations)
}

/// Fetches repository fork network
pub async fn fetch_repo_fork_network(
    fetcher: &Fetcher,
    owner: &str,
    repo: &str,
    depth: usize,
    max_forks: Option<usize>,
) -> Result<Vec<RepoFork>> {
    let mut forks = Vec::new();
    let mut processed_repos = std::collections::HashSet::new();
    let queue: std::collections::VecDeque<(u64, String, usize)> = std::collections::VecDeque::new();
    
    // Start with the target repository
    let repo_info = fetcher.octocrab.repos(owner, repo).get().await?;
    let source_repo_id = repo_info.id.0;
    let _source_repo_name = format!("{}/{}", owner, repo);
    processed_repos.insert(source_repo_id);
    
    // Add immediate forks
    let initial_forks = fetch_repo_forks_recursive(fetcher, owner, repo, 1, depth, max_forks, &mut processed_repos, &mut forks).await?;
    forks.extend(initial_forks);
    
    Ok(forks)
}

async fn fetch_repo_forks_recursive(
    fetcher: &Fetcher,
    owner: &str,
    repo: &str,
    current_depth: usize,
    max_depth: usize,
    max_forks: Option<usize>,
    processed_repos: &mut std::collections::HashSet<u64>,
    forks: &mut Vec<RepoFork>,
) -> Result<Vec<RepoFork>> {
    if current_depth > max_depth {
        return Ok(Vec::new());
    }
    
    let mut result = Vec::new();
    
    let mut forks_page = fetcher
        .octocrab
        .repos(owner, repo)
        .list_forks()
        .per_page(100)
        .send()
        .await?;
    
    loop {
        for fork in &forks_page.items {
            if max_forks.map_or(false, |max| forks.len() >= max) {
                break;
            }
            
            let fork_repo_name = format!("{}/{}", fork.owner.as_ref().unwrap().login, fork.name);
            
            let fork_info = RepoFork {
                source_repo_id: fork.parent.as_ref().map(|p| p.id.0).unwrap_or(0),
                source_repo_name: fork.parent.as_ref()
                    .map(|p| format!("{}/{}", p.owner.as_ref().unwrap().login, p.name))
                    .unwrap_or_else(|| format!("{}/{}", owner, repo)),
                fork_repo_id: fork.id.0,
                fork_repo_name: fork_repo_name.clone(),
                fork_owner_login: fork.owner.as_ref().unwrap().login.clone(),
                created_at: fork.created_at.unwrap_or_default().to_rfc3339(),
            };
            
            result.push(fork_info.clone());
            forks.push(fork_info);
            
            // Note: Recursive fork fetching disabled to avoid async recursion issues
            // In a production implementation, you would use iterative approach or Box::pin
        }
        
        let next_page = fetcher.octocrab.get_page(&forks_page.next).await?;
        match next_page {
            Some(new_page) => forks_page = new_page,
            None => break,
        }
    }
    
    Ok(result)
}

/// Fetches repository dependencies from package files (simplified version)
pub async fn fetch_repo_dependencies(
    _fetcher: &Fetcher,
    _owner: &str,
    _repo: &str,
) -> Result<Vec<RepoDependency>> {
    let dependencies = Vec::new();
    
    // This is a simplified implementation - in a real scenario, you would:
    // 1. Parse package.json, Cargo.toml, requirements.txt, etc.
    // 2. Extract GitHub dependencies
    // 3. Map them to repository information
    
    // For now, we'll just return empty data as this requires more complex parsing
    // In a complete implementation, you would:
    // - Fetch repository contents
    // - Parse dependency files
    // - Resolve GitHub URLs to repository IDs
    
    println!("Note: Dependency parsing requires file content parsing. This is a placeholder implementation.");
    
    Ok(dependencies)
}

/// Fetches user collaboration network based on issues, PRs, and commits
pub async fn fetch_user_collaboration_network(
    fetcher: &Fetcher,
    owner: &str,
    repo: &str,
    max_collaborations: Option<usize>,
) -> Result<Vec<UserCollaboration>> {
    let mut collaborations = Vec::new();
    
    // Get repository info
    let repo_info = fetcher.octocrab.repos(owner, repo).get().await?;
    let repo_id = repo_info.id.0;
    let repo_name = format!("{}/{}", owner, repo);
    
    // Fetch issue creators
    let mut issues_page = fetcher
        .octocrab
        .issues(owner, repo)
        .list()
        .state(octocrab::params::State::All)
        .per_page(100)
        .send()
        .await?;
    
    loop {
        for issue in &issues_page.items {
            if max_collaborations.map_or(false, |max| collaborations.len() >= max) {
                break;
            }
            
            if issue.pull_request.is_none() { // Only count issues, not PRs
                collaborations.push(UserCollaboration {
                    user_id: issue.user.id.0,
                    user_login: issue.user.login.clone(),
                    repo_id,
                    repo_name: repo_name.clone(),
                    collaboration_type: "issue".to_string(),
                    created_at: issue.created_at.to_rfc3339(),
                });
            }
        }
        
        let next_page = fetcher.octocrab.get_page(&issues_page.next).await?;
        match next_page {
            Some(new_page) => issues_page = new_page,
            None => break,
        }
    }
    
    // Fetch PR creators
    let mut prs_page = fetcher
        .octocrab
        .pulls(owner, repo)
        .list()
        .state(octocrab::params::State::All)
        .per_page(100)
        .send()
        .await?;
    
    loop {
        for pr in &prs_page.items {
            if max_collaborations.map_or(false, |max| collaborations.len() >= max) {
                break;
            }
            
            collaborations.push(UserCollaboration {
                user_id: pr.user.as_ref().map(|u| u.id.0).unwrap_or(0),
                user_login: pr.user.as_ref().map(|u| u.login.clone()).unwrap_or_default(),
                repo_id,
                repo_name: repo_name.clone(),
                collaboration_type: "pull_request".to_string(),
                created_at: pr.created_at.unwrap_or_default().to_rfc3339(),
            });
        }
        
        let next_page = fetcher.octocrab.get_page(&prs_page.next).await?;
        match next_page {
            Some(new_page) => prs_page = new_page,
            None => break,
        }
    }
    
    // Fetch commit authors
    let mut commits_page = fetcher
        .octocrab
        .repos(owner, repo)
        .list_commits()
        .per_page(100)
        .send()
        .await?;
    
    loop {
        for commit in &commits_page {
            if max_collaborations.map_or(false, |max| collaborations.len() >= max) {
                break;
            }
            
            if let Some(ref author) = commit.author {
                collaborations.push(UserCollaboration {
                    user_id: author.id.0,
                    user_login: author.login.clone(),
                    repo_id,
                    repo_name: repo_name.clone(),
                    collaboration_type: "commit".to_string(),
                    created_at: commit.commit.author.as_ref()
                        .and_then(|a| a.date)
                        .unwrap_or_default()
                        .to_rfc3339(),
                });
            }
        }
        
        let next_page = fetcher.octocrab.get_page(&commits_page.next).await?;
        match next_page {
            Some(new_page) => commits_page = new_page,
            None => break,
        }
    }
    
    Ok(collaborations)
}

/// Fetches all network data for a repository
pub async fn fetch_all_network_data(
    fetcher: &Fetcher,
    owner: &str,
    repo: &str,
    user_depth: usize,
    fork_depth: usize,
    max_items: Option<usize>,
) -> Result<NetworkData> {
    let repo_info = fetcher.octocrab.repos(owner, repo).get().await?;
    
    // Fetch user collaboration network
    let user_collaborations = fetch_user_collaboration_network(
        fetcher, owner, repo, max_items
    ).await?;
    
    // Fetch fork network
    let repo_forks = fetch_repo_fork_network(
        fetcher, owner, repo, fork_depth, max_items
    ).await?;
    
    // Fetch repository owner's follow network (limited scope)
    let user_follow_relations = if user_depth > 0 {
        fetch_user_follow_network(
            fetcher, &repo_info.owner.as_ref().unwrap().login, user_depth, max_items
        ).await?
    } else {
        Vec::new()
    };
    
    // Fetch dependencies (placeholder)
    let repo_dependencies = fetch_repo_dependencies(fetcher, owner, repo).await?;
    
    Ok(NetworkData {
        user_follow_relations,
        repo_dependencies,
        repo_forks,
        user_collaborations,
    })
}