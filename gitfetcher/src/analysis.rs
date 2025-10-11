use crate::error::Result;
use crate::models::{NetworkData, RepoDependency, RepoFork, UserCollaboration, UserFollowRelation};
use std::collections::{HashMap, HashSet};

/// Basic network analysis metrics
pub struct NetworkMetrics {
    pub node_count: usize,
    pub edge_count: usize,
    pub density: f64,
    pub avg_degree: f64,
    pub max_degree: usize,
    pub degree_distribution: HashMap<usize, usize>,
    pub connected_components: usize,
}

/// Calculate basic metrics for user follow network
pub fn analyze_user_follow_network(relations: &[UserFollowRelation]) -> Result<NetworkMetrics> {
    let mut graph = HashMap::new();
    let mut nodes = HashSet::new();

    // Build adjacency list
    for rel in relations {
        nodes.insert(&rel.follower_login);
        nodes.insert(&rel.followed_login);
        graph
            .entry(&rel.follower_login)
            .or_insert_with(HashSet::new)
            .insert(&rel.followed_login);
    }

    calculate_network_metrics(&graph, nodes.len())
}

/// Calculate basic metrics for repository fork network
pub fn analyze_repo_fork_network(forks: &[RepoFork]) -> Result<NetworkMetrics> {
    let mut graph = HashMap::new();
    let mut nodes = HashSet::new();

    for fork in forks {
        nodes.insert(&fork.source_repo_name);
        nodes.insert(&fork.fork_repo_name);
        graph
            .entry(&fork.source_repo_name)
            .or_insert_with(HashSet::new)
            .insert(&fork.fork_repo_name);
    }

    calculate_network_metrics(&graph, nodes.len())
}

/// Calculate basic metrics for collaboration network
pub fn analyze_collaboration_network(
    collaborations: &[UserCollaboration],
) -> Result<NetworkMetrics> {
    let mut graph = HashMap::new();
    let mut nodes = HashSet::new();

    for collab in collaborations {
        nodes.insert(&collab.user_login);
        nodes.insert(&collab.repo_name);
        graph
            .entry(&collab.user_login)
            .or_insert_with(HashSet::new)
            .insert(&collab.repo_name);
    }

    calculate_network_metrics(&graph, nodes.len())
}

/// Generic network metrics calculation
fn calculate_network_metrics(
    graph: &HashMap<&String, HashSet<&String>>,
    node_count: usize,
) -> Result<NetworkMetrics> {
    let edge_count = graph
        .values()
        .map(|neighbors| neighbors.len())
        .sum::<usize>();

    // Calculate degree distribution
    let mut degree_dist = HashMap::new();
    let mut degrees = Vec::new();

    for node in graph.keys() {
        let degree = graph
            .get(node)
            .map(|neighbors| neighbors.len())
            .unwrap_or(0);
        degrees.push(degree);
        *degree_dist.entry(degree).or_insert(0) += 1;
    }

    // Add nodes with degree 0 that aren't in the graph
    let nodes_with_edges = graph.len();
    if node_count > nodes_with_edges {
        *degree_dist.entry(0).or_insert(0) += node_count - nodes_with_edges;
        for _ in 0..(node_count - nodes_with_edges) {
            degrees.push(0);
        }
    }

    let avg_degree = if node_count > 0 {
        degrees.iter().sum::<usize>() as f64 / node_count as f64
    } else {
        0.0
    };

    let max_degree = degrees.iter().max().copied().unwrap_or(0);

    // Calculate density for undirected graph
    let max_possible_edges = node_count * (node_count - 1) / 2;
    let density = if max_possible_edges > 0 {
        edge_count as f64 / max_possible_edges as f64
    } else {
        0.0
    };

    // Simple connected components calculation (simplified)
    let connected_components = if node_count == 0 {
        0
    } else if edge_count == 0 {
        node_count
    } else {
        // This is a simplified approximation
        // For accurate calculation, you'd need BFS/DFS
        1.max(node_count.saturating_sub(edge_count / 2))
    };

    Ok(NetworkMetrics {
        node_count,
        edge_count,
        density,
        avg_degree,
        max_degree,
        degree_distribution: degree_dist,
        connected_components,
    })
}

/// Find top nodes by degree (influence)
pub fn find_top_influential_users(
    relations: &[UserFollowRelation],
    top_n: usize,
) -> Vec<(String, usize)> {
    let mut user_degrees = HashMap::new();

    // Count followers (in-degree for followed users)
    for rel in relations {
        *user_degrees.entry(&rel.followed_login).or_insert(0) += 1;
    }

    // Sort by degree (descending)
    let mut sorted: Vec<_> = user_degrees
        .into_iter()
        .map(|(login, degree)| (login.clone(), degree))
        .collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));

    sorted.truncate(top_n);
    sorted
}

/// Find most forked repositories
pub fn find_most_forked_repos(forks: &[RepoFork], top_n: usize) -> Vec<(String, usize)> {
    let mut fork_counts = HashMap::new();

    for fork in forks {
        *fork_counts.entry(&fork.source_repo_name).or_insert(0) += 1;
    }

    let mut sorted: Vec<_> = fork_counts
        .into_iter()
        .map(|(repo, count)| (repo.clone(), count))
        .collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));

    sorted.truncate(top_n);
    sorted
}

/// Find most active collaborators
pub fn find_most_active_collaborators(
    collaborations: &[UserCollaboration],
    top_n: usize,
) -> Vec<(String, usize)> {
    let mut activity_counts = HashMap::new();

    for collab in collaborations {
        *activity_counts.entry(&collab.user_login).or_insert(0) += 1;
    }

    let mut sorted: Vec<_> = activity_counts
        .into_iter()
        .map(|(user, count)| (user.clone(), count))
        .collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));

    sorted.truncate(top_n);
    sorted
}

/// Generate a summary report for network data
pub fn generate_network_summary(network_data: &NetworkData) -> Result<String> {
    let mut report = String::new();

    report.push_str("=== GitHub Network Analysis Report ===\n\n");

    // User Follow Network
    if !network_data.user_follow_relations.is_empty() {
        let metrics = analyze_user_follow_network(&network_data.user_follow_relations)?;
        let top_users = find_top_influential_users(&network_data.user_follow_relations, 5);

        report.push_str("User Follow Network:\n");
        report.push_str(&format!("  Nodes: {}\n", metrics.node_count));
        report.push_str(&format!("  Edges: {}\n", metrics.edge_count));
        report.push_str(&format!("  Density: {:.3}\n", metrics.density));
        report.push_str(&format!("  Average Degree: {:.2}\n", metrics.avg_degree));
        report.push_str(&format!(
            "  Connected Components: {}\n",
            metrics.connected_components
        ));

        report.push_str("\n  Most Followed Users:\n");
        for (user, followers) in top_users {
            report.push_str(&format!("    {}: {} followers\n", user, followers));
        }
        report.push_str("\n");
    }

    // Repository Fork Network
    if !network_data.repo_forks.is_empty() {
        let metrics = analyze_repo_fork_network(&network_data.repo_forks)?;
        let top_repos = find_most_forked_repos(&network_data.repo_forks, 5);

        report.push_str("Repository Fork Network:\n");
        report.push_str(&format!("  Nodes: {}\n", metrics.node_count));
        report.push_str(&format!("  Edges: {}\n", metrics.edge_count));
        report.push_str(&format!("  Density: {:.3}\n", metrics.density));
        report.push_str(&format!("  Average Degree: {:.2}\n", metrics.avg_degree));

        report.push_str("\n  Most Forked Repositories:\n");
        for (repo, forks) in top_repos {
            report.push_str(&format!("    {}: {} forks\n", repo, forks));
        }
        report.push_str("\n");
    }

    // User Collaboration Network
    if !network_data.user_collaborations.is_empty() {
        let metrics = analyze_collaboration_network(&network_data.user_collaborations)?;
        let top_collaborators =
            find_most_active_collaborators(&network_data.user_collaborations, 5);

        report.push_str("User Collaboration Network:\n");
        report.push_str(&format!("  Nodes: {}\n", metrics.node_count));
        report.push_str(&format!("  Edges: {}\n", metrics.edge_count));
        report.push_str(&format!("  Density: {:.3}\n", metrics.density));
        report.push_str(&format!("  Average Degree: {:.2}\n", metrics.avg_degree));

        report.push_str("\n  Most Active Collaborators:\n");
        for (user, activities) in top_collaborators {
            report.push_str(&format!("    {}: {} activities\n", user, activities));
        }
        report.push_str("\n");
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::UserFollowRelation;

    #[test]
    fn test_analyze_user_follow_network() {
        let relations = vec![
            UserFollowRelation {
                follower_id: 1,
                follower_login: "user1".to_string(),
                followed_id: 2,
                followed_login: "user2".to_string(),
                created_at: "2025-01-01".to_string(),
            },
            UserFollowRelation {
                follower_id: 1,
                follower_login: "user1".to_string(),
                followed_id: 3,
                followed_login: "user3".to_string(),
                created_at: "2025-01-01".to_string(),
            },
            UserFollowRelation {
                follower_id: 2,
                follower_login: "user2".to_string(),
                followed_id: 3,
                followed_login: "user3".to_string(),
                created_at: "2025-01-01".to_string(),
            },
        ];

        let metrics = analyze_user_follow_network(&relations).unwrap();

        assert_eq!(metrics.node_count, 3);
        assert_eq!(metrics.edge_count, 3);
        assert!(metrics.density > 0.0 && metrics.density <= 1.0);
        assert_eq!(metrics.max_degree, 2); // user3 has 2 followers
    }

    #[test]
    fn test_find_top_influential_users() {
        let relations = vec![
            UserFollowRelation {
                follower_id: 1,
                follower_login: "user1".to_string(),
                followed_id: 2,
                followed_login: "popular".to_string(),
                created_at: "2025-01-01".to_string(),
            },
            UserFollowRelation {
                follower_id: 2,
                follower_login: "user2".to_string(),
                followed_id: 3,
                followed_login: "popular".to_string(),
                created_at: "2025-01-01".to_string(),
            },
            UserFollowRelation {
                follower_id: 3,
                follower_login: "user3".to_string(),
                followed_id: 4,
                followed_login: "normal".to_string(),
                created_at: "2025-01-01".to_string(),
            },
        ];

        let top_users = find_top_influential_users(&relations, 5);

        assert_eq!(top_users.len(), 2);
        assert_eq!(top_users[0], ("popular".to_string(), 2));
        assert_eq!(top_users[1], ("normal".to_string(), 1));
    }
}
