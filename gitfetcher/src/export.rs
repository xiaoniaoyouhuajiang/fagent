use crate::error::Result;
use crate::models::{NetworkData, UserFollowRelation, RepoDependency, RepoFork, UserCollaboration};
use serde::Serialize;
use std::fs::File;
use std::io::Write;

/// Saves a slice of serializable data to a file in JSON format.
pub fn save_to_json<T: Serialize>(data: &[T], path: &str) -> Result<()> {
    let json_string = serde_json::to_string_pretty(data)?;
    let mut file = File::create(path)?;
    file.write_all(json_string.as_bytes())?;
    Ok(())
}

/// Saves a slice of serializable data to a file in CSV format.
pub fn save_to_csv<T: Serialize>(data: &[T], path: &str) -> Result<()> {
    let mut writer = csv::Writer::from_path(path)?;
    for item in data {
        writer.serialize(item)?;
    }
    writer.flush()?;
    Ok(())
}

// --- Network Data Export Functions ---

/// Saves network data to separate files for each network type
pub fn save_network_data(network_data: &NetworkData, output_dir: &str, format: &str) -> Result<()> {
    std::fs::create_dir_all(output_dir)?;
    
    let extension = match format {
        "json" => "json",
        "csv" => "csv",
        "gexf" => "gexf", // Will be implemented later
        "edgelist" => "txt",
        _ => "json",
    };
    
    if !network_data.user_follow_relations.is_empty() {
        let path = std::path::Path::new(output_dir).join(format!("user_follow_relations.{}", extension));
        save_network_edges(&network_data.user_follow_relations, &path.to_string_lossy(), format)?;
    }
    
    if !network_data.repo_dependencies.is_empty() {
        let path = std::path::Path::new(output_dir).join(format!("repo_dependencies.{}", extension));
        save_network_edges(&network_data.repo_dependencies, &path.to_string_lossy(), format)?;
    }
    
    if !network_data.repo_forks.is_empty() {
        let path = std::path::Path::new(output_dir).join(format!("repo_forks.{}", extension));
        save_network_edges(&network_data.repo_forks, &path.to_string_lossy(), format)?;
    }
    
    if !network_data.user_collaborations.is_empty() {
        let path = std::path::Path::new(output_dir).join(format!("user_collaborations.{}", extension));
        save_network_edges(&network_data.user_collaborations, &path.to_string_lossy(), format)?;
    }
    
    // Save combined network data as JSON
    let combined_path = std::path::Path::new(output_dir).join(format!("network_data.{}", extension));
    let json_string = serde_json::to_string_pretty(network_data)?;
    let mut file = std::fs::File::create(combined_path)?;
    file.write_all(json_string.as_bytes())?;
    
    Ok(())
}

/// Generic function to save network edge data in various formats
fn save_network_edges<T: NetworkEdge + serde::Serialize>(edges: &[T], path: &str, format: &str) -> Result<()> {
    match format {
        "json" => save_to_json(edges, path),
        "csv" => save_to_csv(edges, path),
        "edgelist" => save_to_edgelist(edges, path),
        "gexf" => save_to_gexf(edges, path), // Placeholder for now
        _ => save_to_json(edges, path),
    }
}

/// Saves edge data as a simple edge list (source,target[,weight])
fn save_to_edgelist<T: NetworkEdge>(edges: &[T], path: &str) -> Result<()> {
    let mut file = File::create(path)?;
    
    // Write header for weighted edges if available
    if edges.first().map_or(false, |e| e.weight().is_some()) {
        writeln!(file, "source,target,weight")?;
    } else {
        writeln!(file, "source,target")?;
    }
    
    for edge in edges {
        match edge.weight() {
            Some(weight) => writeln!(file, "{},{},{}", edge.source(), edge.target(), weight)?,
            None => writeln!(file, "{},{}", edge.source(), edge.target())?,
        }
    }
    
    Ok(())
}

/// Placeholder for GEXF format export
fn save_to_gexf<T: NetworkEdge>(_edges: &[T], _path: &str) -> Result<()> {
    // This would require a proper GEXF library implementation
    println!("GEXF export not yet implemented. Please use JSON or CSV format for now.");
    Ok(())
}

/// Trait for network edge data structures
pub trait NetworkEdge {
    fn source(&self) -> String;
    fn target(&self) -> String;
    fn weight(&self) -> Option<String> {
        None
    }
}

// Implement NetworkEdge for each network type
impl NetworkEdge for UserFollowRelation {
    fn source(&self) -> String {
        self.follower_login.clone()
    }
    
    fn target(&self) -> String {
        self.followed_login.clone()
    }
}

impl NetworkEdge for RepoDependency {
    fn source(&self) -> String {
        self.source_repo_name.clone()
    }
    
    fn target(&self) -> String {
        self.target_repo_name.clone()
    }
    
    fn weight(&self) -> Option<String> {
        Some(self.dependency_type.clone())
    }
}

impl NetworkEdge for RepoFork {
    fn source(&self) -> String {
        self.source_repo_name.clone()
    }
    
    fn target(&self) -> String {
        self.fork_repo_name.clone()
    }
}

impl NetworkEdge for UserCollaboration {
    fn source(&self) -> String {
        self.user_login.clone()
    }
    
    fn target(&self) -> String {
        self.repo_name.clone()
    }
    
    fn weight(&self) -> Option<String> {
        Some(self.collaboration_type.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Issue;
    use tempfile::tempdir;

    fn create_mock_issues() -> Vec<Issue> {
        vec![
            Issue {
                id: 1,
                number: 1,
                title: "Test Issue 1".to_string(),
                user: "user1".to_string(),
                state: "open".to_string(),
                created_at: "2025-01-01T00:00:00Z".to_string(),
                updated_at: "2025-01-01T00:00:00Z".to_string(),
                body: Some("Body 1".to_string()),
            },
            Issue {
                id: 2,
                number: 2,
                title: "Test Issue 2".to_string(),
                user: "user2".to_string(),
                state: "closed".to_string(),
                created_at: "2025-01-02T00:00:00Z".to_string(),
                updated_at: "2025-01-02T00:00:00Z".to_string(),
                body: Some("Body 2".to_string()),
            },
        ]
    }

    #[test]
    fn test_save_to_json() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.json");
        let issues = create_mock_issues();

        let result = save_to_json(&issues, file_path.to_str().unwrap());
        assert!(result.is_ok());

        let content = std::fs::read_to_string(file_path).unwrap();
        assert!(content.contains("Test Issue 1"));
    }

    #[test]
    fn test_save_to_csv() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.csv");
        let issues = create_mock_issues();

        let result = save_to_csv(&issues, file_path.to_str().unwrap());
        assert!(result.is_ok());

        let content = std::fs::read_to_string(file_path).unwrap();
        assert!(content.contains("id,number,title,user,state,created_at,updated_at,body"));
        assert!(content.contains("Test Issue 2"));
    }
}
