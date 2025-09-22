# GitFetcher Network Analysis Extension

This document describes the new network analysis capabilities added to GitFetcher, enabling extraction and analysis of GitHub social and dependency networks.

## Overview

GitFetcher now supports extracting and analyzing various types of network data from GitHub:

1. **User Follow Networks** - Maps follower/following relationships between users
2. **Repository Fork Networks** - Tracks how repositories are forked and derived from each other  
3. **User Collaboration Networks** - Analyzes collaboration patterns through issues, PRs, and commits
4. **Repository Dependency Networks** - Maps software dependencies between projects (placeholder implementation)

## New Data Models

### UserFollowRelation
Represents follower relationships between GitHub users:
```rust
pub struct UserFollowRelation {
    pub follower_id: u64,
    pub follower_login: String,
    pub followed_id: u64, 
    pub followed_login: String,
    pub created_at: String,
}
```

### RepoFork
Tracks repository fork relationships:
```rust
pub struct RepoFork {
    pub source_repo_id: u64,
    pub source_repo_name: String,
    pub fork_repo_id: u64,
    pub fork_repo_name: String,
    pub fork_owner_login: String,
    pub created_at: String,
}
```

### UserCollaboration
Captures user collaboration on repositories:
```rust
pub struct UserCollaboration {
    pub user_id: u64,
    pub user_login: String,
    pub repo_id: u64,
    pub repo_name: String,
    pub collaboration_type: String, // "issue", "pull_request", "commit", "comment"
    pub created_at: String,
}
```

## Core API Functions

### Network Data Fetching

```rust
// Fetch comprehensive network data for a repository
let network_data = gitfetcher::fetch_network_to_memory(
    "owner", 
    "repo", 
    Some(token), // Optional GitHub token
    1,           // User follow depth
    1,           // Fork depth
    Some(100)    // Maximum items to fetch
).await?;

// Fetch specific network types
let follow_network = gitfetcher::fetch_user_follow_network(
    "username", 
    Some(token), 
    2,            // Depth 
    Some(100)     // Max users
).await?;

let fork_network = gitfetcher::fetch_repo_fork_network(
    "owner", 
    "repo", 
    Some(token),
    2,            // Depth
    Some(50)      // Max forks
).await?;

let collab_network = gitfetcher::fetch_user_collaboration_network(
    "owner", 
    "repo", 
    Some(token),
    Some(200)     // Max collaborations
).await?;
```

### Network Analysis

```rust
// Calculate network metrics
let metrics = gitfetcher::analyze_user_follow_network(&follow_network)?;
println!("Network density: {:.3}", metrics.density);
println!("Average degree: {:.2}", metrics.avg_degree);

// Find influential users
let top_users = gitfetcher::find_top_influential_users(&follow_network, 10);
for (user, followers) in top_users {
    println!("{} has {} followers", user, followers);
}

// Find most forked repositories  
let top_repos = gitfetcher::find_most_forked_repos(&fork_network, 10);
for (repo, forks) in top_repos {
    println!("{} has {} forks", repo, forks);
}

// Find most active collaborators
let top_collaborators = gitfetcher::find_most_active_collaborators(&collab_network, 10);
for (user, activities) in top_collaborators {
    println!("{} participated in {} activities", user, activities);
}

// Generate comprehensive report
let report = gitfetcher::generate_network_summary(&network_data)?;
println!("{}", report);
```

### Data Export

```rust
// Save network data in multiple formats
gitfetcher::run_network_fetch(
    "owner",
    "repo",
    Some(token),
    1, 1, Some(100),
    "json",             // Output format: "json", "csv", "edgelist", "gexf"
    "network_output"     // Output directory
).await?;
```

## Supported Export Formats

- **JSON**: Structured data with full metadata
- **CSV**: Tabular format for spreadsheet analysis
- **Edge List**: Simple source-target pairs for network analysis tools
- **GEXF**: (Future) Gephi-compatible format for advanced visualization

## Network Metrics Available

### Basic Metrics
- **Node Count**: Number of unique entities (users/repos)
- **Edge Count**: Number of relationships/connections
- **Density**: How connected the network is (0.0 = no connections, 1.0 = fully connected)
- **Average Degree**: Average number of connections per node
- **Maximum Degree**: Most connected node's connection count
- **Degree Distribution**: Histogram of connection counts
- **Connected Components**: Number of separate network clusters

### Advanced Analysis
- **Centrality Measures**: Identify influential nodes
- **Community Detection**: Find clusters of related users/projects
- **Path Analysis**: Calculate shortest paths and distances
- **Clustering Coefficient**: Measure local connectivity patterns

## Use Cases

### 1. Open Source Project Analysis
- Map contributor collaboration networks
- Identify key contributors and their influence spheres
- Track project evolution and community growth over time
- Find potential maintainers based on activity patterns

### 2. Social Network Analysis  
- Analyze developer relationships and influence networks
- Identify domain experts and thought leaders
- Study information flow and collaboration patterns
- Find bridge users who connect different communities

### 3. Software Ecosystem Analysis
- Map dependency relationships between projects
- Identify critical infrastructure and potential single points of failure
- Analyze technology adoption and spread patterns
- Study ecosystem evolution and trends

### 4. Community Health Monitoring
- Track community engagement and activity levels
- Identify emerging communities and potential contributors
- Monitor collaboration quality and patterns
- Detect community fragmentation or silo formation

## Integration with External Tools

### Python Ecosystem
Network data can be exported and analyzed with Python libraries:
```python
import networkx as nx
import pandas as pd

# Load exported network data
df = pd.read_csv('user_follow_relations.csv')
G = nx.from_pandas_edgelist(df, 'follower_login', 'followed_login')

# Calculate network metrics
centrality = nx.degree_centrality(G)
communities = nx.community.louvain_communities(G)
```

### Rustworkx Integration
The library is designed to integrate with rustworkx for advanced analysis:
```rust
use rustworkx as rx;

// Convert to rustworkx graph for advanced algorithms
let mut graph = rx::PyGraph::new();
// Add nodes and edges from network data
let path = rx::dijkstra_shortest_paths(&graph, source, target, None);
```

### Visualization Tools
Exported data works with popular network visualization tools:
- **Gephi**: For interactive network visualization and analysis
- **Cytoscape**: For complex network visualization and exploration
- **Graphviz**: For static network diagram generation
- **D3.js**: For web-based interactive visualizations

## Rate Limiting and Performance

The implementation includes several optimizations for handling large-scale network extraction:

- **Rate Limiting**: Built-in delays to respect GitHub API rate limits
- **Depth Control**: Configurable traversal depth to prevent exponential growth
- **Size Limits**: Optional maximum item counts to control data volume
- **Incremental Processing**: Network data is processed incrementally to manage memory usage

## Security and Privacy Considerations

- **Token Management**: GitHub tokens are handled securely and never logged
- **Data Scope**: Only publicly available GitHub data is accessed
- **Rate Limits**: API calls respect GitHub's rate limiting policies
- **Privacy**: No private repository data is accessed without proper permissions

## Future Enhancements

Planned improvements for future releases:

1. **Enhanced Dependency Analysis**: Full parsing of package files (package.json, Cargo.toml, etc.)
2. **Temporal Network Analysis**: Track network evolution over time
3. **Machine Learning Integration**: Community detection and anomaly detection
4. **Real-time Monitoring**: Continuous network data collection and analysis
5. **Advanced Visualization**: Built-in network visualization capabilities
6. **Cross-platform Analysis**: Multi-repository network correlation

## Examples

See `examples/network_example.py` for detailed usage examples and integration patterns.

## Contributing

This extension was designed to maintain compatibility with the existing GitFetcher architecture while adding powerful network analysis capabilities. When contributing:

1. Follow existing code patterns and conventions
2. Ensure proper error handling and rate limiting
3. Add comprehensive tests for new network analysis functions
4. Update documentation for new features
5. Consider performance implications for large network operations