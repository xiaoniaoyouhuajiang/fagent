use serde::Serialize;

// --- 配置枚举 ---

/// 定义数据输出格式
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Json,
    Csv,
}

/// 定义获取数据的详细程度
#[derive(Debug, Clone, Copy)]
pub enum DetailLevel {
    Core, // 仅包含核心字段
    Full, // 包含所有可用字段
}

// --- 数据模型 ---

#[derive(Serialize, Debug, Clone)]
pub struct Issue {
    pub id: u64,
    pub number: u64,
    pub title: String,
    pub user: String, // 简化为用户名
    pub state: String,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>, // 仅在 Full 模式下填充
}

#[derive(Serialize, Debug, Clone)]
pub struct PullRequest {
    pub id: u64,
    pub number: u64,
    pub title: String,
    pub user: String,
    pub state: String,
    pub created_at: String,
    pub updated_at: String,
    pub merged_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
pub struct Commit {
    pub sha: String,
    pub author: String, // 简化为用户名
    pub message: String,
    pub date: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct User {
    pub id: u64,
    pub login: String,
    pub name: Option<String>,
    pub company: Option<String>,
    pub location: Option<String>,
}

/// 用于存储从 API 获取的数据的内存缓存
#[derive(Default, Debug)]
pub struct RepoData {
    pub issues: Vec<Issue>,
    pub pull_requests: Vec<PullRequest>,
    pub commits: Vec<Commit>,
}

// --- 网络数据模型 ---

#[derive(Serialize, Debug, Clone)]
pub struct UserFollowRelation {
    pub follower_id: u64,
    pub follower_login: String,
    pub followed_id: u64,
    pub followed_login: String,
    pub created_at: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct RepoDependency {
    pub source_repo_id: u64,
    pub source_repo_name: String,
    pub target_repo_id: u64,
    pub target_repo_name: String,
    pub dependency_type: String, // "depends_on", "used_by", "forks"
    pub created_at: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct RepoFork {
    pub source_repo_id: u64,
    pub source_repo_name: String,
    pub fork_repo_id: u64,
    pub fork_repo_name: String,
    pub fork_owner_login: String,
    pub created_at: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct UserCollaboration {
    pub user_id: u64,
    pub user_login: String,
    pub repo_id: u64,
    pub repo_name: String,
    pub collaboration_type: String, // "issue", "pull_request", "commit", "comment"
    pub created_at: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct NetworkMetrics {
    pub node_count: usize,
    pub edge_count: usize,
    pub density: f64,
    pub clustering_coefficient: f64,
    pub avg_degree: f64,
    pub max_degree: usize,
    pub connected_components: usize,
}

#[derive(Default, Debug, Serialize)]
pub struct NetworkData {
    pub user_follow_relations: Vec<UserFollowRelation>,
    pub repo_dependencies: Vec<RepoDependency>,
    pub repo_forks: Vec<RepoFork>,
    pub user_collaborations: Vec<UserCollaboration>,
}
