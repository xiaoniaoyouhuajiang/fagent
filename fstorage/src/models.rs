use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EntityIdentifier {
    pub uri: String,
    pub entity_type: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReadinessReport {
    pub is_fresh: bool,
    pub freshness_gap_seconds: Option<i64>,
    pub coverage_metrics: serde_json::Value,
}

#[derive(Debug, Clone)]
pub enum SyncBudget {
    ByDuration(std::time::Duration),
    ByRequestCount(u32),
}

#[derive(Debug, Clone)]
pub struct SyncContext {
    pub triggering_query: Option<String>,
    pub target_entities: Vec<EntityIdentifier>,
}

// --- Metadata Catalog (SQLite) Models ---

#[derive(Debug)]
pub struct EntityReadiness {
    pub entity_uri: String,
    pub entity_type: String,
    pub last_synced_at: Option<i64>, // Unix timestamp
    pub ttl_seconds: Option<i64>,
    pub coverage_metrics: String, // JSON string
}

#[derive(Debug)]
pub struct ApiBudget {
    pub api_endpoint: String,
    pub requests_left: i64,
    pub reset_time: i64, // Unix timestamp
}

#[derive(Debug)]
pub struct TaskLog {
    pub task_id: i64,
    pub task_name: String,
    pub start_time: i64, // Unix timestamp
    pub end_time: Option<i64>,
    pub status: String,
    pub details: String, // JSON string
}
