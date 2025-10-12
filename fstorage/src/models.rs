use serde::{Deserialize, Serialize};

use crate::fetch::ProbeReport;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EntityIdentifier {
    pub uri: String,
    pub entity_type: String,
    #[serde(default)]
    pub fetcher_name: Option<String>,
    #[serde(default)]
    pub params: Option<serde_json::Value>,
    #[serde(default)]
    pub anchor_key: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReadinessReport {
    pub is_fresh: bool,
    pub freshness_gap_seconds: Option<i64>,
    pub coverage_metrics: serde_json::Value,
    #[serde(default)]
    pub probe_report: Option<ProbeReport>,
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

#[derive(Debug, Clone)]
pub struct IngestionOffset {
    pub table_path: String,
    pub entity_type: String,
    pub category: crate::fetch::EntityCategory,
    pub primary_keys: Vec<String>,
    pub last_version: i64,
}

#[derive(Debug, Clone)]
pub struct SourceAnchor {
    pub entity_uri: String,
    pub fetcher: String,
    pub anchor_key: String,
    pub anchor_value: Option<String>,
    pub updated_at: i64,
}
