use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};

use anyhow::Context;
use axum::{
    body::Body,
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use clap::{Args, Parser, Subcommand};
use fstorage::sync::DataSynchronizer;
use fstorage::{
    config::StorageConfig,
    errors::StorageError,
    fetch::{EntityCategory, FetcherCapability},
    models::{
        EntityIdentifier, MultiEntitySearchHit, ReadinessReport, SyncBudget, SyncContext,
        TableSummary,
    },
    FStorage,
};
use helix_db::helix_engine::storage_core::graph_visualization::GraphVisualization;
use helix_db::helix_engine::types::GraphError;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

/// Runs the command line interface for the fagent dashboard.
pub async fn run_cli() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();
    match cli.command {
        Some(Command::Dashboard(args)) => run_dashboard(args).await?,
        None => {
            println!("No subcommand provided. Use --help to see available commands.");
        }
    }

    Ok(())
}

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Starts the fagent dashboard HTTP service
    Dashboard(DashboardArgs),
}

#[derive(Args)]
struct DashboardArgs {
    /// Base directory for fstorage lake/catalog/engine data
    #[arg(long, env = "FSTORAGE_BASE_PATH")]
    base_path: PathBuf,
    /// Socket address to bind the dashboard service
    #[arg(long, default_value = "127.0.0.1:3000")]
    bind: String,
    /// Optional GitHub token for GitFetcher
    #[arg(long, env = "GITHUB_TOKEN")]
    github_token: Option<String>,
    /// Disable registering GitFetcher
    #[arg(long, default_value_t = false)]
    disable_gitfetcher: bool,
}

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<FStorage>,
}

impl AppState {
    pub fn new(storage: Arc<FStorage>) -> Self {
        Self { storage }
    }
}

#[derive(Debug, thiserror::Error)]
enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Internal(String),
}

impl ApiError {
    fn from_storage(err: StorageError) -> Self {
        match err {
            StorageError::InvalidArg(msg) => ApiError::BadRequest(msg),
            StorageError::NotFound(msg) => ApiError::NotFound(msg),
            StorageError::Graph(graph_err) => match graph_err {
                GraphError::New(msg) => ApiError::NotFound(msg),
                GraphError::NodeNotFound
                | GraphError::EdgeNotFound
                | GraphError::LabelNotFound
                | GraphError::ShortestPathNotFound => ApiError::NotFound(graph_err.to_string()),
                GraphError::TraversalError(msg) => ApiError::BadRequest(msg),
                GraphError::ParamNotFound(param) => {
                    ApiError::BadRequest(format!("parameter {param} not found"))
                }
                other => ApiError::Internal(other.to_string()),
            },
            other => ApiError::Internal(other.to_string()),
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = Json(json!({ "error": self.to_string() }));
        (status, body).into_response()
    }
}

#[derive(Clone, Deserialize)]
struct TablesQuery {
    #[serde(default)]
    prefix: Option<String>,
}

#[derive(Clone, Deserialize)]
struct GraphVisualQuery {
    #[serde(default)]
    k: Option<usize>,
    #[serde(default)]
    node_prop: Option<String>,
}

#[derive(Clone, Deserialize)]
struct GraphOverviewQuery {
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Clone, Deserialize)]
struct GraphSearchQuery {
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    entity_type: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Clone, Deserialize)]
struct GraphSubgraphQuery {
    start_id: String,
    #[serde(default)]
    depth: Option<usize>,
    #[serde(default)]
    node_limit: Option<usize>,
    #[serde(default)]
    edge_limit: Option<usize>,
    #[serde(default)]
    edge_types: Option<String>,
}

#[derive(Clone, Deserialize)]
struct GraphNodeDetailQuery {
    id: String,
}

#[derive(Clone, Deserialize)]
struct HybridMultiQuery {
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    entity_types: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    alpha: Option<f32>,
}

#[derive(Deserialize)]
struct SyncRequest {
    fetcher: String,
    #[serde(default)]
    params: JsonValue,
    #[serde(default)]
    triggering_query: Option<String>,
    #[serde(default)]
    target_entities: Vec<EntityIdentifier>,
    #[serde(default)]
    budget: Option<SyncBudgetPayload>,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SyncBudgetPayload {
    DurationSecs { seconds: u64 },
    RequestCount { count: u32 },
}

impl From<SyncBudgetPayload> for SyncBudget {
    fn from(value: SyncBudgetPayload) -> Self {
        match value {
            SyncBudgetPayload::DurationSecs { seconds } => {
                SyncBudget::ByDuration(std::time::Duration::from_secs(seconds))
            }
            SyncBudgetPayload::RequestCount { count } => SyncBudget::ByRequestCount(count),
        }
    }
}

impl Default for SyncBudgetPayload {
    fn default() -> Self {
        SyncBudgetPayload::RequestCount { count: 100 }
    }
}

#[derive(Serialize)]
struct StatusResponse {
    db_stats: JsonValue,
    entity_count: usize,
    registered_fetchers: usize,
}

#[derive(Serialize)]
struct SyncResponse {
    message: String,
}

#[derive(Serialize)]
struct GraphNodeSummary {
    id: String,
    entity_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
}

#[derive(Serialize)]
struct GraphOverviewResponse {
    candidates: Vec<GraphNodeSummary>,
}

#[derive(Serialize)]
struct GraphSearchResponse {
    candidates: Vec<GraphNodeSummary>,
}

#[derive(Serialize, Clone)]
struct GraphNodeDto {
    id: String,
    entity_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    properties: JsonValue,
}

#[derive(Serialize)]
struct GraphEdgeDto {
    id: String,
    label: String,
    from: String,
    to: String,
    properties: JsonValue,
}

#[derive(Serialize)]
struct GraphSubgraphResponse {
    center: GraphNodeDto,
    nodes: Vec<GraphNodeDto>,
    edges: Vec<GraphEdgeDto>,
}

#[derive(Serialize)]
struct HybridMultiResponse {
    entity_types: Vec<String>,
    hits: Vec<MultiEntitySearchHit>,
}

type ApiResult<T> = Result<T, ApiError>;

fn init_tracing() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();
}

const INDEX_HTML: &str = include_str!("../dashboard_ui/index.html");
const GRAPH_HTML: &str = include_str!("../dashboard_ui/graph.html");
const STYLES_CSS: &str = include_str!("../dashboard_ui/styles.css");
const APP_JS: &str = include_str!("../dashboard_ui/app.js");
const GRAPH_JS: &str = include_str!("../dashboard_ui/graph.js");

#[derive(Serialize, Clone)]
struct GraphTypeColorStyle {
    background: &'static str,
    border: &'static str,
    highlight_background: &'static str,
    highlight_border: &'static str,
}

#[derive(Serialize, Clone)]
struct GraphTypeStyle {
    entity_type: &'static str,
    display_name: &'static str,
    font_color: &'static str,
    color: GraphTypeColorStyle,
    #[serde(skip_serializing_if = "Option::is_none")]
    aliases: Option<&'static [&'static str]>,
}

const GRAPH_TYPE_STYLES: &[GraphTypeStyle] = &[
    GraphTypeStyle {
        entity_type: "Project",
        display_name: "Project 项目",
        font_color: "#0f172a",
        color: GraphTypeColorStyle {
            background: "#0ea5e9",
            border: "#38bdf8",
            highlight_background: "#38bdf8",
            highlight_border: "#0ea5e9",
        },
        aliases: Some(&["PROJECT"]),
    },
    GraphTypeStyle {
        entity_type: "Version",
        display_name: "Version 版本",
        font_color: "#0f172a",
        color: GraphTypeColorStyle {
            background: "#22d3ee",
            border: "#67e8f9",
            highlight_background: "#67e8f9",
            highlight_border: "#22d3ee",
        },
        aliases: Some(&["VERSION"]),
    },
    GraphTypeStyle {
        entity_type: "Commit",
        display_name: "Commit 提交",
        font_color: "#0f172a",
        color: GraphTypeColorStyle {
            background: "#f97316",
            border: "#fb923c",
            highlight_background: "#fb923c",
            highlight_border: "#f97316",
        },
        aliases: Some(&["COMMIT"]),
    },
    GraphTypeStyle {
        entity_type: "File",
        display_name: "File 文件",
        font_color: "#0f172a",
        color: GraphTypeColorStyle {
            background: "#6366f1",
            border: "#818cf8",
            highlight_background: "#818cf8",
            highlight_border: "#6366f1",
        },
        aliases: Some(&["FILE"]),
    },
    GraphTypeStyle {
        entity_type: "Directory",
        display_name: "Directory 目录",
        font_color: "#0f172a",
        color: GraphTypeColorStyle {
            background: "#6b7280",
            border: "#9ca3af",
            highlight_background: "#9ca3af",
            highlight_border: "#6b7280",
        },
        aliases: Some(&["DIRECTORY"]),
    },
    GraphTypeStyle {
        entity_type: "Module",
        display_name: "Module 模块",
        font_color: "#0f172a",
        color: GraphTypeColorStyle {
            background: "#8b5cf6",
            border: "#a855f7",
            highlight_background: "#a855f7",
            highlight_border: "#8b5cf6",
        },
        aliases: Some(&["MODULE"]),
    },
    GraphTypeStyle {
        entity_type: "Class",
        display_name: "Class 类",
        font_color: "#0f172a",
        color: GraphTypeColorStyle {
            background: "#14b8a6",
            border: "#2dd4bf",
            highlight_background: "#2dd4bf",
            highlight_border: "#14b8a6",
        },
        aliases: Some(&["CLASS"]),
    },
    GraphTypeStyle {
        entity_type: "Struct",
        display_name: "Struct 结构体",
        font_color: "#e0f2f1",
        color: GraphTypeColorStyle {
            background: "#0f766e",
            border: "#14b8a6",
            highlight_background: "#14b8a6",
            highlight_border: "#0f766e",
        },
        aliases: Some(&["STRUCT"]),
    },
    GraphTypeStyle {
        entity_type: "Trait",
        display_name: "Trait 特征",
        font_color: "#0f172a",
        color: GraphTypeColorStyle {
            background: "#059669",
            border: "#34d399",
            highlight_background: "#34d399",
            highlight_border: "#059669",
        },
        aliases: Some(&["TRAIT"]),
    },
    GraphTypeStyle {
        entity_type: "Function",
        display_name: "Function 函数",
        font_color: "#0f172a",
        color: GraphTypeColorStyle {
            background: "#ef4444",
            border: "#f87171",
            highlight_background: "#f87171",
            highlight_border: "#ef4444",
        },
        aliases: Some(&["FUNCTION"]),
    },
    GraphTypeStyle {
        entity_type: "Method",
        display_name: "Method 方法",
        font_color: "#f8fafc",
        color: GraphTypeColorStyle {
            background: "#dc2626",
            border: "#f87171",
            highlight_background: "#f87171",
            highlight_border: "#dc2626",
        },
        aliases: Some(&["METHOD"]),
    },
    GraphTypeStyle {
        entity_type: "Interface",
        display_name: "Interface 接口",
        font_color: "#0f172a",
        color: GraphTypeColorStyle {
            background: "#2563eb",
            border: "#3b82f6",
            highlight_background: "#3b82f6",
            highlight_border: "#2563eb",
        },
        aliases: Some(&["INTERFACE"]),
    },
    GraphTypeStyle {
        entity_type: "Enum",
        display_name: "Enum 枚举",
        font_color: "#0f172a",
        color: GraphTypeColorStyle {
            background: "#fbbf24",
            border: "#fcd34d",
            highlight_background: "#fcd34d",
            highlight_border: "#fbbf24",
        },
        aliases: Some(&["ENUM"]),
    },
    GraphTypeStyle {
        entity_type: "Package",
        display_name: "Package 包",
        font_color: "#0f172a",
        color: GraphTypeColorStyle {
            background: "#9333ea",
            border: "#a855f7",
            highlight_background: "#a855f7",
            highlight_border: "#9333ea",
        },
        aliases: Some(&["PACKAGE"]),
    },
    GraphTypeStyle {
        entity_type: "Call",
        display_name: "Call 调用",
        font_color: "#0f172a",
        color: GraphTypeColorStyle {
            background: "#f59e0b",
            border: "#fbbf24",
            highlight_background: "#fbbf24",
            highlight_border: "#f59e0b",
        },
        aliases: Some(&["CALL"]),
    },
    GraphTypeStyle {
        entity_type: "ReadmeChunk",
        display_name: "README 片段",
        font_color: "#0f172a",
        color: GraphTypeColorStyle {
            background: "#facc15",
            border: "#fde047",
            highlight_background: "#fde047",
            highlight_border: "#facc15",
        },
        aliases: Some(&["README_CHUNK", "README"]),
    },
    GraphTypeStyle {
        entity_type: "CodeChunk",
        display_name: "Code 片段",
        font_color: "#0f172a",
        color: GraphTypeColorStyle {
            background: "#22c55e",
            border: "#4ade80",
            highlight_background: "#4ade80",
            highlight_border: "#22c55e",
        },
        aliases: Some(&["CODE_CHUNK"]),
    },
    GraphTypeStyle {
        entity_type: "Vector",
        display_name: "向量表示",
        font_color: "#3b0764",
        color: GraphTypeColorStyle {
            background: "#e879f9",
            border: "#f0abfc",
            highlight_background: "#f0abfc",
            highlight_border: "#e879f9",
        },
        aliases: Some(&["VECTOR", "EMBEDDING"]),
    },
    GraphTypeStyle {
        entity_type: "Default",
        display_name: "其他",
        font_color: "#e2e8f0",
        color: GraphTypeColorStyle {
            background: "#475569",
            border: "#94a3b8",
            highlight_background: "#94a3b8",
            highlight_border: "#475569",
        },
        aliases: None,
    },
];

async fn run_dashboard(args: DashboardArgs) -> anyhow::Result<()> {
    let addr: SocketAddr = args.bind.parse().context("failed to parse bind address")?;

    let config = StorageConfig::new(&args.base_path);
    let storage = Arc::new(FStorage::new(config).await?);

    if !args.disable_gitfetcher {
        match gitfetcher::GitFetcher::with_default_client(args.github_token.clone()) {
            Ok(fetcher) => {
                storage.register_fetcher(Arc::new(fetcher));
                info!("GitFetcher registered");
            }
            Err(err) => {
                error!("Failed to initialize GitFetcher: {}", err);
            }
        }
    }

    let state = AppState::new(storage);
    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("failed to bind dashboard listener")?;

    info!("Dashboard listening on {}", addr);
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("dashboard server error")?;

    Ok(())
}

/// Builds the HTTP router used by the dashboard service.
pub fn build_router(state: AppState) -> Router {
    let api = Router::new()
        .route("/api/fetchers", get(list_fetchers))
        .route("/api/status", get(get_status))
        .route("/api/tables", get(list_tables))
        .route("/api/graph/overview", get(graph_overview))
        .route("/api/graph/types", get(graph_types))
        .route("/api/graph/search", get(graph_search))
        .route("/api/graph/subgraph", get(graph_subgraph))
        .route("/api/graph/node", get(graph_node_detail))
        .route("/api/graph/visual", get(graph_visual))
        .route("/api/search/hybrid/types", get(hybrid_entity_types))
        .route("/api/search/hybrid_all", get(hybrid_multi_search))
        .route("/api/readiness", post(check_readiness))
        .route("/api/sync", post(trigger_sync))
        .with_state(state);

    let static_routes = Router::new()
        .route("/", get(serve_index))
        .route("/graph.html", get(serve_graph))
        .route("/styles.css", get(serve_styles))
        .route("/app.js", get(serve_app_js))
        .route("/graph.js", get(serve_graph_js))
        .fallback(get(serve_index));

    api.merge(static_routes)
}

async fn serve_index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn serve_graph() -> Html<&'static str> {
    Html(GRAPH_HTML)
}

async fn serve_styles() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/css; charset=utf-8")
        .body(Body::from(STYLES_CSS))
        .unwrap()
}

async fn serve_app_js() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/javascript; charset=utf-8")
        .body(Body::from(APP_JS))
        .unwrap()
}

async fn serve_graph_js() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/javascript; charset=utf-8")
        .body(Body::from(GRAPH_JS))
        .unwrap()
}

async fn list_fetchers(State(state): State<AppState>) -> ApiResult<Json<Vec<FetcherCapability>>> {
    let capabilities = state.storage.list_fetchers_capability();
    Ok(Json(capabilities))
}

async fn get_status(State(state): State<AppState>) -> ApiResult<Json<StatusResponse>> {
    let txn = state
        .storage
        .engine
        .storage
        .graph_env
        .read_txn()
        .map_err(|err| ApiError::Internal(err.to_string()))?;

    let stats_str = state
        .storage
        .engine
        .storage
        .get_db_stats_json(&txn)
        .map_err(|err| ApiError::from_storage(StorageError::Graph(err)))?;
    let stats: JsonValue =
        serde_json::from_str(&stats_str).map_err(|err| ApiError::Internal(err.to_string()))?;

    let entities = state
        .storage
        .list_known_entities()
        .map_err(ApiError::from_storage)?;

    let response = StatusResponse {
        db_stats: stats,
        entity_count: entities.len(),
        registered_fetchers: state.storage.list_fetchers_capability().len(),
    };

    Ok(Json(response))
}

async fn list_tables(
    State(state): State<AppState>,
    Query(query): Query<TablesQuery>,
) -> ApiResult<Json<Vec<TableSummary>>> {
    let prefix = query.prefix.unwrap_or_else(|| "".to_string());
    let tables = state
        .storage
        .list_tables(&prefix)
        .await
        .map_err(ApiError::from_storage)?;
    Ok(Json(tables))
}

async fn graph_visual(
    State(state): State<AppState>,
    Query(query): Query<GraphVisualQuery>,
) -> ApiResult<Json<JsonValue>> {
    let txn = state
        .storage
        .engine
        .storage
        .graph_env
        .read_txn()
        .map_err(|err| ApiError::Internal(err.to_string()))?;
    let raw = state
        .storage
        .engine
        .storage
        .nodes_edges_to_json(&txn, query.k, query.node_prop.clone())
        .map_err(|err| ApiError::from_storage(StorageError::Graph(err)))?;
    let payload: JsonValue =
        serde_json::from_str(&raw).map_err(|err| ApiError::Internal(err.to_string()))?;
    Ok(Json(payload))
}

async fn graph_types() -> ApiResult<Json<Vec<GraphTypeStyle>>> {
    let styles: Vec<GraphTypeStyle> = GRAPH_TYPE_STYLES.iter().cloned().collect();
    Ok(Json(styles))
}

async fn graph_overview(
    State(state): State<AppState>,
    Query(query): Query<GraphOverviewQuery>,
) -> ApiResult<Json<GraphOverviewResponse>> {
    let limit = query.limit.unwrap_or(30).clamp(1, 300);
    let candidates = collect_overview_candidates(&state, limit).await?;
    Ok(Json(GraphOverviewResponse { candidates }))
}

async fn collect_overview_candidates(
    state: &AppState,
    limit: usize,
) -> ApiResult<Vec<GraphNodeSummary>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let snapshot = {
        let txn = state
            .storage
            .engine
            .storage
            .graph_env
            .read_txn()
            .map_err(|err| ApiError::Internal(err.to_string()))?;
        state
            .storage
            .engine
            .storage
            .nodes_edges_to_json(&txn, Some(limit), None)
            .map_err(|err| ApiError::from_storage(StorageError::Graph(err)))?
    };

    let parsed: JsonValue =
        serde_json::from_str(&snapshot).map_err(|err| ApiError::Internal(err.to_string()))?;
    let mut candidates = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    if let Some(nodes_array) = parsed.get("nodes").and_then(|value| value.as_array()) {
        for node_value in nodes_array {
            if candidates.len() >= limit {
                break;
            }

            let node_id = node_value
                .get("id")
                .and_then(|value| value.as_str())
                .map(|value| value.to_string());
            let Some(node_id) = node_id else {
                continue;
            };

            if !seen.insert(node_id.clone()) {
                continue;
            }

            let fetched = state
                .storage
                .lake
                .get_node_by_id(&node_id, None)
                .await
                .map_err(ApiError::from_storage)?;

            let Some(node_map) = fetched else {
                continue;
            };

            if let Some(summary) = map_node_summary(node_map) {
                candidates.push(summary);
            }
        }
    }

    Ok(candidates)
}

async fn graph_search(
    State(state): State<AppState>,
    Query(query): Query<GraphSearchQuery>,
) -> ApiResult<Json<GraphSearchResponse>> {
    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let term = query.q.unwrap_or_default();
    let term = term.trim();
    let entity_type = query.entity_type.as_deref();

    let candidates = if term.is_empty() && entity_type.is_none() {
        collect_overview_candidates(&state, limit).await?
    } else {
        search_candidates(&state, term, entity_type, limit).await?
    };

    Ok(Json(GraphSearchResponse { candidates }))
}

async fn search_candidates(
    state: &AppState,
    term: &str,
    entity_type: Option<&str>,
    limit: usize,
) -> ApiResult<Vec<GraphNodeSummary>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let mut entity_types = Vec::new();
    if let Some(explicit) = entity_type {
        entity_types.push(explicit.to_string());
    } else {
        let offsets = state
            .storage
            .catalog
            .list_ingestion_offsets()
            .map_err(ApiError::from_storage)?;
        for offset in offsets {
            if offset.category == EntityCategory::Node {
                entity_types.push(offset.entity_type);
            }
        }
    }

    if entity_types.is_empty() {
        return Ok(Vec::new());
    }

    entity_types.sort();
    entity_types.dedup();

    let mut seen: HashSet<String> = HashSet::new();
    let mut results = Vec::new();

    for entity in entity_types {
        if results.len() >= limit {
            break;
        }
        let remaining = limit - results.len();
        let rows = state
            .storage
            .lake
            .search_index_nodes(&entity, term, remaining)
            .await
            .map_err(ApiError::from_storage)?;

        for row in rows {
            if results.len() >= limit {
                break;
            }
            let Some(id) = row.get("id").and_then(|value| value.as_str()) else {
                continue;
            };
            if !seen.insert(id.to_string()) {
                continue;
            }

            let node_map = state
                .storage
                .lake
                .get_node_by_id(id, Some(&entity))
                .await
                .map_err(ApiError::from_storage)?;
            let Some(node_map) = node_map else {
                continue;
            };

            if let Some(summary) = map_node_summary(node_map) {
                results.push(summary);
            }
        }
    }

    Ok(results)
}

fn gather_hybrid_entity_types(state: &AppState) -> ApiResult<Vec<String>> {
    let offsets = state
        .storage
        .catalog
        .list_ingestion_offsets()
        .map_err(ApiError::from_storage)?;
    let mut types = Vec::new();
    for offset in offsets {
        if matches!(
            offset.category,
            EntityCategory::Node | EntityCategory::Vector
        ) {
            types.push(offset.entity_type);
        }
    }
    types.sort();
    types.dedup();
    Ok(types)
}

async fn hybrid_entity_types(State(state): State<AppState>) -> ApiResult<Json<Vec<String>>> {
    let types = gather_hybrid_entity_types(&state)?;
    Ok(Json(types))
}

async fn hybrid_multi_search(
    State(state): State<AppState>,
    Query(query): Query<HybridMultiQuery>,
) -> ApiResult<Json<HybridMultiResponse>> {
    let mut entity_types: Vec<String> = query
        .entity_types
        .as_deref()
        .map(|raw| {
            raw.split(',')
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(|value| value.to_string())
                .collect()
        })
        .unwrap_or_default();

    if entity_types.is_empty() {
        entity_types = gather_hybrid_entity_types(&state)?;
    }

    if entity_types.is_empty() {
        return Ok(Json(HybridMultiResponse {
            entity_types,
            hits: Vec::new(),
        }));
    }

    let query_text = query.q.unwrap_or_default();
    let trimmed = query_text.trim();
    if trimmed.is_empty() {
        return Ok(Json(HybridMultiResponse {
            entity_types,
            hits: Vec::new(),
        }));
    }

    let alpha = query.alpha.unwrap_or(0.5).clamp(0.0, 1.0);
    let limit = query.limit.unwrap_or(20).clamp(1, 200);

    let hits = state
        .storage
        .search_hybrid_multi(&entity_types, trimmed, alpha, limit)
        .await
        .map_err(ApiError::from_storage)?;

    Ok(Json(HybridMultiResponse { entity_types, hits }))
}

async fn graph_subgraph(
    State(state): State<AppState>,
    Query(query): Query<GraphSubgraphQuery>,
) -> ApiResult<Json<GraphSubgraphResponse>> {
    let depth = query.depth.unwrap_or(1);
    let node_limit = query.node_limit.unwrap_or(150);
    let edge_limit = query.edge_limit.unwrap_or(200);
    let edge_filters = parse_edge_types(query.edge_types.as_deref());
    let edge_refs = edge_filters
        .as_ref()
        .map(|values| values.iter().map(String::as_str).collect::<Vec<&str>>());

    let subgraph = state
        .storage
        .lake
        .subgraph_bfs(
            &query.start_id,
            edge_refs.as_deref(),
            depth,
            node_limit,
            edge_limit,
        )
        .await
        .map_err(ApiError::from_storage)?;

    let center_map = state
        .storage
        .lake
        .get_node_by_id(&query.start_id, None)
        .await
        .map_err(ApiError::from_storage)?;
    let center_map = center_map
        .ok_or_else(|| ApiError::NotFound(format!("未找到起始节点 '{}'", query.start_id)))?;
    let center_node = map_node_record(center_map)
        .ok_or_else(|| ApiError::Internal("无法解析起始节点".to_string()))?;

    let mut nodes: HashMap<String, GraphNodeDto> = HashMap::new();
    nodes.insert(center_node.id.clone(), center_node.clone());
    for node_map in subgraph.nodes {
        if let Some(node) = map_node_record(node_map) {
            nodes.entry(node.id.clone()).or_insert(node);
        }
    }

    let mut edges = Vec::new();
    for edge_map in subgraph.edges {
        if let Some(edge) = map_edge_record(edge_map) {
            edges.push(edge);
        }
    }

    Ok(Json(GraphSubgraphResponse {
        center: center_node,
        nodes: nodes.into_values().collect(),
        edges,
    }))
}

async fn graph_node_detail(
    State(state): State<AppState>,
    Query(query): Query<GraphNodeDetailQuery>,
) -> ApiResult<Json<GraphNodeDto>> {
    let fetched = state
        .storage
        .lake
        .get_node_by_id(&query.id, None)
        .await
        .map_err(ApiError::from_storage)?;
    let node_map =
        fetched.ok_or_else(|| ApiError::NotFound(format!("节点 '{}' 不存在", query.id)))?;
    let node = map_node_record(node_map)
        .ok_or_else(|| ApiError::Internal("无法解析节点数据".to_string()))?;
    Ok(Json(node))
}

fn parse_edge_types(raw: Option<&str>) -> Option<Vec<String>> {
    let values: Vec<String> = raw
        .unwrap_or_default()
        .split(|c: char| c == ',' || c == ';' || c.is_whitespace())
        .filter_map(|token| {
            let trimmed = token.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .collect();
    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

fn map_node_record(map: HashMap<String, JsonValue>) -> Option<GraphNodeDto> {
    let id = map.get("id")?.as_str()?.to_string();
    let entity_type = map
        .get("label")
        .and_then(|value| value.as_str())
        .unwrap_or("Unknown")
        .to_string();
    let properties = map.get("properties").cloned().unwrap_or(JsonValue::Null);
    let title = map.get("title").and_then(|value| value.as_str());
    let display_name = infer_display_name(&properties, title, &id);

    Some(GraphNodeDto {
        id,
        entity_type,
        display_name,
        properties,
    })
}

fn map_node_summary(map: HashMap<String, JsonValue>) -> Option<GraphNodeSummary> {
    let node = map_node_record(map)?;
    Some(GraphNodeSummary {
        id: node.id.clone(),
        entity_type: node.entity_type.clone(),
        display_name: node.display_name.clone(),
    })
}

fn map_edge_record(map: HashMap<String, JsonValue>) -> Option<GraphEdgeDto> {
    let id = map
        .get("id")
        .or_else(|| map.get("title"))
        .and_then(|value| value.as_str())?
        .to_string();
    let from = map
        .get("from_node_id")
        .or_else(|| map.get("from"))
        .and_then(|value| value.as_str())?
        .to_string();
    let to = map
        .get("to_node_id")
        .or_else(|| map.get("to"))
        .and_then(|value| value.as_str())?
        .to_string();
    let label = map
        .get("label")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("EDGE")
        .to_string();
    let properties = map.get("properties").cloned().unwrap_or(JsonValue::Null);

    Some(GraphEdgeDto {
        id,
        label,
        from,
        to,
        properties,
    })
}

fn infer_display_name(
    properties: &JsonValue,
    fallback_title: Option<&str>,
    fallback_id: &str,
) -> Option<String> {
    if let Some(object) = properties.as_object() {
        for key in [
            "display_name",
            "name",
            "title",
            "slug",
            "identifier",
            "path",
            "file_path",
            "repo",
            "repository",
            "value",
        ] {
            if let Some(value) = object.get(key).and_then(|v| v.as_str()) {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
    }

    if let Some(title) = fallback_title {
        let trimmed = title.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    let trimmed = fallback_id.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

async fn check_readiness(
    State(state): State<AppState>,
    Json(body): Json<Vec<EntityIdentifier>>,
) -> ApiResult<Json<std::collections::HashMap<String, ReadinessReport>>> {
    let readiness = state
        .storage
        .get_readiness(&body)
        .await
        .map_err(ApiError::from_storage)?;
    Ok(Json(readiness))
}

async fn trigger_sync(
    State(state): State<AppState>,
    Json(body): Json<SyncRequest>,
) -> ApiResult<(StatusCode, Json<SyncResponse>)> {
    let context = SyncContext {
        triggering_query: body.triggering_query.clone(),
        target_entities: body.target_entities.clone(),
    };
    let budget = body
        .budget
        .map(SyncBudget::from)
        .unwrap_or_else(|| SyncBudget::ByRequestCount(100));

    state
        .storage
        .synchronizer
        .sync(&body.fetcher, body.params.clone(), context, budget)
        .await
        .map_err(ApiError::from_storage)?;

    Ok((
        StatusCode::OK,
        Json(SyncResponse {
            message: "sync completed".to_string(),
        }),
    ))
}

async fn shutdown_signal() {
    let _ = signal::ctrl_c().await;
    info!("Shutdown signal received");
}

impl From<StorageError> for ApiError {
    fn from(value: StorageError) -> Self {
        ApiError::from_storage(value)
    }
}
