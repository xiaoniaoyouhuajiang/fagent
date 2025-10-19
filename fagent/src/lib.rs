use std::{net::SocketAddr, path::PathBuf, sync::Arc};

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
    fetch::FetcherCapability,
    models::{EntityIdentifier, ReadinessReport, SyncBudget, SyncContext, TableSummary},
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

type ApiResult<T> = Result<T, ApiError>;

fn init_tracing() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();
}

const INDEX_HTML: &str = include_str!("../dashboard_ui/index.html");
const STYLES_CSS: &str = include_str!("../dashboard_ui/styles.css");
const APP_JS: &str = include_str!("../dashboard_ui/app.js");

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
        .route("/api/graph/visual", get(graph_visual))
        .route("/api/readiness", post(check_readiness))
        .route("/api/sync", post(trigger_sync))
        .with_state(state);

    let static_routes = Router::new()
        .route("/", get(serve_index))
        .route("/styles.css", get(serve_styles))
        .route("/app.js", get(serve_app_js))
        .fallback(get(serve_index));

    api.merge(static_routes)
}

async fn serve_index() -> Html<&'static str> {
    Html(INDEX_HTML)
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
