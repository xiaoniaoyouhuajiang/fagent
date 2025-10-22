use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use clap::{Parser, Subcommand, ValueEnum, builder::ArgAction};
use deltalake::arrow::array::ArrayRef;
use deltalake::arrow::util::pretty::pretty_format_batches;
use deltalake::datafusion::datasource::TableProvider;
use deltalake::datafusion::execution::context::{SessionConfig, SessionContext};
use deltalake::open_table;
use fstorage::FStorage;
use fstorage::config::StorageConfig;
use fstorage::lake::{NeighborDirection, NeighborRecord, Subgraph};
use fstorage::models::{IngestionOffset, TableSummary};
use helix_db::helix_engine::storage_core::graph_visualization::GraphVisualization;
use helix_db::helix_engine::traversal_core::HelixGraphEngine;
use helix_db::protocol::value::Value as HelixValue;
use helix_db::utils::id::ID;
use helix_db::utils::items::Node;
use log::LevelFilter;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use url::Url;

#[derive(Parser, Debug)]
#[command(
    name = "fstorage-cli",
    about = "Inspect cold (Delta Lake) and hot (Helix) layers for an fstorage deployment."
)]
struct Cli {
    /// Base path of the fstorage instance (directory containing lake/catalog/engine).
    #[arg(short, long, default_value = ".", value_hint = clap::ValueHint::DirPath)]
    base_path: PathBuf,

    /// Minimum log level to display.
    #[arg(long, default_value_t = LogLevelArg::Info, value_enum)]
    log_level: LogLevelArg,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Inspect Delta Lake (cold layer) tables.
    Cold {
        #[command(subcommand)]
        command: ColdCommand,
    },
    /// Inspect Helix (hot layer) data stored in the graph engine.
    Hot {
        #[command(subcommand)]
        command: HotCommand,
    },
    /// Print catalog ingestion offsets.
    Catalog {
        /// Emit JSON instead of a textual table.
        #[arg(long)]
        json: bool,
    },
    /// Print a quick summary of both cold and hot layers.
    Summary {
        /// Emit JSON instead of text.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
enum ColdCommand {
    /// List discovered Delta tables under an optional prefix.
    List {
        /// Only inspect tables beneath this subdirectory (relative to the lake root).
        #[arg(long, default_value = "")]
        prefix: String,

        /// Additionally compute row counts for each table.
        #[arg(long)]
        counts: bool,
    },
    /// Display the first N records of a Delta table.
    Show {
        /// Table path relative to the lake root (e.g. silver/entities/project).
        table: String,

        /// Maximum number of rows to display (0 = no limit).
        #[arg(long, default_value_t = 20)]
        limit: usize,

        /// Emit JSON array instead of a pretty-printed table.
        #[arg(long)]
        json: bool,
    },
    /// Print the schema for a Delta table.
    Schema {
        /// Table path relative to the lake root (e.g. silver/entities/project).
        table: String,
    },
}

#[derive(Subcommand, Debug)]
enum HotCommand {
    /// Display aggregate counts for nodes, edges, and vectors.
    Stats,
    /// List nodes with the provided label.
    Nodes {
        /// Logical label (entity type), e.g. project/version/commit.
        label: String,

        /// Maximum number of nodes to display (0 = no limit).
        #[arg(long, default_value_t = 10)]
        limit: usize,

        /// Emit JSON instead of a text table.
        #[arg(long)]
        json: bool,
    },
    /// Fetch a node by its UUID.
    Node {
        /// Node UUID (stored in Helix) – accepts both hyphenated and bare formats.
        id: String,

        /// Emit JSON instead of a debug representation.
        #[arg(long)]
        json: bool,
    },
    /// Resolve a node by primary-key values via the lookup index.
    ByKeys {
        /// Entity label (same as the Delta entity type).
        entity_type: String,

        /// Primary key pairs, e.g. --key url=https://github.com/foo/bar
        #[arg(
            long = "key",
            value_parser = parse_key_value,
            action = ArgAction::Append
        )]
        keys: Vec<(String, String)>,

        /// Emit JSON instead of text.
        #[arg(long)]
        json: bool,
    },
    /// Explore neighbours for a node.
    Neighbors {
        /// Starting node UUID.
        id: String,

        /// Orientation of traversed edges.
        #[arg(long, value_enum, default_value_t = DirectionArg::Both)]
        direction: DirectionArg,

        /// Optional comma-separated list of edge labels to include.
        #[arg(long, value_delimiter = ',')]
        edge_types: Vec<String>,

        /// Maximum number of neighbour edges to materialise (0 = no cap).
        #[arg(long, default_value_t = 20)]
        limit: usize,

        /// Emit JSON instead of text.
        #[arg(long)]
        json: bool,
    },
    /// Build a BFS subgraph from a starting node.
    Subgraph {
        /// Starting node UUID.
        id: String,

        /// Optional comma-separated list of edge labels to include.
        #[arg(long, value_delimiter = ',')]
        edge_types: Vec<String>,

        /// BFS depth limit (0 = unlimited).
        #[arg(long, default_value_t = 2)]
        depth: usize,

        /// Maximum number of nodes to materialise (0 = unlimited).
        #[arg(long, default_value_t = 100)]
        node_limit: usize,

        /// Maximum number of edges to materialise (0 = unlimited).
        #[arg(long, default_value_t = 200)]
        edge_limit: usize,

        /// Emit JSON instead of text.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum LogLevelArg {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl From<LogLevelArg> for LevelFilter {
    fn from(value: LogLevelArg) -> Self {
        match value {
            LogLevelArg::Error => LevelFilter::Error,
            LogLevelArg::Warn => LevelFilter::Warn,
            LogLevelArg::Info => LevelFilter::Info,
            LogLevelArg::Debug => LevelFilter::Debug,
            LogLevelArg::Trace => LevelFilter::Trace,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum DirectionArg {
    Outgoing,
    Incoming,
    Both,
}

impl From<DirectionArg> for NeighborDirection {
    fn from(value: DirectionArg) -> Self {
        match value {
            DirectionArg::Outgoing => NeighborDirection::Outgoing,
            DirectionArg::Incoming => NeighborDirection::Incoming,
            DirectionArg::Both => NeighborDirection::Both,
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("Error: {err:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    let mut logger = env_logger::Builder::from_env(env_logger::Env::default());
    logger.filter_level(LevelFilter::from(cli.log_level));
    let _ = logger.try_init();

    let base_hint = if cli.base_path == PathBuf::from(".") {
        std::env::var("FSTORAGE_BASE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| cli.base_path.clone())
    } else {
        cli.base_path.clone()
    };

    let base_path = if base_hint.is_absolute() {
        base_hint
    } else {
        std::env::current_dir()
            .context("failed to resolve current directory")?
            .join(&base_hint)
    };

    if !base_path.exists() {
        bail!("Base path '{}' does not exist", base_path.display());
    }

    let config = StorageConfig::new(base_path);
    let storage = FStorage::new(config)
        .await
        .context("failed to open fstorage instance")?;

    match cli.command {
        Commands::Cold { command } => handle_cold(&storage, command).await,
        Commands::Hot { command } => handle_hot(&storage, command).await,
        Commands::Catalog { json } => handle_catalog(&storage, json),
        Commands::Summary { json } => handle_summary(&storage, json).await,
    }
}

async fn handle_cold(storage: &FStorage, command: ColdCommand) -> Result<()> {
    let lake_root = storage.config.lake_path.clone();
    match command {
        ColdCommand::List { prefix, counts } => {
            let tables =
                storage.lake.list_tables(&prefix).await.with_context(|| {
                    format!("failed to enumerate tables under prefix '{prefix}'")
                })?;

            if tables.is_empty() {
                println!("No Delta tables found under prefix '{}'.", prefix);
                return Ok(());
            }

            println!(
                "{:<48} {:<8} {}",
                "TABLE PATH",
                if counts { "ROWS" } else { "" },
                "COLUMNS"
            );
            println!("{}", "-".repeat(80));

            for table in tables {
                let row_count = if counts {
                    Some(
                        delta_row_count(&lake_root, &table.table_path)
                            .await
                            .with_context(|| {
                                format!("failed to count rows for '{}'", table.table_path)
                            })?,
                    )
                } else {
                    None
                };
                let columns = table
                    .columns
                    .into_iter()
                    .map(|col| format!("{}:{}", col.name, col.data_type))
                    .collect::<Vec<_>>()
                    .join(", ");
                if let Some(rows) = row_count {
                    println!("{:<48} {:<8} {}", table.table_path, rows, columns);
                } else {
                    println!("{:<48} {}", table.table_path, columns);
                }
            }
            Ok(())
        }
        ColdCommand::Show { table, limit, json } => {
            let batches = read_table_batches(&lake_root, &table, limit)
                .await
                .with_context(|| format!("failed to read table '{}'", table))?;

            if batches.is_empty() {
                println!("Table '{}' is empty.", table);
                return Ok(());
            }

            if json {
                let rows = record_batches_to_json(&batches)?;
                println!("{}", serde_json::to_string_pretty(&rows)?);
            } else {
                let display = pretty_format_batches(&batches)
                    .map_err(|err| anyhow!("failed to format batches: {err}"))?;
                println!("{display}");
            }
            Ok(())
        }
        ColdCommand::Schema { table } => {
            let summary = open_table_summary(&lake_root, &table)
                .await
                .with_context(|| format!("failed to load schema for '{}'", table))?;
            println!("Table: {}", summary.table_path);
            for column in summary.columns {
                println!(
                    "  - {} ({}){}",
                    column.name,
                    column.data_type,
                    if column.nullable { "" } else { " [NOT NULL]" }
                );
            }
            Ok(())
        }
    }
}

async fn handle_hot(storage: &FStorage, command: HotCommand) -> Result<()> {
    match command {
        HotCommand::Stats => {
            let txn = storage.engine.storage.graph_env.read_txn()?;
            let stats = storage
                .engine
                .storage
                .get_db_stats_json(&txn)
                .map_err(|err| anyhow!(err.to_string()))?;
            println!("{stats}");
            Ok(())
        }
        HotCommand::Nodes { label, limit, json } => {
            let nodes = list_nodes_by_label(storage.engine.as_ref(), &label, limit)?;
            if nodes.is_empty() {
                println!("No nodes with label '{label}' were found.");
                return Ok(());
            }
            if json {
                println!("{}", serde_json::to_string_pretty(&nodes)?);
            } else {
                print_node_rows(&nodes);
            }
            Ok(())
        }
        HotCommand::Node { id, json } => {
            let node = storage
                .lake
                .get_node_by_id(&id, None)
                .await
                .with_context(|| format!("failed to load node '{id}'"))?;
            match node {
                Some(map) => {
                    if json {
                        println!("{}", serde_json::to_string_pretty(&map)?);
                    } else {
                        println!("{}", serde_json::to_string_pretty(&map)?);
                    }
                }
                None => {
                    println!("Node '{id}' was not found.");
                }
            }
            Ok(())
        }
        HotCommand::ByKeys {
            entity_type,
            keys,
            json,
        } => {
            if keys.is_empty() {
                bail!("At least one --key key=value pair is required.");
            }
            let owned_pairs = keys;
            let borrowed: Vec<(&str, &str)> = owned_pairs
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            let node = storage
                .lake
                .get_node_by_keys(&entity_type, &borrowed)
                .await
                .with_context(|| format!("failed to retrieve node '{entity_type}' by keys"))?;
            match node {
                Some(map) => {
                    if json {
                        println!("{}", serde_json::to_string_pretty(&map)?);
                    } else {
                        println!("{}", serde_json::to_string_pretty(&map)?);
                    }
                }
                None => println!("No node of type '{entity_type}' matched the supplied keys."),
            }
            Ok(())
        }
        HotCommand::Neighbors {
            id,
            direction,
            edge_types,
            limit,
            json,
        } => {
            let edge_filters = if edge_types.is_empty() {
                None
            } else {
                Some(edge_types.iter().map(|s| s.as_str()).collect::<Vec<&str>>())
            };
            let neighbors = storage
                .lake
                .neighbors(
                    &id,
                    edge_filters.as_deref(),
                    NeighborDirection::from(direction),
                    limit,
                )
                .await
                .with_context(|| format!("failed to load neighbours for '{id}'"))?;

            if neighbors.is_empty() {
                println!("No matching neighbours were found for node '{id}'.");
                return Ok(());
            }

            if json {
                let rows: Vec<JsonValue> = neighbors
                    .iter()
                    .map(|record| neighbor_to_json(record))
                    .collect();
                println!("{}", serde_json::to_string_pretty(&rows)?);
            } else {
                print_neighbor_rows(&neighbors);
            }
            Ok(())
        }
        HotCommand::Subgraph {
            id,
            edge_types,
            depth,
            node_limit,
            edge_limit,
            json,
        } => {
            let edge_filters = if edge_types.is_empty() {
                None
            } else {
                Some(edge_types.iter().map(|s| s.as_str()).collect::<Vec<&str>>())
            };
            let subgraph = storage
                .lake
                .subgraph_bfs(&id, edge_filters.as_deref(), depth, node_limit, edge_limit)
                .await
                .with_context(|| format!("failed to materialise subgraph from '{id}'"))?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&subgraph_to_value(&subgraph))?
                );
            } else {
                println!(
                    "Subgraph: {} nodes / {} edges",
                    subgraph.nodes.len(),
                    subgraph.edges.len()
                );
                println!(
                    "{}",
                    serde_json::to_string_pretty(&subgraph_to_value(&subgraph))?
                );
            }
            Ok(())
        }
    }
}

fn handle_catalog(storage: &FStorage, json: bool) -> Result<()> {
    let mut entries = storage.catalog.list_ingestion_offsets()?;
    entries.sort_by(|a, b| a.table_path.cmp(&b.table_path));

    if json {
        let payload: Vec<JsonValue> = entries.iter().map(offset_to_json).collect();
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    println!(
        "{:<48} {:<16} {:<8} {:<10} {}",
        "TABLE PATH", "ENTITY", "CATEGORY", "VERSION", "PRIMARY KEYS"
    );
    println!("{}", "-".repeat(100));
    for entry in entries {
        let keys = if entry.primary_keys.is_empty() {
            "-".to_string()
        } else {
            entry.primary_keys.join(",")
        };
        println!(
            "{:<48} {:<16} {:<8} {:<10} {}",
            entry.table_path,
            entry.entity_type,
            entry.category.as_str(),
            entry.last_version,
            keys
        );
    }
    Ok(())
}

async fn handle_summary(storage: &FStorage, json: bool) -> Result<()> {
    let cold_tables = storage.lake.list_tables("").await?;
    let txn = storage.engine.storage.graph_env.read_txn()?;
    let stats = storage
        .engine
        .storage
        .get_db_stats_json(&txn)
        .map_err(|err| anyhow!(err.to_string()))?;
    let stats_value: JsonValue = serde_json::from_str(&stats)?;

    if json {
        let catalog_entries = storage.catalog.list_ingestion_offsets()?;
        let catalog_json: Vec<JsonValue> = catalog_entries.iter().map(offset_to_json).collect();
        let summary = serde_json::json!({
            "catalog_entries": catalog_json,
            "tables": &cold_tables,
            "hot_stats": stats_value,
        });
        println!("{}", serde_json::to_string_pretty(&summary)?);
    } else {
        println!("Hot layer stats: {stats}");
        println!("Known Delta tables:");
        for table in cold_tables {
            println!("  - {}", table.table_path);
        }
    }
    Ok(())
}

async fn delta_row_count(lake_root: &Path, table_path: &str) -> Result<usize> {
    let batches = read_table_batches(lake_root, table_path, 0).await?;
    Ok(batches.iter().map(|batch| batch.num_rows()).sum())
}

async fn read_table_batches(
    lake_root: &Path,
    table_path: &str,
    limit: usize,
) -> Result<Vec<deltalake::arrow::record_batch::RecordBatch>> {
    let path = lake_root.join(table_path);
    if !path.exists() {
        bail!(
            "Delta table '{}' does not exist at {}",
            table_path,
            path.display()
        );
    }
    let url = Url::from_file_path(&path)
        .map_err(|_| anyhow!("invalid table path '{}'", path.display()))?;
    let table = open_table(url).await?;
    let session = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let table_provider: Arc<dyn TableProvider> = Arc::new(table);
    let df = session.read_table(table_provider)?;
    let df = if limit > 0 {
        df.limit(0, Some(limit))?
    } else {
        df
    };
    let batches = df.collect().await?;
    Ok(batches)
}

async fn open_table_summary(lake_root: &Path, table_path: &str) -> Result<TableSummary> {
    let path = lake_root.join(table_path);
    if !path.exists() {
        bail!(
            "Delta table '{}' does not exist at {}",
            table_path,
            path.display()
        );
    }
    let url = Url::from_file_path(&path)
        .map_err(|_| anyhow!("invalid table path '{}'", path.display()))?;
    let table = open_table(url).await?;
    let schema = table.schema();
    let mut columns = Vec::new();
    for field in schema.fields() {
        columns.push(fstorage::models::ColumnSummary {
            name: field.name().to_string(),
            data_type: field.data_type().to_string(),
            nullable: field.is_nullable(),
        });
    }
    columns.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(TableSummary {
        table_path: table_path.to_string(),
        columns,
    })
}

fn list_nodes_by_label(
    engine: &HelixGraphEngine,
    label: &str,
    limit: usize,
) -> Result<Vec<JsonValue>> {
    let txn = engine.storage.graph_env.read_txn()?;
    let mut nodes = Vec::new();
    let mut seen = 0usize;
    for next in engine.storage.nodes_db.iter(&txn)? {
        let (id, raw) = next?;
        let node = Node::decode_node(raw, id).map_err(|err| anyhow!(err.to_string()))?;
        if node.label == label {
            nodes.push(node_to_json(&node));
            seen += 1;
            if limit != 0 && seen >= limit {
                break;
            }
        }
    }
    Ok(nodes)
}

fn print_node_rows(nodes: &[JsonValue]) {
    let mut table = String::new();
    writeln!(
        &mut table,
        "{:<40} {:<16} {}",
        "ID", "LABEL", "NAME/PRIMARY"
    )
    .ok();
    writeln!(&mut table, "{}", "-".repeat(80)).ok();

    for node in nodes {
        let id = node.get("id").and_then(|v| v.as_str()).unwrap_or_default();
        let label = node
            .get("label")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let primary = node
            .get("properties")
            .and_then(|props| props.as_object())
            .and_then(|props| {
                props
                    .get("name")
                    .or_else(|| props.get("title"))
                    .or_else(|| props.get("url"))
            })
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        writeln!(&mut table, "{:<40} {:<16} {}", id, label, primary).ok();
    }
    print!("{table}");
}

fn neighbor_to_json(record: &NeighborRecord) -> JsonValue {
    let mut obj = JsonMap::new();
    obj.insert(
        "orientation".into(),
        JsonValue::String(
            match record.orientation {
                fstorage::lake::NeighborEdgeOrientation::Outgoing => "outgoing",
                fstorage::lake::NeighborEdgeOrientation::Incoming => "incoming",
            }
            .to_string(),
        ),
    );
    obj.insert("edge".into(), map_to_json(record.edge.clone()));
    obj.insert(
        "neighbor_id".into(),
        JsonValue::String(record.node_id.clone()),
    );
    if let Some(props) = &record.node {
        obj.insert("neighbor_node".into(), map_to_json(props.clone()));
    }
    JsonValue::Object(obj)
}

fn subgraph_to_value(subgraph: &Subgraph) -> JsonValue {
    serde_json::json!({
        "nodes": subgraph.nodes.iter().cloned().map(map_to_json).collect::<Vec<_>>(),
        "edges": subgraph.edges.iter().cloned().map(map_to_json).collect::<Vec<_>>(),
    })
}

fn print_neighbor_rows(neighbors: &[NeighborRecord]) {
    let mut table = String::new();
    writeln!(
        &mut table,
        "{:<10} {:<40} {:<20} {:<40}",
        "DIR", "EDGE ID", "EDGE LABEL", "NEIGHBOR ID"
    )
    .ok();
    writeln!(&mut table, "{}", "-".repeat(120)).ok();
    for record in neighbors {
        let orientation = match record.orientation {
            fstorage::lake::NeighborEdgeOrientation::Outgoing => "OUT",
            fstorage::lake::NeighborEdgeOrientation::Incoming => "IN",
        };
        let edge_id = record
            .edge
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        let edge_label = record
            .edge
            .get("label")
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        writeln!(
            &mut table,
            "{:<10} {:<40} {:<20} {:<40}",
            orientation, edge_id, edge_label, record.node_id
        )
        .ok();
    }
    print!("{table}");
}

fn map_to_json(map: HashMap<String, JsonValue>) -> JsonValue {
    let mut obj = JsonMap::new();
    for (k, v) in map {
        obj.insert(k, v);
    }
    JsonValue::Object(obj)
}

fn record_batches_to_json(
    batches: &[deltalake::arrow::record_batch::RecordBatch],
) -> Result<Vec<JsonValue>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            rows.push(record_batch_row_to_json(batch, row_idx)?);
        }
    }
    Ok(rows)
}

fn record_batch_row_to_json(
    batch: &deltalake::arrow::record_batch::RecordBatch,
    row: usize,
) -> Result<JsonValue> {
    let mut obj = JsonMap::new();
    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(col_idx);
        if column.is_null(row) {
            obj.insert(field.name().clone(), JsonValue::Null);
            continue;
        }
        obj.insert(
            field.name().clone(),
            arrow_cell_to_json(column, row).unwrap_or(JsonValue::Null),
        );
    }
    Ok(JsonValue::Object(obj))
}

fn arrow_cell_to_json(array: &ArrayRef, row: usize) -> Option<JsonValue> {
    use deltalake::arrow::array::*;
    use deltalake::arrow::datatypes::DataType;

    match array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => Some(JsonValue::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()?
                .value(row)
                .to_string(),
        )),
        DataType::Boolean => Some(JsonValue::Bool(
            array.as_any().downcast_ref::<BooleanArray>()?.value(row),
        )),
        DataType::Int32 => Some(JsonValue::Number(JsonNumber::from(
            array.as_any().downcast_ref::<Int32Array>()?.value(row),
        ))),
        DataType::Int64 => Some(JsonValue::Number(JsonNumber::from(
            array.as_any().downcast_ref::<Int64Array>()?.value(row),
        ))),
        DataType::UInt32 => Some(JsonValue::Number(JsonNumber::from(
            array.as_any().downcast_ref::<UInt32Array>()?.value(row),
        ))),
        DataType::UInt64 => Some(JsonValue::Number(JsonNumber::from(
            array.as_any().downcast_ref::<UInt64Array>()?.value(row),
        ))),
        DataType::Float32 => {
            JsonNumber::from_f64(array.as_any().downcast_ref::<Float32Array>()?.value(row) as f64)
                .map(JsonValue::Number)
        }
        DataType::Float64 => {
            JsonNumber::from_f64(array.as_any().downcast_ref::<Float64Array>()?.value(row))
                .map(JsonValue::Number)
        }
        DataType::Timestamp(_, _) => {
            let micros = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()?
                .value(row);
            Some(JsonValue::Number(JsonNumber::from(micros)))
        }
        DataType::Struct(_) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>()?;
            let mut obj = JsonMap::new();
            for (field_idx, field) in struct_array.columns().iter().enumerate() {
                let column_names = struct_array.column_names();
                let field_name = column_names
                    .get(field_idx)
                    .map(|name| name.to_string())
                    .unwrap_or_else(|| format!("col_{field_idx}"));
                if field.is_null(row) {
                    obj.insert(field_name.clone(), JsonValue::Null);
                } else if let Some(value) = arrow_cell_to_json(field, row) {
                    obj.insert(field_name.clone(), value);
                }
            }
            Some(JsonValue::Object(obj))
        }
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>()?;
            Some(list_values_to_json(list.value(row)))
        }
        DataType::LargeList(_) => {
            let list = array.as_any().downcast_ref::<LargeListArray>()?;
            Some(list_values_to_json(list.value(row)))
        }
        DataType::Binary => {
            let bytes = array.as_any().downcast_ref::<BinaryArray>()?.value(row);
            Some(JsonValue::String(BASE64.encode(bytes)))
        }
        DataType::LargeBinary => {
            let bytes = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()?
                .value(row);
            Some(JsonValue::String(BASE64.encode(bytes)))
        }
        _ => Some(JsonValue::String(format!(
            "<unsupported {:?}>",
            array.data_type()
        ))),
    }
}

fn list_values_to_json(values: ArrayRef) -> JsonValue {
    let mut result = Vec::new();
    for idx in 0..values.len() {
        if values.is_null(idx) {
            result.push(JsonValue::Null);
        } else if let Some(value) = arrow_cell_to_json(&values, idx) {
            result.push(value);
        } else {
            result.push(JsonValue::Null);
        }
    }
    JsonValue::Array(result)
}

fn offset_to_json(offset: &IngestionOffset) -> JsonValue {
    serde_json::json!({
        "table_path": offset.table_path.clone(),
        "entity_type": offset.entity_type.clone(),
        "category": offset.category.as_str(),
        "primary_keys": offset.primary_keys.clone(),
        "last_version": offset.last_version,
    })
}

fn node_to_json(node: &Node) -> JsonValue {
    let mut obj = JsonMap::new();
    obj.insert(
        "id".into(),
        JsonValue::String(ID::from(node.id).stringify()),
    );
    obj.insert("label".into(), JsonValue::String(node.label.clone()));
    if let Some(props) = &node.properties {
        let mut props_json = JsonMap::new();
        for (k, v) in props {
            props_json.insert(k.clone(), helix_value_to_json(v));
        }
        obj.insert("properties".into(), JsonValue::Object(props_json));
    } else {
        obj.insert("properties".into(), JsonValue::Null);
    }
    JsonValue::Object(obj)
}

fn helix_value_to_json(value: &HelixValue) -> JsonValue {
    match value {
        HelixValue::String(s) => JsonValue::String(s.clone()),
        HelixValue::F32(v) => JsonNumber::from_f64(*v as f64)
            .map(JsonValue::Number)
            .unwrap_or_else(|| JsonValue::String(v.to_string())),
        HelixValue::F64(v) => JsonNumber::from_f64(*v)
            .map(JsonValue::Number)
            .unwrap_or_else(|| JsonValue::String(v.to_string())),
        HelixValue::I8(v) => JsonValue::Number(JsonNumber::from(*v)),
        HelixValue::I16(v) => JsonValue::Number(JsonNumber::from(*v)),
        HelixValue::I32(v) => JsonValue::Number(JsonNumber::from(*v)),
        HelixValue::I64(v) => JsonValue::Number(JsonNumber::from(*v)),
        HelixValue::U8(v) => JsonValue::Number(JsonNumber::from(*v)),
        HelixValue::U16(v) => JsonValue::Number(JsonNumber::from(*v)),
        HelixValue::U32(v) => JsonValue::Number(JsonNumber::from(*v)),
        HelixValue::U64(v) => JsonValue::Number(JsonNumber::from(*v)),
        HelixValue::U128(v) => JsonValue::String(v.to_string()),
        HelixValue::Date(d) => JsonValue::String(d.to_string()),
        HelixValue::Boolean(v) => JsonValue::Bool(*v),
        HelixValue::Id(id) => JsonValue::String(id.stringify()),
        HelixValue::Array(values) => {
            JsonValue::Array(values.iter().map(helix_value_to_json).collect::<Vec<_>>())
        }
        HelixValue::Object(map) => {
            let mut obj = JsonMap::new();
            for (k, v) in map {
                obj.insert(k.clone(), helix_value_to_json(v));
            }
            JsonValue::Object(obj)
        }
        HelixValue::Empty => JsonValue::Null,
    }
}

fn parse_key_value(raw: &str) -> Result<(String, String), String> {
    let (key, value) = raw
        .split_once('=')
        .ok_or_else(|| "expected KEY=VALUE".to_string())?;
    if key.is_empty() {
        return Err("key must not be empty".to_string());
    }
    Ok((key.to_string(), value.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_key_value_splits_pairs() {
        let pair = parse_key_value("name=foo").unwrap();
        assert_eq!(pair.0, "name");
        assert_eq!(pair.1, "foo");
    }

    #[test]
    fn parse_key_value_rejects_missing_equals() {
        assert!(parse_key_value("abc").is_err());
        assert!(parse_key_value("=missing").is_err());
    }
}
