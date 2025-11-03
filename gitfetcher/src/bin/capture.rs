use std::{
    fs::{create_dir_all, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use arrow_json::ArrayWriter;
use chrono::Utc;
use clap::Parser;
use deltalake::arrow::ipc::writer::FileWriter;
use fstorage::{
    embedding::NullEmbeddingProvider,
    errors::StorageError,
    fetch::{EntityCategory, FetchResponse, Fetcher},
};
use gitfetcher::GitFetcher;
use serde_json::Value as JsonValue;

#[derive(Parser, Debug)]
#[command(
    name = "gitfetcher-capture",
    about = "Capture gitfetcher FetchResponse and persist as reusable fixtures",
    after_help = "Example:\n  cargo run -p gitfetcher --bin capture -- \\\n    --params '{\"mode\":\"repo_snapshot\",\"repo\":\"talent-plan/tinykv\",\"include_code\":true}' \\\n    --output-dir fixtures/tinykv --emit-json"
)]
struct Args {
    /// JSON string describing fetch parameters (matches fetcher capability schema).
    #[arg(long, conflicts_with = "params_file")]
    params: Option<String>,

    /// Path to a JSON file containing fetch parameters.
    #[arg(long)]
    params_file: Option<PathBuf>,

    /// Output directory to store captured data.
    #[arg(long, default_value = "captures")]
    output_dir: PathBuf,

    /// GitHub token; if omitted the GITHUB_TOKEN environment variable is used.
    #[arg(long)]
    token: Option<String>,

    /// Additionally emit JSON copies of each dataset alongside Arrow files.
    #[arg(long)]
    emit_json: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .try_init();

    let args = Args::parse();
    run(args).await
}

async fn run(args: Args) -> Result<()> {
    let params_value = load_params(&args)?;
    let _validated: gitfetcher::params::FetcherParams =
        serde_json::from_value(params_value.clone())
            .context("params do not conform to gitfetcher schema")?;

    let token = args
        .token
        .or_else(|| std::env::var("GITHUB_TOKEN").ok())
        .context("GitHub token must be provided via --token or GITHUB_TOKEN")?;

    log::info!(
        "Starting capture with params: {}",
        params_value.to_string().replace('\n', "")
    );

    let fetcher = GitFetcher::with_default_client(Some(token))
        .context("failed to initialize GitFetcher client")?;

    log::info!("Fetching repository snapshot …");
    let response = fetcher
        .fetch(params_value.clone(), Arc::new(NullEmbeddingProvider))
        .await
        .context("fetcher execution failed")?;

    log::info!("Fetch completed, persisting datasets to {:?}", args.output_dir);
    persist_response(&args.output_dir, &params_value, response, args.emit_json)?;
    log::info!("Capture finished successfully");
    Ok(())
}

fn load_params(args: &Args) -> Result<JsonValue> {
    if let Some(ref raw) = args.params {
        let value: JsonValue =
            serde_json::from_str(raw).context("failed to parse --params JSON string")?;
        return Ok(value);
    }

    if let Some(ref path) = args.params_file {
        let file = File::open(path).with_context(|| format!("failed to open {:?}", path))?;
        let value: JsonValue =
            serde_json::from_reader(file).context("failed to parse params file")?;
        return Ok(value);
    }

    anyhow::bail!("fetch params must be supplied via --params or --params-file");
}

fn persist_response(
    output_dir: &Path,
    params: &JsonValue,
    response: FetchResponse,
    emit_json: bool,
) -> Result<()> {
    create_dir_all(output_dir).context("failed to create output directory")?;

    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "fetcher".to_string(),
        JsonValue::String("gitfetcher".into()),
    );
    metadata.insert("params".to_string(), params.clone());
    metadata.insert(
        "captured_at".to_string(),
        JsonValue::String(Utc::now().to_rfc3339()),
    );

    let mut datasets = Vec::new();

    match response {
        FetchResponse::GraphData(mut graph) => {
            for entity in graph.entities.drain(..) {
                let entity_type = entity.entity_type_any();
                let category = entity.category_any();
                let batch = entity
                    .to_record_batch_any()
                    .map_err(to_anyhow("failed to convert record batch"))?;
                let arrow_path =
                    materialize_record_batch(output_dir, category, entity_type, &batch)?;
                let json_path = if emit_json {
                    Some(materialize_record_batch_json(
                        output_dir,
                        category,
                        entity_type,
                        &batch,
                    )?)
                } else {
                    None
                };
                datasets.push(dataset_entry(
                    category.as_str(),
                    entity_type,
                    arrow_path,
                    json_path,
                    output_dir,
                ));
            }
        }
        FetchResponse::PanelData { table_name, batch } => {
            let arrow_path = materialize_panel(output_dir, &table_name, &batch)?;
            let json_path = if emit_json {
                Some(materialize_panel_json(output_dir, &table_name, &batch)?)
            } else {
                None
            };
            datasets.push(dataset_entry(
                "panel",
                &table_name,
                arrow_path,
                json_path,
                output_dir,
            ));
        }
    }

    metadata.insert("datasets".to_string(), JsonValue::Array(datasets));

    let metadata_path = output_dir.join("metadata.json");
    let mut metadata_file = File::create(&metadata_path)
        .with_context(|| format!("failed to create {:?}", metadata_path))?;
    serde_json::to_writer_pretty(&mut metadata_file, &JsonValue::Object(metadata))
        .context("failed to write metadata")?;
    Ok(())
}

fn materialize_record_batch(
    output_dir: &Path,
    category: EntityCategory,
    entity_type: &str,
    batch: &deltalake::arrow::record_batch::RecordBatch,
) -> Result<PathBuf> {
    let subdir = match category {
        EntityCategory::Node => "nodes",
        EntityCategory::Edge => "edges",
        EntityCategory::Vector => "vectors",
    };

    let dir = output_dir.join(subdir);
    create_dir_all(&dir)
        .with_context(|| format!("failed to create category directory {:?}", dir))?;

    let file_path = dir.join(format!("{entity_type}.arrow"));
    let file =
        File::create(&file_path).with_context(|| format!("failed to create {:?}", file_path))?;
    let mut writer = FileWriter::try_new(BufWriter::new(file), &batch.schema())
        .context("failed to create Arrow file writer")?;
    writer.write(batch)?;
    writer
        .finish()
        .context("failed to finalize Arrow file writer")?;
    Ok(file_path)
}

fn materialize_record_batch_json(
    output_dir: &Path,
    category: EntityCategory,
    entity_type: &str,
    batch: &deltalake::arrow::record_batch::RecordBatch,
) -> Result<PathBuf> {
    let subdir = match category {
        EntityCategory::Node => "nodes",
        EntityCategory::Edge => "edges",
        EntityCategory::Vector => "vectors",
    };

    let dir = output_dir.join(subdir);
    create_dir_all(&dir)
        .with_context(|| format!("failed to create category directory {:?}", dir))?;

    let file_path = dir.join(format!("{entity_type}.json"));
    let mut json_writer = ArrayWriter::new(Vec::new());
    json_writer
        .write_batches(&[batch])
        .map_err(|err| anyhow::anyhow!("failed to encode json array: {err}"))?;
    json_writer
        .finish()
        .map_err(|err| anyhow::anyhow!("failed to finalize json writer: {err}"))?;
    let json_bytes = json_writer.into_inner();
    let mut writer = BufWriter::new(
        File::create(&file_path).with_context(|| format!("failed to create {:?}", file_path))?,
    );
    writer
        .write_all(&json_bytes)
        .with_context(|| format!("failed to write json for {:?}", file_path))?;
    writer
        .flush()
        .with_context(|| format!("failed to flush json writer for {:?}", file_path))?;
    Ok(file_path)
}

fn materialize_panel(
    output_dir: &Path,
    table_name: &str,
    batch: &deltalake::arrow::record_batch::RecordBatch,
) -> Result<PathBuf> {
    let dir = output_dir.join("panels");
    create_dir_all(&dir).with_context(|| format!("failed to create {:?}", dir))?;
    let sanitized = table_name
        .replace('/', "_")
        .replace('\\', "_")
        .replace(':', "_");
    let file_path = dir.join(format!("{sanitized}.arrow"));
    let file =
        File::create(&file_path).with_context(|| format!("failed to create {:?}", file_path))?;
    let mut writer = FileWriter::try_new(BufWriter::new(file), &batch.schema())
        .context("failed to create Arrow file writer")?;
    writer.write(batch)?;
    writer
        .finish()
        .context("failed to finalize panel Arrow file writer")?;
    Ok(file_path)
}

fn materialize_panel_json(
    output_dir: &Path,
    table_name: &str,
    batch: &deltalake::arrow::record_batch::RecordBatch,
) -> Result<PathBuf> {
    let dir = output_dir.join("panels");
    create_dir_all(&dir).with_context(|| format!("failed to create {:?}", dir))?;
    let sanitized = table_name
        .replace('/', "_")
        .replace('\\', "_")
        .replace(':', "_");
    let file_path = dir.join(format!("{sanitized}.json"));
    let mut json_writer = ArrayWriter::new(Vec::new());
    json_writer
        .write_batches(&[batch])
        .map_err(|err| anyhow::anyhow!("failed to encode json array: {err}"))?;
    json_writer
        .finish()
        .map_err(|err| anyhow::anyhow!("failed to finalize json writer: {err}"))?;
    let json_bytes = json_writer.into_inner();
    let mut writer = BufWriter::new(
        File::create(&file_path).with_context(|| format!("failed to create {:?}", file_path))?,
    );
    writer
        .write_all(&json_bytes)
        .with_context(|| format!("failed to write json for {:?}", file_path))?;
    writer
        .flush()
        .with_context(|| format!("failed to flush json writer for {:?}", file_path))?;
    Ok(file_path)
}

fn dataset_entry(
    category: &str,
    name: &str,
    arrow_path: PathBuf,
    json_path: Option<PathBuf>,
    output_dir: &Path,
) -> JsonValue {
    let mut map = serde_json::Map::new();
    map.insert(
        "category".to_string(),
        JsonValue::String(category.to_string()),
    );
    map.insert("name".to_string(), JsonValue::String(name.to_string()));
    map.insert(
        "arrow".to_string(),
        JsonValue::String(relative_path(&arrow_path, output_dir)),
    );
    if let Some(path) = json_path {
        map.insert(
            "json".to_string(),
            JsonValue::String(relative_path(&path, output_dir)),
        );
    }
    JsonValue::Object(map)
}

fn relative_path(path: &Path, root: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

fn to_anyhow<'a>(msg: &'a str) -> impl Fn(StorageError) -> anyhow::Error + 'a {
    move |err| anyhow::anyhow!("{msg}: {err}")
}
