//! Helper utilities for loading captured fetcher responses and replaying them in tests.
//!
//! The fixture pipeline expects the output format produced by the `gitfetcher` capture
//! CLI. Metadata is loaded from `metadata.json`, Arrow batches are reconstructed, and a
//! synthetic `FixtureFetcher` exposes them via the standard fetcher trait so that the
//! full synchronization pipeline can be exercised without hitting external services.

use std::{collections::HashMap, fs::File, path::Path, sync::Arc};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use deltalake::arrow::{
    ipc::reader::FileReader,
    record_batch::RecordBatch,
};
use fstorage::{
    embedding::EmbeddingProvider,
    errors::{Result as StorageResult, StorageError},
    fetch::{
        AnyFetchable, EntityCategory, FetchResponse, Fetchable, Fetcher, FetcherCapability,
        GraphData, ProbeReport,
    },
    schemas::generated_schemas as schemas,
};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct DatasetEntry {
    pub category: String,
    pub name: String,
    pub arrow: String,
    #[serde(default)]
    pub json: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct FixtureMetadata {
    pub fetcher: String,
    pub params: JsonValue,
    pub captured_at: String,
    pub datasets: Vec<DatasetEntry>,
}

#[allow(dead_code)]
pub struct FixtureDataset {
    pub category: EntityCategory,
    pub name: String,
    pub batches: Vec<RecordBatch>,
}

#[allow(dead_code)]
pub struct FixtureData {
    pub metadata: FixtureMetadata,
    pub datasets: Vec<FixtureDataset>,
}

impl FixtureData {
    pub fn load_from_dir(dir: &Path) -> Result<Self> {
        let metadata_path = dir.join("metadata.json");
        let metadata_file = File::open(&metadata_path)
            .with_context(|| format!("failed to open {:?}", metadata_path))?;
        let metadata: FixtureMetadata =
            serde_json::from_reader(metadata_file).context("failed to parse metadata.json")?;

        let mut datasets = Vec::new();
        for entry in &metadata.datasets {
            let arrow_path = dir.join(&entry.arrow);
            let category = entry
                .category
                .parse::<EntityCategory>()
                .with_context(|| format!("invalid category '{}' in fixture", entry.category))?;
            let batches = read_arrow_batches(&arrow_path)
                .with_context(|| format!("failed to read {:?}", arrow_path))?;
            datasets.push(FixtureDataset {
                category,
                name: entry.name.clone(),
                batches,
            });
        }

        Ok(Self { metadata, datasets })
    }
}

fn read_arrow_batches(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(path).with_context(|| format!("failed to open {:?}", path))?;
    let reader = FileReader::try_new(file, None).context("failed to create arrow reader")?;
    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(batches)
}

pub struct FixtureFetcher {
    fixtures: Arc<Mutex<HashMap<String, FixtureData>>>,
}

impl FixtureFetcher {
    pub fn new() -> Self {
        Self {
            fixtures: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn register_fixture(&self, key: impl Into<String>, data: FixtureData) {
        self.fixtures.lock().await.insert(key.into(), data);
    }

    pub async fn load_and_register(
        &self,
        key: impl Into<String>,
        directory: impl AsRef<Path>,
    ) -> Result<()> {
        let key = key.into();
        let data = FixtureData::load_from_dir(directory.as_ref())?;
        self.register_fixture(key, data).await;
        Ok(())
    }
}

#[async_trait]
impl Fetcher for FixtureFetcher {
    fn name(&self) -> &'static str {
        "fixture_fetcher"
    }

    fn capability(&self) -> FetcherCapability {
        FetcherCapability {
            name: self.name(),
            description: "Serves pre-recorded fixture data for synchronization tests",
            param_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "fixture_key": { "type": "string" }
                },
                "required": ["fixture_key"]
            }),
            produces: Vec::new(),
            default_ttl_secs: None,
            examples: vec![serde_json::json!({"fixture_key": "tinykv"})],
        }
    }

    async fn probe(&self, params: JsonValue) -> StorageResult<ProbeReport> {
        let key = params.get("fixture_key").and_then(JsonValue::as_str);
        let fixtures = self.fixtures.lock().await;
        Ok(ProbeReport {
            fresh: Some(key.map(|k| fixtures.contains_key(k)).unwrap_or(false)),
            remote_anchor: None,
            local_anchor: None,
            anchor_key: None,
            estimated_missing: None,
            rate_limit_left: None,
            reason: None,
        })
    }

    async fn fetch(
        &self,
        params: serde_json::Value,
        _embedding_provider: Arc<dyn EmbeddingProvider>,
    ) -> StorageResult<FetchResponse> {
        let fixture_key = params
            .get("fixture_key")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| {
                StorageError::InvalidArg(
                    "FixtureFetcher requires 'fixture_key' parameter".to_string(),
                )
            })?;

        let fixtures = self.fixtures.lock().await;
        let data = fixtures.get(fixture_key).ok_or_else(|| {
            StorageError::InvalidArg(format!("fixture '{}' is not registered", fixture_key))
        })?;

        let mut graph = GraphData::new();
        for dataset in &data.datasets {
            let spec = entity_spec(&dataset.name, dataset.category).map_err(|err| {
                StorageError::InvalidArg(format!(
                    "unsupported fixture dataset '{}': {}",
                    dataset.name, err
                ))
            })?;
            graph.entities.push(Box::new(FixtureBatch {
                category: dataset.category,
                spec,
                batches: dataset.batches.clone(),
            }) as Box<dyn AnyFetchable>);
        }

        Ok(FetchResponse::GraphData(graph))
    }
}

struct EntitySpec {
    entity_type: &'static str,
    primary_keys: Vec<&'static str>,
    table_name: String,
}

fn entity_spec(name: &str, category: EntityCategory) -> Result<EntitySpec> {
    match name {
        "project" => Ok(EntitySpec {
            entity_type: schemas::Project::ENTITY_TYPE,
            primary_keys: schemas::Project::primary_keys(),
            table_name: schemas::Project::table_name(),
        }),
        "version" => Ok(EntitySpec {
            entity_type: schemas::Version::ENTITY_TYPE,
            primary_keys: schemas::Version::primary_keys(),
            table_name: schemas::Version::table_name(),
        }),
        "commit" => Ok(EntitySpec {
            entity_type: schemas::Commit::ENTITY_TYPE,
            primary_keys: schemas::Commit::primary_keys(),
            table_name: schemas::Commit::table_name(),
        }),
        "edge_hasversion" => Ok(EntitySpec {
            entity_type: schemas::HasVersion::ENTITY_TYPE,
            primary_keys: schemas::HasVersion::primary_keys(),
            table_name: format!("silver/edges/{}", trim_edge_prefix(name)),
        }),
        "edge_iscommit" => Ok(EntitySpec {
            entity_type: schemas::IsCommit::ENTITY_TYPE,
            primary_keys: schemas::IsCommit::primary_keys(),
            table_name: format!("silver/edges/{}", trim_edge_prefix(name)),
        }),
        "readmechunk" => Ok(EntitySpec {
            entity_type: schemas::ReadmeChunk::ENTITY_TYPE,
            primary_keys: schemas::ReadmeChunk::primary_keys(),
            table_name: schemas::ReadmeChunk::table_name(),
        }),
        other => Err(anyhow!(
            "dataset '{}' (category {:?}) is not yet supported",
            other,
            category
        )),
    }
}

fn trim_edge_prefix(name: &str) -> String {
    name.strip_prefix("edge_")
        .unwrap_or(name)
        .to_lowercase()
}

struct FixtureBatch {
    category: EntityCategory,
    spec: EntitySpec,
    batches: Vec<RecordBatch>,
}

impl AnyFetchable for FixtureBatch {
    fn to_record_batch_any(&self) -> StorageResult<RecordBatch> {
        if self.batches.is_empty() {
            return Err(StorageError::InvalidArg(format!(
                "fixture dataset '{}' contains no batches",
                self.spec.entity_type
            )));
        }
        if self.batches.len() == 1 {
            return Ok(self.batches[0].clone());
        }
        let schema = self.batches[0].schema();
        let combined = self
            .batches
            .iter()
            .try_fold(vec![], |mut acc, batch| -> Result<_, StorageError> {
                acc.push(batch.clone());
                Ok(acc)
            })?
            .into_iter()
            .collect::<Vec<_>>();
        deltalake::arrow::compute::concat_batches(&schema, &combined)
            .map_err(|err| StorageError::Arrow(err.into()))
    }

    fn entity_type_any(&self) -> &'static str {
        self.spec.entity_type
    }

    fn category_any(&self) -> EntityCategory {
        self.category
    }

    fn primary_keys_any(&self) -> Vec<&'static str> {
        self.spec.primary_keys.clone()
    }

    fn table_name(&self) -> String {
        self.spec.table_name.clone()
    }
}
