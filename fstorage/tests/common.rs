use std::sync::Arc;

use fstorage::{
    catalog::Catalog, config::StorageConfig, embedding::NullEmbeddingProvider, lake::Lake,
    sync::FStorageSynchronizer,
};
use helix_db::helix_engine::traversal_core::{HelixGraphEngine, HelixGraphEngineOpts};
use tempfile::TempDir;

#[allow(dead_code)]
pub struct TestContext {
    pub temp_dir: TempDir,
    pub config: StorageConfig,
    pub catalog: Arc<Catalog>,
    pub engine: Arc<HelixGraphEngine>,
    pub lake: Arc<Lake>,
    pub synchronizer: FStorageSynchronizer,
}

pub async fn init_test_context() -> anyhow::Result<TestContext> {
    let temp_dir = tempfile::tempdir()?;
    let config = StorageConfig::new(temp_dir.path());

    tokio::fs::create_dir_all(&config.engine_path).await?;

    let catalog = Arc::new(Catalog::new(&config)?);
    catalog.initialize_schema()?;

    let engine_opts = HelixGraphEngineOpts {
        path: config
            .engine_path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("engine path contains invalid UTF-8"))?
            .to_string(),
        ..Default::default()
    };
    let engine = Arc::new(HelixGraphEngine::new(engine_opts)?);

    let lake = Arc::new(Lake::new(config.clone(), Arc::clone(&engine)).await?);

    let synchronizer = FStorageSynchronizer::new(
        Arc::clone(&catalog),
        Arc::clone(&lake),
        Arc::clone(&engine),
        Arc::new(NullEmbeddingProvider),
    );

    Ok(TestContext {
        temp_dir,
        config,
        catalog,
        engine,
        lake,
        synchronizer,
    })
}
