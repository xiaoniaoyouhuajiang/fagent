use std::sync::Arc;

use deltalake::open_table;
use fstorage::{
    catalog::Catalog,
    config::StorageConfig,
    embedding::NullEmbeddingProvider,
    fetch::{Fetchable, GraphData},
    lake::Lake,
    schemas::generated_schemas::Project,
    sync::{DataSynchronizer, FStorageSynchronizer},
    utils,
};
use helix_db::helix_engine::storage_core::storage_methods::StorageMethods;
use helix_db::helix_engine::traversal_core::{HelixGraphEngine, HelixGraphEngineOpts};
use tempfile::tempdir;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    println!("🔄 Graph pipeline smoke-test");
    println!("============================\n");

    let temp_dir = tempdir()?;
    let config = StorageConfig::new(temp_dir.path());

    tokio::fs::create_dir_all(&config.engine_path).await?;

    let catalog = Arc::new(Catalog::new(&config)?);
    catalog.initialize_schema()?;

    let engine_opts = HelixGraphEngineOpts {
        path: config
            .engine_path
            .to_str()
            .ok_or_else(|| fstorage::errors::StorageError::Config("Non-UTF8 engine path".into()))?
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

    let mut graph_data = GraphData::new();
    graph_data.add_entities(vec![Project {
        url: Some("https://github.com/example/repo".to_string()),
        name: Some("example".to_string()),
        description: Some("Graph pipeline smoke test".to_string()),
        language: Some("Rust".to_string()),
        stars: Some(42),
        forks: Some(7),
    }]);

    println!("➡️  Writing graph data via synchronizer...");
    synchronizer.process_graph_data(graph_data).await?;

    println!("✅ Delta Lake write verified");
    let table_path = config.lake_path.join(Project::table_name());
    let table_uri = table_path
        .to_str()
        .ok_or_else(|| fstorage::errors::StorageError::Config("Non-UTF8 table path".into()))?;
    let table = open_table(table_uri).await?;
    println!(
        "  - table version: {}, files: {}",
        table.version(),
        table.get_file_uris().into_iter().count()
    );

    println!("✅ Helix graph write verified");
    let txn = engine.storage.graph_env.read_txn()?;
    let node_id = utils::id::stable_node_id_u128(
        Project::ENTITY_TYPE,
        &[("url", "https://github.com/example/repo".to_string())],
    );
    let expected_uuid = Uuid::from_u128(node_id);
    println!("  - expected node id: {}", expected_uuid);

    let mut node_iter = engine.storage.nodes_db.iter(&txn)?;
    while let Some(entry) = node_iter.next() {
        let (key, _) = entry?;
        let stored_id: u128 = key;
        let stored_uuid = Uuid::from_u128(stored_id);
        println!("  - stored node id: {}", stored_uuid);
    }

    let node = engine.storage.get_node(&txn, &node_id)?;
    println!("  - node label: {}", node.label);
    if let Some(props) = node.properties {
        println!(
            "  - properties keys: {:?}",
            props.keys().collect::<Vec<_>>()
        );
    }

    println!("\n🎉 Graph pipeline smoke-test completed successfully");
    Ok(())
}
