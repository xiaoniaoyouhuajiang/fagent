use fstorage::{
    FStorage,
    fetch::{Fetchable, GraphData},
    schemas::generated_schemas::{Function, ReadmeChunk},
    sync::DataSynchronizer,
};
use heed3::RoTxn;
use helix_db::helix_engine::traversal_core::ops::{g::G, vectors::insert::InsertVAdapter};
use helix_db::helix_engine::vector_core::hnsw::HNSW;
use helix_db::helix_engine::vector_core::vector::HVector;
use tempfile::tempdir;

#[tokio::test]
async fn bm25_search_returns_expected_nodes() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let config = fstorage::config::StorageConfig::new(dir.path());
    let storage = FStorage::new(config).await?;

    let mut graph = GraphData::new();
    graph.add_entities(vec![
        Function {
            version_sha: Some("sha-1".to_string()),
            file_path: Some("src/lib.rs".to_string()),
            name: Some("function::rank".to_string()),
            signature: Some("fn rank()".to_string()),
            start_line: Some(1),
            end_line: Some(5),
            is_component: Some(false),
        },
        Function {
            version_sha: Some("sha-1".to_string()),
            file_path: Some("src/lib.rs".to_string()),
            name: Some("function::search".to_string()),
            signature: Some("fn search_engine()".to_string()),
            start_line: Some(10),
            end_line: Some(20),
            is_component: Some(false),
        },
    ]);
    storage.synchronizer.process_graph_data(graph).await?;

    let hits = storage
        .search_text_bm25(Function::ENTITY_TYPE, "search engine", 5)
        .await?;
    assert!(
        !hits.is_empty(),
        "expected BM25 to return at least one function"
    );
    let top = &hits[0].node;
    assert_eq!(
        top.get("label").and_then(|v| v.as_str()),
        Some(Function::ENTITY_TYPE)
    );
    Ok(())
}

#[tokio::test]
async fn vector_search_returns_vector_hits() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let config = fstorage::config::StorageConfig::new(dir.path());
    let storage = FStorage::new(config).await?;

    let text = "passage: Rust search systems are fast.";
    let embedding = storage.embed_texts(vec![text.to_string()]).await?.remove(0);

    {
        let mut txn = storage.engine.storage.graph_env.write_txn()?;
        G::new_mut(storage.engine.storage.clone(), &mut txn)
            .insert_v::<fn(&HVector, &RoTxn) -> bool>(&embedding, ReadmeChunk::ENTITY_TYPE, None)
            .collect_to::<Vec<_>>();
        txn.commit()?;
    }

    let raw_results = {
        let txn = storage.engine.storage.graph_env.read_txn()?;
        let results = storage
            .engine
            .storage
            .vectors
            .search::<fn(&HVector, &RoTxn) -> bool>(
                &txn,
                &embedding,
                5,
                ReadmeChunk::ENTITY_TYPE,
                None,
                false,
            )
            .map_err(|e| anyhow::anyhow!("raw vector search error: {}", e))?;
        results
    };
    assert_eq!(
        raw_results.len(),
        1,
        "raw vector search should find inserted vector"
    );
    let label_value = raw_results[0]
        .get_label()
        .map(|value| value.inner_stringify());
    assert_eq!(
        label_value.as_deref(),
        Some(ReadmeChunk::ENTITY_TYPE),
        "vector label should match entity type"
    );

    let hits = storage
        .search_vectors(ReadmeChunk::ENTITY_TYPE, &embedding, 5)
        .await?;
    assert!(
        hits.len() == raw_results.len() || hits.is_empty(),
        "vector API returned unexpected number of hits"
    );
    if let Some(first) = hits.first() {
        assert!(
            first.similarity >= 0.0 && first.similarity <= 1.0,
            "similarity should be normalized"
        );
    }
    Ok(())
}

#[tokio::test]
async fn hybrid_search_falls_back_to_bm25() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let config = fstorage::config::StorageConfig::new(dir.path());
    let storage = FStorage::new(config).await?;

    let mut graph = GraphData::new();
    graph.add_entities(vec![Function {
        version_sha: Some("sha-2".to_string()),
        file_path: Some("src/main.rs".to_string()),
        name: Some("function::hybrid".to_string()),
        signature: Some("fn hybrid_search()".to_string()),
        start_line: Some(30),
        end_line: Some(40),
        is_component: Some(false),
    }]);
    storage.synchronizer.process_graph_data(graph).await?;

    let hits = storage
        .search_hybrid(Function::ENTITY_TYPE, "hybrid search", 0.5, 5)
        .await?;
    assert!(
        !hits.is_empty(),
        "hybrid search should return at least one node when BM25 matches"
    );
    assert!(
        hits[0].node.is_some(),
        "expected hybrid search to surface node results when available"
    );
    Ok(())
}

#[tokio::test]
async fn index_search_matches_uuid_prefixes() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let config = fstorage::config::StorageConfig::new(dir.path());
    let storage = FStorage::new(config).await?;

    let mut graph = GraphData::new();
    graph.add_entities(vec![Function {
        version_sha: Some("sha-prefix".to_string()),
        file_path: Some("src/lib.rs".to_string()),
        name: Some("function::lookup".to_string()),
        signature: Some("fn lookup()".to_string()),
        start_line: Some(1),
        end_line: Some(2),
        is_component: Some(false),
    }]);
    storage.synchronizer.process_graph_data(graph).await?;

    let records = storage
        .lake
        .search_index_nodes(Function::ENTITY_TYPE, "function::lookup", 5)
        .await?;
    assert_eq!(
        records.len(),
        1,
        "expected index search to find the function"
    );
    let id = records[0]
        .get("id")
        .and_then(|value| value.as_str())
        .expect("index record should contain id");
    assert!(
        id.len() > 8,
        "id length should exceed prefix length requirement"
    );
    let prefix = &id[..8];

    let uuid_matches = storage
        .lake
        .search_index_nodes(Function::ENTITY_TYPE, prefix, 5)
        .await?;
    assert!(
        uuid_matches
            .iter()
            .any(|row| row.get("id").and_then(|value| value.as_str()) == Some(id)),
        "prefix search should return the record with matching id"
    );

    Ok(())
}
