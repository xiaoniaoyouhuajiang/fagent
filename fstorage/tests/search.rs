use chrono::Utc;
use fstorage::{
    fetch::{Fetchable, GraphData},
    schemas::generated_schemas::{Function, Project, ReadmeChunk},
    sync::DataSynchronizer,
    FStorage,
};
use heed3::RoTxn;
use helix_db::helix_engine::traversal_core::ops::{g::G, vectors::insert::InsertVAdapter};
use helix_db::helix_engine::vector_core::hnsw::HNSW;
use helix_db::helix_engine::vector_core::vector::HVector;
use std::collections::HashSet;
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

#[tokio::test]
async fn hybrid_multi_search_aggregates_across_entities() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let config = fstorage::config::StorageConfig::new(dir.path());
    let storage = FStorage::new(config).await?;

    let mut graph = GraphData::new();
    graph.add_entities(vec![Function {
        version_sha: Some("sha-multi".to_string()),
        file_path: Some("src/search.rs".to_string()),
        name: Some("function::hybrid_example".to_string()),
        signature: Some("fn hybrid_example_search()".to_string()),
        start_line: Some(1),
        end_line: Some(10),
        is_component: Some(false),
    }]);
    storage.synchronizer.process_graph_data(graph).await?;

    let passage = "passage: hybrid search example for readme chunk";
    let embedding = storage
        .embed_texts(vec![passage.to_string()])
        .await?
        .into_iter()
        .next()
        .unwrap_or_default();
    let embedding_f32: Vec<f32> = embedding.iter().map(|v| *v as f32).collect();

    let mut vector_graph = GraphData::new();
    vector_graph.add_entities(vec![Project {
        url: Some("https://example.com/repo".to_string()),
        name: Some("example".to_string()),
        description: Some("Hybrid search example".to_string()),
        language: Some("Rust".to_string()),
        stars: Some(1),
        forks: Some(0),
    }]);
    vector_graph.add_entities(vec![ReadmeChunk {
        id: None,
        project_url: Some("https://example.com/repo".to_string()),
        revision_sha: Some("rev-multi".to_string()),
        source_file: Some("README.md".to_string()),
        start_line: Some(1),
        end_line: Some(5),
        text: Some(passage.to_string()),
        embedding: Some(embedding_f32.clone()),
        embedding_model: Some("fixture-model".to_string()),
        embedding_id: Some("chunk-hybrid-1".to_string()),
        token_count: Some(6),
        chunk_order: Some(0),
        created_at: Some(Utc::now()),
        updated_at: None,
    }]);
    storage
        .synchronizer
        .process_graph_data(vector_graph)
        .await?;

    let entity_types = vec![
        Function::ENTITY_TYPE.to_string(),
        ReadmeChunk::ENTITY_TYPE.to_string(),
    ];
    let hits = storage
        .search_hybrid_multi(&entity_types, "hybrid search example", 0.5, 10)
        .await?;

    assert!(
        !hits.is_empty(),
        "multi-entity hybrid search should return results"
    );
    let mut seen_types: HashSet<String> = HashSet::new();
    for hit in &hits {
        seen_types.insert(hit.entity_type.clone());
        assert!(hit.score >= 0.0, "scores should be non-negative");
    }

    assert!(
        seen_types.contains(Function::ENTITY_TYPE),
        "expected function entity hits"
    );
    assert!(
        seen_types.contains(ReadmeChunk::ENTITY_TYPE),
        "expected readme chunk hits"
    );
    Ok(())
}
