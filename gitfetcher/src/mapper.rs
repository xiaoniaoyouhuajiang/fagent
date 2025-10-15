use std::sync::Arc;

use crate::readme::{chunk_readme, ReadmeChunkPiece};
use deltalake::arrow::{
    array::{Int64Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use fstorage::{
    embedding::EmbeddingProvider,
    errors::{Result as StorageResult, StorageError},
    fetch::Fetchable,
    fetch::GraphData,
    schemas::generated_schemas::{Commit, HasVersion, IsCommit, Project, ReadmeChunk, Version},
    utils::id::{stable_edge_id_u128, stable_node_id_u128},
};
use uuid::Uuid;

use crate::{
    models::{RepoSnapshot, SearchRepository},
    params::RepoSnapshotParams,
};

const README_MAX_LINES_PER_CHUNK: usize = 120;

pub async fn build_repo_snapshot_graph(
    snapshot: &RepoSnapshot,
    params: &RepoSnapshotParams,
    embedding_provider: Arc<dyn EmbeddingProvider>,
) -> StorageResult<GraphData> {
    let repo = &snapshot.repository;
    let commit = &snapshot.commit;
    let revision = &snapshot.revision;

    let project_url = repo.html_url.clone();

    let mut graph = GraphData::new();

    graph.add_entities(vec![Project {
        url: Some(project_url.clone()),
        name: Some(repo.name.clone()),
        description: repo.description.clone(),
        language: repo.language.clone(),
        stars: Some(repo.stargazers as i64),
        forks: Some(repo.forks as i64),
    }]);

    graph.add_entities(vec![Version {
        sha: Some(revision.sha.clone()),
        tag: params.rev.clone(),
        is_head: Some(revision.is_head),
        created_at: Some(commit.authored_at),
    }]);

    graph.add_entities(vec![Commit {
        sha: Some(commit.sha.clone()),
        message: Some(commit.message.clone()),
        committed_at: Some(commit.authored_at),
    }]);

    let project_node_id = uuid_from_node(Project::ENTITY_TYPE, &[("url", project_url.clone())]);
    let version_node_id = uuid_from_node(Version::ENTITY_TYPE, &[("sha", revision.sha.clone())]);
    let commit_node_id = uuid_from_node(Commit::ENTITY_TYPE, &[("sha", commit.sha.clone())]);

    graph.add_entities(vec![HasVersion {
        id: Some(uuid_from_edge(
            HasVersion::ENTITY_TYPE,
            &project_node_id,
            &version_node_id,
        )),
        from_node_id: Some(project_node_id.clone()),
        to_node_id: Some(version_node_id.clone()),
        from_node_type: Some(Project::ENTITY_TYPE.to_string()),
        to_node_type: Some(Version::ENTITY_TYPE.to_string()),
        created_at: Some(commit.authored_at),
        updated_at: Some(commit.authored_at),
    }]);

    graph.add_entities(vec![IsCommit {
        id: Some(uuid_from_edge(
            IsCommit::ENTITY_TYPE,
            &version_node_id,
            &commit_node_id,
        )),
        from_node_id: Some(version_node_id),
        to_node_id: Some(commit_node_id),
        from_node_type: Some(Version::ENTITY_TYPE.to_string()),
        to_node_type: Some(Commit::ENTITY_TYPE.to_string()),
        created_at: Some(commit.authored_at),
        updated_at: Some(commit.authored_at),
    }]);

    if params.include_readme {
        if let Some(readme) = &snapshot.readme {
            let chunk_pieces = chunk_readme(&readme.text, README_MAX_LINES_PER_CHUNK);
            let chunk_texts: Vec<String> = chunk_pieces
                .iter()
                .map(|piece| piece.text.clone())
                .collect();

            let embeddings: Vec<Vec<f32>> = if chunk_texts.is_empty() {
                Vec::new()
            } else {
                embedding_provider
                    .embed(chunk_texts)
                    .await?
                    .into_iter()
                    .map(|values| values.into_iter().map(|v| v as f32).collect())
                    .collect()
            };

            let embedding_model = detect_embedding_model_from_env();

            let mut readme_chunks = Vec::with_capacity(chunk_pieces.len());
            for (idx, piece) in chunk_pieces.into_iter().enumerate() {
                let ReadmeChunkPiece {
                    start_line,
                    end_line,
                    text: chunk_text,
                } = piece;

                let embedding = embeddings.get(idx).cloned().filter(|vec| !vec.is_empty());
                let embedding_model_value =
                    embedding.as_ref().and_then(|_| embedding_model.clone());
                let token_count = approximate_token_count(&chunk_text);

                let embedding_id =
                    embedding_identifier(&repo.full_name, &revision.sha, start_line, end_line, idx);

                readme_chunks.push(ReadmeChunk {
                    source_file: Some(readme.source_file.clone()),
                    start_line: Some(start_line),
                    end_line: Some(end_line),
                    text: Some(chunk_text),
                    embedding,
                    embedding_model: embedding_model_value,
                    embedding_id: Some(embedding_id),
                    token_count,
                    chunk_order: Some(idx as i32),
                    created_at: Some(commit.authored_at),
                    updated_at: None,
                });
            }

            if !readme_chunks.is_empty() {
                graph.add_entities(readme_chunks);
            }
        }
    }

    Ok(graph)
}

fn uuid_from_node(entity_type: &str, keys: &[(&str, String)]) -> String {
    let id = stable_node_id_u128(entity_type, keys);
    Uuid::from_u128(id).to_string()
}

fn uuid_from_edge(edge_label: &str, from: &str, to: &str) -> String {
    let id = stable_edge_id_u128(edge_label, from, to);
    Uuid::from_u128(id).to_string()
}

fn embedding_identifier(
    repo_full_name: &str,
    revision_sha: &str,
    start_line: i32,
    end_line: i32,
    chunk_index: usize,
) -> String {
    let source = format!(
        "readme|{}|{}|{}|{}|{}",
        repo_full_name, revision_sha, start_line, end_line, chunk_index
    );
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, source.as_bytes());
    uuid.to_string()
}

fn approximate_token_count(text: &str) -> Option<i32> {
    let tokens = text.split_whitespace().count();
    i32::try_from(tokens).ok()
}

fn detect_embedding_model_from_env() -> Option<String> {
    for key in [
        "FSTORAGE_EMBEDDING_MODEL",
        "OPENAI_EMBEDDING_MODEL",
        "EMBEDDING_MODEL",
    ] {
        if let Ok(value) = std::env::var(key) {
            if !value.trim().is_empty() {
                return Some(value);
            }
        }
    }
    None
}

pub fn build_search_panel(results: &[SearchRepository]) -> StorageResult<RecordBatch> {
    let schema = Schema::new(vec![
        Field::new("full_name", DataType::Utf8, false),
        Field::new("html_url", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("language", DataType::Utf8, true),
        Field::new("stargazers", DataType::Int64, false),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]);

    let full_name = StringArray::from(
        results
            .iter()
            .map(|row| row.full_name.clone())
            .collect::<Vec<_>>(),
    );
    let html_url = StringArray::from(
        results
            .iter()
            .map(|row| row.html_url.clone())
            .collect::<Vec<_>>(),
    );
    let description = StringArray::from(
        results
            .iter()
            .map(|row| row.description.clone())
            .collect::<Vec<_>>(),
    );
    let language = StringArray::from(
        results
            .iter()
            .map(|row| row.language.clone())
            .collect::<Vec<_>>(),
    );
    let stargazers = Int64Array::from(
        results
            .iter()
            .map(|row| row.stargazers as i64)
            .collect::<Vec<_>>(),
    );
    let updated_at = TimestampMicrosecondArray::from(
        results
            .iter()
            .map(|row| row.updated_at.map(|ts| ts.timestamp_micros()))
            .collect::<Vec<_>>(),
    )
    .with_timezone("UTC");

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(full_name),
            Arc::new(html_url),
            Arc::new(description),
            Arc::new(language),
            Arc::new(stargazers),
            Arc::new(updated_at),
        ],
    )
    .map_err(StorageError::Arrow)?;

    Ok(batch)
}
