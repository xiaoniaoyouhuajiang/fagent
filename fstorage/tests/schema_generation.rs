use chrono::Utc;
use fstorage::{
    fetch::Fetchable,
    schemas::generated_schemas::{Project, ReadmeChunk},
};

#[test]
fn generated_structs_convert_to_record_batch() {
    let project = Project {
        url: Some("https://github.com/example/repo".to_string()),
        name: Some("example-repo".to_string()),
        description: Some("An example repository".to_string()),
        language: Some("Rust".to_string()),
        stars: Some(100),
        forks: Some(20),
    };

    let batch = Project::to_record_batch(vec![project]).expect("record batch conversion succeeds");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.schema().fields().len(), 6);
}

#[test]
fn generated_vector_struct_handles_embedding() {
    let chunk = ReadmeChunk {
        source_file: Some("README.md".to_string()),
        start_line: Some(1),
        end_line: Some(20),
        text: Some("# Example".to_string()),
        embedding: Some(vec![0.1_f32, 0.2_f32, 0.3_f32]),
        embedding_model: Some("test-model".to_string()),
        embedding_id: Some("chunk-1".to_string()),
        token_count: Some(12),
        chunk_order: Some(0),
        created_at: Some(Utc::now()),
        updated_at: None,
    };

    let batch = ReadmeChunk::to_record_batch(vec![chunk]).expect("vector batch conversion");
    assert_eq!(batch.num_rows(), 1);
    assert!(
        batch
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == "embedding")
    );
}
