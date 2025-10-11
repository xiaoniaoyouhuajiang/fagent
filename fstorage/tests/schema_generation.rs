use fstorage::{fetch::Fetchable, schemas::generated_schemas::Project};

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
