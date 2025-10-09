use fstorage::fetch::Fetchable;
use fstorage::schemas::generated_schemas::Project;

fn main() {
    // 1. Manually create an instance of the generated `PROJECT` struct.
    let project_instance = Project {
        url: Some("https://github.com/example/repo".to_string()),
        name: Some("example-repo".to_string()),
        description: Some("An example repository".to_string()),
        language: Some("Rust".to_string()),
        stars: Some(100),
        forks: Some(20),
    };

    let projects = vec![project_instance];

    // Test to_record_batch
    match Project::to_record_batch(projects) {
        Ok(batch) => {
            println!("Successfully converted to RecordBatch!");
            println!("Schema: {:?}", batch.schema());
            println!("Number of rows: {}", batch.num_rows());
            // You can add more detailed checks here if needed.
        }
        Err(e) => {
            eprintln!("Failed to convert to RecordBatch: {:?}", e);
            std::process::exit(1);
        }
    }
}
