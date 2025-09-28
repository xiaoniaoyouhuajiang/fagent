use fstorage::fetch::Fetchable;
use fstorage::schemas::PROJECT;

fn main() {
    // 1. Manually create an instance of the generated `PROJECT` struct.
    let project_instance = PROJECT {
        url: Some("https://github.com/test/repo".to_string()),
        name: Some("test-repo".to_string()),
        description: Some("A test repository".to_string()),
        language: Some("Rust".to_string()),
        stars: Some(42),
        forks: Some(5),
    };

    // 2. Create a Vec with one instance.
    let projects = vec![project_instance];

    // 3. Call the `to_record_batch` method from the `Fetchable` trait.
    match PROJECT::to_record_batch(projects) {
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
