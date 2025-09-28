use fstorage::{FStorage, config::StorageConfig};
use tempfile::tempdir;
use deltalake::arrow::{array::{StringArray, Int64Array}, datatypes::{DataType, Field, Schema}, record_batch::RecordBatch};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🏗️  Testing Schema-based DataLake Layout Creation");
    println!("================================================\n");
    
    // Create a temporary directory for testing
    let dir = tempdir()?;
    let config = StorageConfig::new(dir.path());
    
    println!("📁 Base directory: {:?}", dir.path());
    
    // Initialize FStorage
    let storage = FStorage::new(config.clone()).await?;
    
    println!("✅ FStorage initialized");
    
    // Check initial directory structure
    println!("\n📂 Initial directory structure:");
    print_dir_structure(dir.path())?;
    
    // Test creating Delta tables for each schema entity type
    println!("\n🚀 Creating schema-based Delta tables...");
    
    // Create PROJECT entities table
    let project_schema = Arc::new(Schema::new(vec![
        Field::new("url", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("language", DataType::Utf8, true),
        Field::new("stars", DataType::Int64, true),
        Field::new("forks", DataType::Int64, true),
    ]));
    
    let sample_projects = RecordBatch::try_new(
        project_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "https://github.com/rust-lang/rust",
                "https://github.com/tokio-rs/tokio",
            ])),
            Arc::new(StringArray::from(vec!["Rust", "Tokio"])),
            Arc::new(StringArray::from(vec![
                "The Rust Programming Language",
                "A runtime for writing reliable network applications",
            ])),
            Arc::new(StringArray::from(vec!["Rust", "Rust"])),
            Arc::new(Int64Array::from(vec![91000, 23000])),
            Arc::new(Int64Array::from(vec![19000, 3500])),
        ],
    )?;
    
    // Write PROJECT entities to silver layer
    println!("📊 Writing PROJECT entities to silver layer...");
    storage.lake.write_batches("silver/entities/projects", vec![sample_projects], None).await?;
    
    // Create DEVELOPER entities table
    let developer_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("followers", DataType::Int64, true),
        Field::new("location", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
    ]));
    
    let sample_developers = RecordBatch::try_new(
        developer_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob"])),
            Arc::new(Int64Array::from(vec![120, 85])),
            Arc::new(StringArray::from(vec!["San Francisco", "New York"])),
            Arc::new(StringArray::from(vec!["alice@example.com", "bob@example.com"])),
        ],
    )?;
    
    println!("👤 Writing DEVELOPER entities to silver layer...");
    storage.lake.write_batches("silver/entities/developers", vec![sample_developers], None).await?;
    
    // Check final directory structure
    println!("\n📂 Final DataLake directory structure:");
    print_dir_structure(dir.path())?;
    
    // Verify Delta tables were created
    println!("\n✅ Delta Table Verification:");
    verify_delta_tables(&storage).await?;
    
    println!("\n🎯 Schema-based DataLake Layout Summary:");
    println!("============================================");
    println!("✅ Lake path: {:?}", config.lake_path);
    println!("✅ Bronze layer: {}/bronze/", config.lake_path.display());
    println!("✅ Silver layer: {}/silver/", config.lake_path.display());
    println!("✅ Entity tables: {}/silver/entities/", config.lake_path.display());
    println!("✅ PROJECT table: {}/silver/entities/projects/", config.lake_path.display());
    println!("✅ DEVELOPER table: {}/silver/entities/developers/", config.lake_path.display());
    
    Ok(())
}

fn print_dir_structure(path: &std::path::Path) -> std::io::Result<()> {
    fn visit_dir(dir: &std::path::Path, prefix: &str) -> std::io::Result<()> {
        let mut entries = std::fs::read_dir(dir)?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .collect::<Vec<_>>();
        entries.sort_by_key(|e| e.path());
        
        for (i, entry) in entries.iter().enumerate() {
            let path = entry.path();
            let filename = path.file_name().unwrap().to_string_lossy();
            let is_last = i == entries.len() - 1;
            let current_prefix = if is_last { "└── " } else { "├── " };
            let next_prefix = if is_last { "    " } else { "│   " };
            
            println!("{}{}{}", prefix, current_prefix, filename);
            
            if path.is_dir() {
                visit_dir(&path, &(prefix.to_string() + next_prefix))?;
            }
        }
        Ok(())
    }
    
    let display_path = path.file_name().unwrap_or_else(|| path.as_os_str());
    println!("{}", display_path.to_string_lossy());
    visit_dir(path, "")
}

async fn verify_delta_tables(storage: &FStorage) -> Result<(), Box<dyn std::error::Error>> {
    let base_path = &storage.config.lake_path;
    
    // Check PROJECT table
    match deltalake::open_table(base_path.join("silver/entities/projects").to_str().unwrap()).await {
        Ok(table) => {
            println!("  ✅ PROJECT table: {} files, version {}", 
                table.get_file_uris().into_iter().count(), 
                table.version());
        }
        Err(e) => println!("  ❌ PROJECT table: {}", e),
    }
    
    // Check DEVELOPER table
    match deltalake::open_table(base_path.join("silver/entities/developers").to_str().unwrap()).await {
        Ok(table) => {
            println!("  ✅ DEVELOPER table: {} files, version {}", 
                table.get_file_uris().into_iter().count(), 
                table.version());
        }
        Err(e) => println!("  ❌ DEVELOPER table: {}", e),
    }
    
    Ok(())
}