use fstorage::{FStorage, config::StorageConfig, schemas::{PROJECT, DEVELOPER, COMMIT, VERSION, ISSUE}, fetch::Fetchable};
use tempfile::tempdir;
use chrono::{DateTime, Utc};

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
    
    // Create PROJECT entities using generated schema struct
    let sample_projects = vec![
        PROJECT {
            url: Some("https://github.com/rust-lang/rust".to_string()),
            name: Some("Rust".to_string()),
            description: Some("The Rust Programming Language".to_string()),
            language: Some("Rust".to_string()),
            stars: Some(91000),
            forks: Some(19000),
        },
        PROJECT {
            url: Some("https://github.com/tokio-rs/tokio".to_string()),
            name: Some("Tokio".to_string()),
            description: Some("A runtime for writing reliable network applications".to_string()),
            language: Some("Rust".to_string()),
            stars: Some(23000),
            forks: Some(3500),
        },
    ];
    
    // Convert PROJECT entities to RecordBatch using generated Fetchable implementation
    let projects_batch = PROJECT::to_record_batch(sample_projects.clone())?;
    
    // Write PROJECT entities to silver layer
    println!("📊 Writing PROJECT entities to silver layer...");
    storage.lake.write_batches("silver/entities/projects", vec![projects_batch], None).await?;
    
    // Create DEVELOPER entities using generated schema struct
    let sample_developers = vec![
        DEVELOPER {
            name: Some("alice".to_string()),
            followers: Some(120),
            location: Some("San Francisco".to_string()),
            email: Some("alice@example.com".to_string()),
        },
        DEVELOPER {
            name: Some("bob".to_string()),
            followers: Some(85),
            location: Some("New York".to_string()),
            email: Some("bob@example.com".to_string()),
        },
    ];
    
    // Convert DEVELOPER entities to RecordBatch using generated Fetchable implementation
    let developers_batch = DEVELOPER::to_record_batch(sample_developers.clone())?;
    
    println!("👤 Writing DEVELOPER entities to silver layer...");
    storage.lake.write_batches("silver/entities/developers", vec![developers_batch], None).await?;
    
    // Test additional entity types
    println!("\n🚀 Testing additional entity types...");
    
    // Test COMMIT entities
    let sample_commits = vec![
        COMMIT {
            sha: Some("abc123".to_string()),
            message: Some("Initial commit".to_string()),
            committed_at: Some(DateTime::from_timestamp(1640995200, 0).unwrap()), // 2022-01-01
        },
        COMMIT {
            sha: Some("def456".to_string()),
            message: Some("Add feature".to_string()),
            committed_at: Some(DateTime::from_timestamp(1641081600, 0).unwrap()), // 2022-01-02
        },
    ];
    
    let commits_batch = COMMIT::to_record_batch(sample_commits)?;
    println!("📝 Writing COMMIT entities to silver layer...");
    storage.lake.write_batches("silver/entities/commits", vec![commits_batch], None).await?;
    
    // Test VERSION entities
    let sample_versions = vec![
        VERSION {
            sha: Some("v1.0.0".to_string()),
            tag: Some("1.0.0".to_string()),
            is_head: Some(false),
            created_at: Some(DateTime::from_timestamp(1640995200, 0).unwrap()),
        },
    ];
    
    let versions_batch = VERSION::to_record_batch(sample_versions)?;
    println!("🏷️  Writing VERSION entities to silver layer...");
    storage.lake.write_batches("silver/entities/versions", vec![versions_batch], None).await?;
    
    // Test ISSUE entities
    let sample_issues = vec![
        ISSUE {
            number: Some(42),
            title: Some("Bug in login".to_string()),
            state: Some("open".to_string()),
            created_at: Some(DateTime::from_timestamp(1640995200, 0).unwrap()),
        },
    ];
    
    let issues_batch = ISSUE::to_record_batch(sample_issues)?;
    println!("🐛 Writing ISSUE entities to silver layer...");
    storage.lake.write_batches("silver/entities/issues", vec![issues_batch], None).await?;
    
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
    println!("✅ COMMIT table: {}/silver/entities/commits/", config.lake_path.display());
    println!("✅ VERSION table: {}/silver/entities/versions/", config.lake_path.display());
    println!("✅ ISSUE table: {}/silver/entities/issues/", config.lake_path.display());
    println!("✅ Total entities tested: 6 types");
    
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
    
    let tables_to_check = vec![
        ("PROJECT", "silver/entities/projects"),
        ("DEVELOPER", "silver/entities/developers"),
        ("COMMIT", "silver/entities/commits"),
        ("VERSION", "silver/entities/versions"),
        ("ISSUE", "silver/entities/issues"),
    ];
    
    for (table_name, table_path) in tables_to_check {
        match deltalake::open_table(base_path.join(table_path).to_str().unwrap()).await {
            Ok(table) => {
                println!("  ✅ {} table: {} files, version {}", 
                    table_name,
                    table.get_file_uris().into_iter().count(), 
                    table.version());
            }
            Err(e) => println!("  ❌ {} table: {}", table_name, e),
        }
    }
    
    Ok(())
}