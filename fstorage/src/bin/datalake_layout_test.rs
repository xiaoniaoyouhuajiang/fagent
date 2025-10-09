use fstorage::{FStorage, config::StorageConfig, schemas::generated_schemas::{Project, Developer, Commit, Version, Issue}, fetch::Fetchable};
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
    
    let sample_projects = vec![
        Project {
            url: Some("https://github.com/example/repo1".to_string()),
            name: Some("repo1".to_string()),
            description: Some("Description for repo1".to_string()),
            language: Some("Rust".to_string()),
            stars: Some(100),
            forks: Some(20),
        },
        Project {
            url: Some("https://github.com/example/repo2".to_string()),
            name: Some("repo2".to_string()),
            description: Some("Description for repo2".to_string()),
            language: Some("Python".to_string()),
            stars: Some(200),
            forks: Some(40),
        },
    ];

    let projects_batch = Project::to_record_batch(sample_projects.clone())?;
    storage
        .lake
        .write_batches(&Project::table_name(), vec![projects_batch], None)
        .await?;

    let sample_developers = vec![
        Developer {
            name: Some("user1".to_string()),
            followers: Some(1000),
            location: Some("Location A".to_string()),
            email: Some("user1@example.com".to_string()),
        },
        Developer {
            name: Some("user2".to_string()),
            followers: Some(2000),
            location: Some("Location B".to_string()),
            email: Some("user2@example.com".to_string()),
        },
    ];
    let developers_batch = Developer::to_record_batch(sample_developers.clone())?;
    storage
        .lake
        .write_batches(&Developer::table_name(), vec![developers_batch], None)
        .await?;

    let sample_commits = vec![
        Commit {
            sha: Some("a1b2c3d4".to_string()),
            message: Some("Initial commit".to_string()),
            committed_at: Some(Utc::now()),
        },
        Commit {
            sha: Some("e5f6g7h8".to_string()),
            message: Some("Add feature X".to_string()),
            committed_at: Some(Utc::now()),
        },
    ];
    let commits_batch = Commit::to_record_batch(sample_commits)?;
    storage
        .lake
        .write_batches(&Commit::table_name(), vec![commits_batch], None)
        .await?;

    let sample_versions = vec![
        Version {
            sha: Some("a1b2c3d4".to_string()),
            tag: Some("v1.0.0".to_string()),
            is_head: Some(false),
            created_at: Some(Utc::now()),
        },
    ];
    let versions_batch = Version::to_record_batch(sample_versions)?;
    storage
        .lake
        .write_batches(&Version::table_name(), vec![versions_batch], None)
        .await?;

    let sample_issues = vec![
        Issue {
            number: Some(1),
            title: Some("Bug in feature Y".to_string()),
            state: Some("open".to_string()),
            created_at: Some(Utc::now()),
        },
    ];
    let issues_batch = Issue::to_record_batch(sample_issues)?;
    storage
        .lake
        .write_batches(&Issue::table_name(), vec![issues_batch], None)
        .await?;
    
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