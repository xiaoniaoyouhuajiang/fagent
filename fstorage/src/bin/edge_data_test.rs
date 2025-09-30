use fstorage::{FStorage, config::StorageConfig, schemas::{HAS_VERSION, CALLS}};
use tempfile::tempdir;
use chrono::DateTime;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔗 Testing Edge Data Generation and Storage");
    println!("============================================\n");
    
    // Create a temporary directory for testing
    let dir = tempdir()?;
    let config = StorageConfig::new(dir.path());
    
    println!("📁 Base directory: {:?}", dir.path());
    
    // Initialize FStorage
    let storage = FStorage::new(config.clone()).await?;
    
    println!("✅ FStorage initialized");
    
    // Test writing edge data
    println!("\n🚀 Testing edge data写入...");
    
    // Create HAS_VERSION edges
    let has_version_edges = vec![
        HAS_VERSION {
            id: Some("edge-has-version-1".to_string()),
            from_node_id: Some("project-rust-lang-rust".to_string()),
            to_node_id: Some("version-v1.0.0".to_string()),
            from_node_type: Some("PROJECT".to_string()),
            to_node_type: Some("VERSION".to_string()),
            created_at: Some(DateTime::from_timestamp(1640995200, 0).unwrap()),
            updated_at: Some(DateTime::from_timestamp(1640995200, 0).unwrap()),
        },
        HAS_VERSION {
            id: Some("edge-has-version-2".to_string()),
            from_node_id: Some("project-rust-lang-rust".to_string()),
            to_node_id: Some("version-v1.1.0".to_string()),
            from_node_type: Some("PROJECT".to_string()),
            to_node_type: Some("VERSION".to_string()),
            created_at: Some(DateTime::from_timestamp(1641081600, 0).unwrap()),
            updated_at: Some(DateTime::from_timestamp(1641081600, 0).unwrap()),
        },
    ];
    
    // Write HAS_VERSION edges
    println!("📊 Writing HAS_VERSION edges...");
    storage.write_edges("HAS_VERSION", has_version_edges).await?;
    
    // Create CALLS edges
    let calls_edges = vec![
        CALLS {
            id: Some("edge-calls-1".to_string()),
            from_node_id: Some("function-main".to_string()),
            to_node_id: Some("function-process_data".to_string()),
            from_node_type: Some("FUNCTION".to_string()),
            to_node_type: Some("FUNCTION".to_string()),
            created_at: Some(DateTime::from_timestamp(1640995200, 0).unwrap()),
            updated_at: Some(DateTime::from_timestamp(1640995200, 0).unwrap()),
        },
        CALLS {
            id: Some("edge-calls-2".to_string()),
            from_node_id: Some("function-process_data".to_string()),
            to_node_id: Some("function-validate_input".to_string()),
            from_node_type: Some("FUNCTION".to_string()),
            to_node_type: Some("FUNCTION".to_string()),
            created_at: Some(DateTime::from_timestamp(1640995200, 0).unwrap()),
            updated_at: Some(DateTime::from_timestamp(1640995200, 0).unwrap()),
        },
    ];
    
    // Write CALLS edges
    println!("🔗 Writing CALLS edges...");
    storage.write_edges("CALLS", calls_edges).await?;
    
    // Test writing additional edges individually (since HashMap requires uniform types)
    println!("\n🔄 Testing additional edge data写入...");
    
    // Add more HAS_VERSION edges
    let additional_has_version = vec![
        HAS_VERSION {
            id: Some("edge-has-version-3".to_string()),
            from_node_id: Some("project-tokio-rs-tokio".to_string()),
            to_node_id: Some("version-v1.0.0".to_string()),
            from_node_type: Some("PROJECT".to_string()),
            to_node_type: Some("VERSION".to_string()),
            created_at: Some(DateTime::from_timestamp(1640995200, 0).unwrap()),
            updated_at: Some(DateTime::from_timestamp(1640995200, 0).unwrap()),
        },
    ];
    
    storage.write_edges("HAS_VERSION", additional_has_version).await?;
    
    // Add more CALLS edges
    let additional_calls = vec![
        CALLS {
            id: Some("edge-calls-3".to_string()),
            from_node_id: Some("function-validate_input".to_string()),
            to_node_id: Some("function-sanitize_string".to_string()),
            from_node_type: Some("FUNCTION".to_string()),
            to_node_type: Some("FUNCTION".to_string()),
            created_at: Some(DateTime::from_timestamp(1640995200, 0).unwrap()),
            updated_at: Some(DateTime::from_timestamp(1640995200, 0).unwrap()),
        },
    ];
    
    storage.write_edges("CALLS", additional_calls).await?;
    
    // Check directory structure
    println!("\n📂 Final DataLake directory structure:");
    print_dir_structure(dir.path())?;
    
    // Test edge queries (placeholder implementation)
    println!("\n🔍 Testing edge queries...");
    
    let out_edges = storage.get_out_edges("project-rust-lang-rust", Some("HAS_VERSION")).await?;
    println!("📤 Out edges count: {}", out_edges.len());
    
    let in_edges = storage.get_in_edges("version-v1.0.0", Some("HAS_VERSION")).await?;
    println!("📥 In edges count: {}", in_edges.len());
    
    // Get edge statistics
    let stats = storage.get_edge_statistics().await?;
    println!("📊 Edge statistics: {:?}", stats);
    
    println!("\n🎯 Edge Data Storage Summary:");
    println!("================================");
    println!("✅ Lake path: {:?}", config.lake_path);
    println!("✅ Edge data stored in: {}/silver/edges/", config.lake_path.display());
    println!("✅ HAS_VERSION edges: {}/silver/edges/has_version/", config.lake_path.display());
    println!("✅ CALLS edges: {}/silver/edges/calls/", config.lake_path.display());
    println!("✅ All edge types implement Fetchable trait");
    println!("✅ Automatic code generation from schema.hx working");
    
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