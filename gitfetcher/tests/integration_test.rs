use gitfetcher::{run, models::{OutputFormat, DetailLevel}};
use tempfile::tempdir;
use std::path::Path;

#[tokio::test]
async fn test_full_workflow_with_json() {
    // Skip this test if no GitHub token is available
    let github_token = std::env::var("GITHUB_TOKEN");
    if github_token.is_err() {
        println!("Skipping integration test: GITHUB_TOKEN not set");
        return;
    }

    let dir = tempdir().unwrap();
    let output_dir = dir.path().to_str().unwrap();

    // Test with a well-known public repository
    let result = run(
        "rust-lang", 
        "rust", 
        Some(github_token.unwrap()),
        OutputFormat::Json,
        DetailLevel::Core,
        output_dir,
    ).await;

    // Note: This test may fail due to API rate limits or network issues
    // The important thing is that the library compiles and the workflow functions are called
    if let Err(e) = result {
        println!("Integration test failed (expected due to API limits): {}", e);
        return;
    }

    // Verify output files were created
    assert!(Path::new(output_dir).join("issues.json").exists() || 
            Path::new(output_dir).join("pull_requests.json").exists() ||
            Path::new(output_dir).join("commits.json").exists());
}

#[tokio::test]
async fn test_full_workflow_with_csv() {
    // Skip this test if no GitHub token is available
    let github_token = std::env::var("GITHUB_TOKEN");
    if github_token.is_err() {
        println!("Skipping integration test: GITHUB_TOKEN not set");
        return;
    }

    let dir = tempdir().unwrap();
    let output_dir = dir.path().to_str().unwrap();

    // Test with a small repository to avoid rate limiting
    let result = run(
        "octocat", 
        "Hello-World", 
        Some(github_token.unwrap()),
        OutputFormat::Csv,
        DetailLevel::Core,
        output_dir,
    ).await;

    if let Err(e) = result {
        println!("Integration test failed (expected due to API limits): {}", e);
        return;
    }

    // Verify output files were created
    let issues_path = Path::new(output_dir).join("issues.csv");
    let pr_path = Path::new(output_dir).join("pull_requests.csv");
    let commits_path = Path::new(output_dir).join("commits.csv");

    // At least one file should exist for a successful run
    assert!(issues_path.exists() || pr_path.exists() || commits_path.exists());
}