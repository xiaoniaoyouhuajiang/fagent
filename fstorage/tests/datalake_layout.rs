use deltalake::open_table;
use fstorage::{
    fetch::Fetchable,
    schemas::generated_schemas::{Commit, Developer, Issue, Project, Version},
};
use url::Url;

mod common;

#[tokio::test]
async fn writes_entities_into_expected_delta_tables() -> anyhow::Result<()> {
    let ctx = common::init_test_context().await?;

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
    let projects_batch = Project::to_record_batch(sample_projects)?;
    ctx.lake
        .write_batches(&Project::table_name(), vec![projects_batch], None)
        .await?;

    let sample_developers = vec![
        Developer {
            name: Some("user1".to_string()),
            followers: Some(1_000),
            location: Some("Location A".to_string()),
            email: Some("user1@example.com".to_string()),
        },
        Developer {
            name: Some("user2".to_string()),
            followers: Some(2_000),
            location: Some("Location B".to_string()),
            email: Some("user2@example.com".to_string()),
        },
    ];
    let developers_batch = Developer::to_record_batch(sample_developers)?;
    ctx.lake
        .write_batches(&Developer::table_name(), vec![developers_batch], None)
        .await?;

    let sample_commits = vec![
        Commit {
            sha: Some("a1b2c3d4".to_string()),
            message: Some("Initial commit".to_string()),
            committed_at: Some(chrono::Utc::now()),
        },
        Commit {
            sha: Some("e5f6g7h8".to_string()),
            message: Some("Add feature X".to_string()),
            committed_at: Some(chrono::Utc::now()),
        },
    ];
    let commits_batch = Commit::to_record_batch(sample_commits)?;
    ctx.lake
        .write_batches(&Commit::table_name(), vec![commits_batch], None)
        .await?;

    let versions_batch = Version::to_record_batch(vec![Version {
        sha: Some("a1b2c3d4".to_string()),
        tag: Some("v1.0.0".to_string()),
        is_head: Some(false),
        created_at: Some(chrono::Utc::now()),
    }])?;
    ctx.lake
        .write_batches(&Version::table_name(), vec![versions_batch], None)
        .await?;

    let issues_batch = Issue::to_record_batch(vec![Issue {
        number: Some(1),
        title: Some("Bug in feature Y".to_string()),
        state: Some("open".to_string()),
        created_at: Some(chrono::Utc::now()),
    }])?;
    ctx.lake
        .write_batches(&Issue::table_name(), vec![issues_batch], None)
        .await?;

    let expected_tables = vec![
        ("PROJECT", Project::table_name()),
        ("DEVELOPER", Developer::table_name()),
        ("COMMIT", Commit::table_name()),
        ("VERSION", Version::table_name()),
        ("ISSUE", Issue::table_name()),
    ];

    for (label, table_rel_path) in expected_tables {
        let table_path = ctx.config.lake_path.join(&table_rel_path);
        assert!(
            table_path.exists(),
            "expected table directory {:?} to exist",
            table_path
        );
        assert!(
            table_path.join("_delta_log").exists(),
            "delta log missing for {:?}",
            table_path
        );

        let url =
            Url::from_file_path(&table_path).map_err(|_| anyhow::anyhow!("non-UTF8 table path"))?;
        let table = open_table(url).await?;
        assert_eq!(
            table.version(),
            Some(0),
            "table {} should have exactly one commit",
            label
        );
        assert!(
            table.get_file_uris()?.count() > 0,
            "table {} should contain data files",
            label
        );
    }

    Ok(())
}
