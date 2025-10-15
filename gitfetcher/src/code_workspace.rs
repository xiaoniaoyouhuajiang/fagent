use std::path::{Path, PathBuf};

use ast::repo::{Repo, Repos};
use fstorage::errors::{Result as StorageResult, StorageError};
use git2::{
    build::{CheckoutBuilder, RepoBuilder},
    FetchOptions, Repository,
};
use tempfile::TempDir;
use tokio::task;

const DEFAULT_REPO_DIR: &str = "repo";

/// Configuration for preparing a local checkout that AST can consume.
pub struct WorkspaceConfig<'a> {
    /// Remote URL or local path that `git` understands.
    pub repo_url: &'a str,
    /// Human readable identifier (e.g. `owner/repo`) used in metadata.
    pub display_name: &'a str,
    /// Commit SHA or reference to checkout. If empty, HEAD will be used.
    pub revision: &'a str,
}

pub struct CodeWorkspace {
    _temp_dir: TempDir,
    repo_root: PathBuf,
    repos: Repos,
    revision: String,
    display_name: String,
}

impl CodeWorkspace {
    pub fn repo_root(&self) -> &Path {
        &self.repo_root
    }

    pub fn repositories(&self) -> &[Repo] {
        &self.repos.0
    }

    pub fn repositories_mut(&mut self) -> &mut [Repo] {
        &mut self.repos.0
    }

    pub fn revision(&self) -> &str {
        &self.revision
    }

    pub fn display_name(&self) -> &str {
        &self.display_name
    }
}

pub async fn prepare_workspace(config: WorkspaceConfig<'_>) -> StorageResult<CodeWorkspace> {
    let temp_dir = TempDir::new().map_err(|err| {
        StorageError::SyncError(format!("failed to create temporary directory: {err}"))
    })?;
    let checkout_path = temp_dir.path().join(DEFAULT_REPO_DIR);

    let repo_url = config.repo_url.to_string();
    let revision = config.revision.to_string();
    let repo_url_for_clone = repo_url.clone();
    let revision_for_clone = revision.clone();
    let checkout_path_clone = checkout_path.clone();

    task::spawn_blocking(move || {
        clone_and_checkout(&repo_url_for_clone, &checkout_path_clone, &revision_for_clone)
    })
        .await
        .map_err(|err| StorageError::SyncError(format!("checkout task failed: {err}")))??;

    let checkout_str = checkout_path
        .to_str()
        .ok_or_else(|| {
            StorageError::SyncError(format!(
                "non-UTF8 checkout path: {}",
                checkout_path.display()
            ))
        })?
        .to_string();

    let _guard_lsp = EnvVarGuard::set("USE_LSP", Some("0"));
    let _guard_skip_post = EnvVarGuard::set("LSP_SKIP_POST_CLONE", Some("1"));
    let _guard_repo_path = EnvVarGuard::set("REPO_PATH", Some(&checkout_str));

    let repo_origin = make_origin_url(config.display_name, &repo_url);
    let repos = ast::repo::Repo::new_multi_detect(
        &checkout_str,
        Some(repo_origin),
        Vec::new(),
        if revision.is_empty() {
            Vec::new()
        } else {
            vec![revision.clone()]
        },
        Some(false),
    )
    .await
    .map_err(|err| StorageError::SyncError(format!("AST language detection failed: {err}")))?;

    Ok(CodeWorkspace {
        _temp_dir: temp_dir,
        repo_root: checkout_path,
        repos,
        revision,
        display_name: config.display_name.to_string(),
    })
}

fn clone_and_checkout(repo_url: &str, dest: &Path, revision: &str) -> Result<(), StorageError> {
    let parent = dest
        .parent()
        .ok_or_else(|| StorageError::SyncError("invalid checkout destination".into()))?;
    std::fs::create_dir_all(parent).map_err(|err| {
        StorageError::SyncError(format!("failed to create checkout parent directory: {err}"))
    })?;

    if dest.exists() {
        std::fs::remove_dir_all(dest).map_err(|err| {
            StorageError::SyncError(format!(
                "failed to clean existing checkout at {}: {err}",
                dest.display()
            ))
        })?;
    }

    let repo = RepoBuilder::new()
        .clone(repo_url, dest)
        .map_err(|err| StorageError::SyncError(format!("git clone failed: {err}")))?;

    if revision.is_empty() {
        return Ok(());
    }

    checkout_revision(&repo, revision)?;

    Ok(())
}

fn checkout_revision(repo: &Repository, revision: &str) -> Result<(), StorageError> {
    let commit_obj = match repo.revparse_single(&format!("{revision}^{{commit}}")) {
        Ok(obj) => obj,
        Err(_) => {
            let mut remote = repo.find_remote("origin").map_err(|err| {
                StorageError::SyncError(format!("failed to find remote 'origin': {err}"))
            })?;
            let mut fetch_options = FetchOptions::new();
            fetch_options.download_tags(git2::AutotagOption::All);
            remote
                .fetch(&[revision], Some(&mut fetch_options), None)
                .map_err(|err| StorageError::SyncError(format!("git fetch failed: {err}")))?;
            repo.revparse_single(&format!("{revision}^{{commit}}"))
                .map_err(|err| StorageError::SyncError(format!("unknown revision {revision}: {err}")))?
        }
    };

    let commit = commit_obj
        .peel_to_commit()
        .map_err(|err| StorageError::SyncError(format!("failed to peel commit: {err}")))?;

    let mut checkout = CheckoutBuilder::new();
    checkout.force();
    repo.checkout_tree(commit.as_object(), Some(&mut checkout))
        .map_err(|err| StorageError::SyncError(format!("checkout failed: {err}")))?;
    repo.set_head_detached(commit.id())
        .map_err(|err| StorageError::SyncError(format!("set HEAD failed: {err}")))?;
    Ok(())
}

fn make_origin_url(display_name: &str, fallback_url: &str) -> String {
    let mut candidate = if display_name.contains("://") || display_name.starts_with('/') {
        display_name.to_string()
    } else if fallback_url.contains("://") || fallback_url.starts_with('/') {
        fallback_url.to_string()
    } else {
        format!("https://github.com/{display_name}")
    };
    if let Some(stripped) = candidate.strip_suffix(".git") {
        candidate = stripped.to_string();
    }
    candidate
}

struct EnvVarGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: Option<&str>) -> Self {
        let previous = std::env::var_os(key);
        match value {
            Some(val) => std::env::set_var(key, val),
            None => std::env::remove_var(key),
        }
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use git2::Signature;
    use tokio::runtime::Runtime;

    #[test]
    fn prepare_workspace_clones_and_detects() {
        let rt = Runtime::new().expect("runtime");
        rt.block_on(async {
            let source_dir = TempDir::new().expect("source dir");
            let source_path = source_dir.path().join("source");
            std::fs::create_dir_all(&source_path).expect("create source");
            let repo = Repository::init(&source_path).expect("init repo");

            std::fs::create_dir_all(source_path.join("src")).expect("create src");
            std::fs::write(
                source_path.join("src").join("lib.rs"),
                "pub fn hello() -> u32 { 42 }",
            )
            .expect("write lib.rs");
            std::fs::write(source_path.join("Cargo.toml"), "[package]\nname=\"demo\"\nversion=\"0.1.0\"\nedition=\"2021\"").expect("write cargo");

            let mut index = repo.index().expect("index");
            index.add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None).expect("add all");
            index.write().expect("write index");
            let tree_id = index.write_tree().expect("tree");
            let tree = repo.find_tree(tree_id).expect("find tree");
            let sig = Signature::now("Tester", "tester@example.com").expect("sig");
            let oid = repo
                .commit(
                    Some("HEAD"),
                    &sig,
                    &sig,
                    "initial commit",
                    &tree,
                    &[],
                )
                .expect("commit");

            let repo_url = source_path
                .to_str()
                .expect("unicode path")
                .to_string();
            let revision = oid.to_string();

            let workspace = prepare_workspace(WorkspaceConfig {
                repo_url: &repo_url,
                display_name: "local/test",
                revision: &revision,
            })
            .await
            .expect("workspace");

            assert!(workspace.repo_root().join("Cargo.toml").exists());
            assert!(
                !workspace.repositories().is_empty(),
                "expected at least one detected language"
            );
            assert_eq!(workspace.revision(), revision);
        });
    }
}
