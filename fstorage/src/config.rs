use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize, Debug, Clone)]
pub struct StorageConfig {
    pub lake_path: PathBuf,
    pub catalog_path: PathBuf,
    pub engine_path: PathBuf,
}

impl StorageConfig {
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        let base_path = base_path.into();
        Self {
            lake_path: base_path.join("lake"),
            catalog_path: base_path.join("catalog.sqlite"),
            engine_path: base_path.join("engine"),
        }
    }
}
