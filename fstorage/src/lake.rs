use crate::config::StorageConfig;
use crate::errors::Result;
use tokio::io::AsyncWriteExt;

pub struct Lake {
    config: StorageConfig,
}

impl Lake {
    pub async fn new(config: StorageConfig) -> Result<Self> {
        // Ensure the base directories exist
        tokio::fs::create_dir_all(&config.lake_path.join("bronze")).await?;
        tokio::fs::create_dir_all(&config.lake_path.join("silver")).await?;
        tokio::fs::create_dir_all(&config.lake_path.join("gold")).await?;
        tokio::fs::create_dir_all(&config.lake_path.join("dicts")).await?;
        Ok(Self { config })
    }

    /// Writes data to a specified path within the data lake.
    pub async fn write_data(&self, path: &str, data: &serde_json::Value) -> Result<()> {
        let full_path = self.config.lake_path.join(path);
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut file = tokio::fs::File::create(full_path).await?;
        let bytes = serde_json::to_vec_pretty(data)?;
        file.write_all(&bytes).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_write_data() {
        let dir = tempdir().unwrap();
        let config = StorageConfig::new(dir.path());
        let lake = Lake::new(config.clone()).await.unwrap();

        let data = json!({"id": 1, "name": "test"});
        let path = "silver/entities/test.json";
        let result = lake.write_data(path, &data).await;
        assert!(result.is_ok());

        let full_path = config.lake_path.join(path);
        assert!(full_path.exists());

        let content = tokio::fs::read_to_string(full_path).await.unwrap();
        let read_data: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(data, read_data);
    }
}
