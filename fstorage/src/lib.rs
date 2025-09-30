pub mod auto_fetchable;
pub mod catalog;
pub mod config;
pub mod errors;
pub mod fetch;
pub mod lake;
pub mod models;
pub mod schemas;
pub mod sync;

use crate::catalog::Catalog;
use crate::config::StorageConfig;
use crate::errors::Result;
use crate::lake::Lake;
use crate::sync::FStorageSynchronizer;
use helix_db::helix_engine::traversal_core::{HelixGraphEngine, HelixGraphEngineOpts};
use std::sync::Arc;

/// The main entry point for the `fstorage` library.
///
/// `FStorage` acts as the primary interface for the data storage layer of the AI agent.
/// It encapsulates all the necessary components for managing a local data lake,
/// including:
/// - A multi-layered data lake (`Lake`) built with Delta Lake for versioned, reliable storage.
/// - A metadata database (`Catalog`) using SQLite to track data readiness, API budgets, and task logs.
/// - A graph database instance (`HelixGraphEngine`) for high-performance querying of indexed data.
/// - A dynamic data synchronization mechanism (`DataSynchronizer`) to keep the local data fresh.
///
/// # Example
///
/// ```rust,no_run
/// use fstorage::{FStorage, config::StorageConfig};
/// use tempfile::tempdir;
///
/// #[tokio::main]
/// async fn main() {
///     let dir = tempdir().unwrap();
///     let config = StorageConfig::new(dir.path());
///     let storage = FStorage::new(config).await.unwrap();
///
///     // Now you can use storage.synchronizer, storage.engine, etc.
/// }
/// ```
pub struct FStorage {
    pub config: StorageConfig,
    pub catalog: Arc<Catalog>,
    pub lake: Arc<Lake>,
    pub engine: Arc<HelixGraphEngine>,
    pub synchronizer: Arc<FStorageSynchronizer>,
}

impl FStorage {
    /// Creates a new instance of FStorage and initializes it.
    ///
    /// This will:
    /// 1. Create the necessary directories for the data lake and graph engine.
    /// 2. Open a connection to the SQLite catalog and initialize its schema.
    /// 3. Initialize the HelixGraphEngine.
    pub async fn new(config: StorageConfig) -> Result<Self> {
        // Ensure engine directory exists
        tokio::fs::create_dir_all(&config.engine_path).await?;

        let catalog = Arc::new(Catalog::new(&config)?);
        catalog.initialize_schema()?;

        let lake = Arc::new(Lake::new(config.clone()).await?);

        let engine_opts = HelixGraphEngineOpts {
            path: config.engine_path.to_str().unwrap().to_string(),
            ..Default::default()
        };
        let engine = Arc::new(HelixGraphEngine::new(engine_opts)?);

        let synchronizer = Arc::new(FStorageSynchronizer::new(
            Arc::clone(&catalog),
            Arc::clone(&lake),
            Arc::clone(&engine),
        ));

        Ok(Self {
            config,
            catalog,
            lake,
            engine,
            synchronizer,
        })
    }

    /// 写入边数据到数据湖
    /// 
    /// # 参数
    /// * `edge_type` - 边类型名称（如 "HAS_VERSION", "CALLS" 等）
    /// * `edges` - 边数据向量，必须实现 Fetchable trait
    /// 
    /// # 返回
    /// * `Result<()>` - 操作结果
    /// 
    /// # 示例
    /// ```rust,no_run
    /// use fstorage::{FStorage, schemas::HAS_VERSION};
    /// use chrono::{DateTime, Utc};
    /// 
    /// let edges = vec![
    ///     HAS_VERSION {
    ///         id: Some("edge-1".to_string()),
    ///         from_node_id: Some("project-1".to_string()),
    ///         to_node_id: Some("version-1".to_string()),
    ///         from_node_type: Some("PROJECT".to_string()),
    ///         to_node_type: Some("VERSION".to_string()),
    ///         created_at: Some(Utc::now()),
    ///         updated_at: Some(Utc::now()),
    ///     },
    /// ];
    /// 
    /// storage.write_edges("HAS_VERSION", edges).await?;
    /// ```
    pub async fn write_edges<T: crate::fetch::Fetchable>(
        &self,
        edge_type: &str,
        edges: Vec<T>,
    ) -> Result<()> {
        self.lake.write_edges(edge_type, edges).await
    }

    /// 批量写入多种类型的边数据
    /// 
    /// # 参数
    /// * `edge_batches` - 边类型到边数据向量的映射
    /// 
    /// # 返回
    /// * `Result<()>` - 操作结果
    pub async fn write_multiple_edges<T: crate::fetch::Fetchable>(
        &self,
        edge_batches: std::collections::HashMap<&str, Vec<T>>,
    ) -> Result<()> {
        for (edge_type, edges) in edge_batches {
            self.lake.write_edges(edge_type, edges).await?;
        }
        Ok(())
    }

    /// 查询节点的出边
    /// 
    /// # 参数
    /// * `node_id` - 节点ID
    /// * `edge_type` - 可选的边类型过滤器
    /// 
    /// # 返回
    /// * `Result<Vec<std::collections::HashMap<String, serde_json::Value>>>` - 边数据列表
    pub async fn get_out_edges(
        &self,
        node_id: &str,
        edge_type: Option<&str>,
    ) -> Result<Vec<std::collections::HashMap<String, serde_json::Value>>> {
        self.lake.get_out_edges(node_id, edge_type).await
    }

    /// 查询节点的入边
    /// 
    /// # 参数
    /// * `node_id` - 节点ID
    /// * `edge_type` - 可选的边类型过滤器
    /// 
    /// # 返回
    /// * `Result<Vec<std::collections::HashMap<String, serde_json::Value>>>` - 边数据列表
    pub async fn get_in_edges(
        &self,
        node_id: &str,
        edge_type: Option<&str>,
    ) -> Result<Vec<std::collections::HashMap<String, serde_json::Value>>> {
        self.lake.get_in_edges(node_id, edge_type).await
    }

    /// 获取边数据统计信息
    /// 
    /// # 返回
    /// * `Result<std::collections::HashMap<String, i64>>` - 边类型到数量的映射
    pub async fn get_edge_statistics(&self) -> Result<std::collections::HashMap<String, i64>> {
        self.lake.get_edge_statistics().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_fstorage_initialization() {
        let dir = tempdir().unwrap();
        let config = StorageConfig::new(dir.path());

        let storage = FStorage::new(config.clone()).await;
        assert!(storage.is_ok());

        // Check if files and directories were created
        assert!(config.lake_path.exists());
        assert!(config.lake_path.join("bronze").exists());
        assert!(config.lake_path.join("silver/entities").exists());
        assert!(config.catalog_path.exists());
        assert!(config.engine_path.exists());
    }
}
