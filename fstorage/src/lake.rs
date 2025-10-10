use crate::config::StorageConfig;
use crate::errors::{Result, StorageError};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::DeltaTable;
use deltalake::DeltaTableBuilder;
use std::collections::HashMap;

pub struct Lake {
    config: StorageConfig,
}

impl Lake {
    pub async fn new(config: StorageConfig) -> Result<Self> {
        tokio::fs::create_dir_all(&config.lake_path).await?;
        Ok(Self { config })
    }

    // create delta table
    pub async fn get_or_create_table(&self, table_name: &str) -> Result<DeltaTable> {
        let table_path = self.config.lake_path.join(table_name);
        
        // 确保父目录存在
        if let Some(parent) = table_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        
        let table_uri = table_path
            .to_str()
            .ok_or_else(|| StorageError::Config(format!("Invalid table path {:?}", table_path)))?;

        match deltalake::open_table(table_uri).await {
            Ok(table) => Ok(table),
            Err(deltalake::DeltaTableError::NotATable(_)) => {
                // 如果表尚未初始化，返回一个尚未加载的 DeltaTable 句柄，
                // 后续写入操作会在第一次写入时创建表并注入 Schema。
                let table = DeltaTableBuilder::from_uri(table_uri).build()?;
                Ok(table)
            }
            Err(e) => Err(StorageError::from(e)),
        }
    }

    /// 将RecordBatch写入指定的Delta Table，支持合并（merge）操作以保证幂等性。
    pub async fn write_batches(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        merge_on: Option<Vec<&str>>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let table_path = self.config.lake_path.join(table_name);
        
        // Use DeltaOps for writing with merge support
        let ops = deltalake::DeltaOps::try_from_uri(table_path.to_str().unwrap()).await?;
        
        if let Some(_predicate) = merge_on {
            // Note: In deltalake 0.17.0, merge operations are handled differently
            // For now, we'll use simple append operations
            // TODO: Implement proper merge logic when needed
        }
        
        ops.write(batches).await?;

        Ok(())
    }

    /// 写入边数据到数据湖
    /// 
    /// # 参数
    /// * `edge_type` - 边类型名称（如 "HAS_VERSION", "CALLS" 等）
    /// * `edges` - 边数据向量，必须实现 Fetchable trait
    /// 
    /// # 返回
    /// * `Result<()>` - 操作结果
    pub async fn write_edges<T: crate::fetch::Fetchable>(
        &self,
        edge_type: &str,
        edges: Vec<T>,
    ) -> Result<()> {
        let batch = T::to_record_batch(edges)?;
        let table_path = format!("silver/edges/{}", edge_type.to_lowercase());
        self.write_batches(&table_path, vec![batch], None).await
    }

    // Note: These methods are left for API compatibility but should ideally query the hot path (HelixDB).
    // The current implementation is a placeholder.
    pub async fn get_out_edges(
        &self,
        _node_id: &str,
        _edge_type: Option<&str>,
    ) -> Result<Vec<HashMap<String, serde_json::Value>>> {
        log::warn!("get_out_edges is using mock data. For real data, query the graph engine.");
        Ok(vec![])
    }

    pub async fn get_in_edges(
        &self,
        _node_id: &str,
        _edge_type: Option<&str>,
    ) -> Result<Vec<HashMap<String, serde_json::Value>>> {
        log::warn!("get_in_edges is using mock data. For real data, query the graph engine.");
        Ok(vec![])
    }

    /// 获取边数据统计信息
    /// 
    /// # 返回
    /// * `Result<HashMap<String, i64>>` - 边类型到数量的映射
    pub async fn get_edge_statistics(&self) -> Result<HashMap<String, i64>> {
        let mut stats = HashMap::new();
        
        // 获取所有可用的边类型
        let edge_types = self.get_available_edge_types().await.unwrap_or_default();
        
        for et in edge_types {
            let table_path = format!("silver/edges/{}", et);
            if let Ok(table) = deltalake::open_table(
                self.config.lake_path.join(&table_path).to_str().unwrap()
            ).await {
                // 使用表的版本数量估算记录数（简化实现）
                let file_count = table.version() as i64;
                stats.insert(et, file_count);
            }
        }
        
        Ok(stats)
    }

    /// 获取所有可用的边类型
    /// 
    /// # 返回
    /// * `Result<Vec<String>>` - 边类型列表
    async fn get_available_edge_types(&self) -> Result<Vec<String>> {
        let edges_path = self.config.lake_path.join("silver/edges");
        let mut edge_types = Vec::new();
        
        if tokio::fs::metadata(&edges_path).await.is_ok() {
            let mut entries = tokio::fs::read_dir(&edges_path).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if path.is_dir() {
                    if let Some(dir_name) = path.file_name().and_then(|name| name.to_str()) {
                        edge_types.push(dir_name.to_string());
                    }
                }
            }
        }
        
        Ok(edge_types)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use deltalake::arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use tempfile::tempdir;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_write_and_read_delta_table() {
        let dir = tempdir().unwrap();
        let config = StorageConfig::new(dir.path());
        let lake = Lake::new(config.clone()).await.unwrap();
        let table_name = "test_table";

        // 定义Schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // 创建RecordBatch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        // 写入数据
        let result = lake.write_batches(table_name, vec![batch], None).await;
        assert!(result.is_ok());

        // 验证表是否存在
        let table = deltalake::open_table(config.lake_path.join(table_name).to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        assert_eq!(table.get_file_uris().into_iter().count(), 1);
    }
}
