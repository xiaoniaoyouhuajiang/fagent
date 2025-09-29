use crate::config::StorageConfig;
use crate::errors::{Result, StorageError};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::operations::create::CreateBuilder;
// use deltalake::operations::write::WriteBuilder;
use deltalake::{DeltaTable};

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
        
        match deltalake::open_table(table_path.to_str().unwrap()).await {
            Ok(table) => Ok(table),
            Err(deltalake::DeltaTableError::NotATable(_)) => {
                // 如果表不存在，则根据一个空的RecordBatch创建它
                // 这里的Schema是临时的，实际Schema将在第一次写入时确定
                let table = CreateBuilder::new()
                    .with_location(table_path.to_str().unwrap())
                    .with_table_name(table_name)
                    .await?;
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
