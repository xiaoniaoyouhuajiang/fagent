use crate::errors::Result;
use async_trait::async_trait;
use deltalake::arrow::record_batch::RecordBatch;

/// A trait for types that can be fetched from an external source and converted to a RecordBatch.
pub trait Fetchable: Send + Sync + Clone + 'static {
    /// A unique identifier for the type of entity (e.g., "repo", "issue").
    const ENTITY_TYPE: &'static str;

    /// The name of the delta table where this entity is stored.
    fn table_name() -> String {
        format!("entities/{}", Self::ENTITY_TYPE)
    }
    
    /// The primary key(s) for this entity, used for merging.
    fn primary_keys() -> Vec<&'static str>;
    
    /// Convert a collection of this type into a RecordBatch for Delta Lake storage.
    fn to_record_batch(data: impl IntoIterator<Item = Self>) -> Result<RecordBatch>;
}

/// A generic trait for a fetcher that can retrieve a specific type `T`.
#[async_trait]
pub trait Fetcher<T: Fetchable>: Send + Sync {
    async fn fetch(&self, identifier: &str) -> Result<Vec<T>>;
}
