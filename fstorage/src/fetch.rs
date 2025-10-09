use crate::errors::Result;
use async_trait::async_trait;
use deltalake::arrow::record_batch::RecordBatch;

/// The category of an entity in the knowledge graph.
pub enum EntityCategory {
    Node,
    Edge,
}

/// A trait for types that can be fetched from an external source and converted to a RecordBatch.
pub trait Fetchable: Send + Sync + Clone + 'static {
    const ENTITY_TYPE: &'static str;
    fn table_name() -> String {
        format!("entities/{}", Self::ENTITY_TYPE)
    }
    fn primary_keys() -> Vec<&'static str>;
    /// Specifies the category of the entity in the graph.
    fn category() -> EntityCategory;
    fn to_record_batch(data: impl IntoIterator<Item = Self>) -> Result<RecordBatch>;
}

// A helper trait for type erasure.
pub trait AnyFetchable: Send {
    fn to_record_batch_any(&self) -> Result<RecordBatch>;
    fn entity_type_any(&self) -> &'static str;
    fn category_any(&self) -> EntityCategory;
}

impl<T: Fetchable + 'static> AnyFetchable for Vec<T> {
    fn to_record_batch_any(&self) -> Result<RecordBatch> {
        T::to_record_batch(self.iter().cloned())
    }
    fn entity_type_any(&self) -> &'static str {
        T::ENTITY_TYPE
    }
    fn category_any(&self) -> EntityCategory {
        T::category()
    }
}

/// A task to vectorize a piece of text and associate it with a graph node.
pub struct TextToVectorize {
    pub node_uri: String, // The ID of the node this text belongs to.
    pub text: String,
    pub metadata: serde_json::Value,
}

/// A unified container for all data related to a graph update.
#[derive(Default)]
pub struct GraphData {
    pub entities: Vec<Box<dyn AnyFetchable>>,
}

impl GraphData {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a vector of a specific Fetchable type (nodes or edges) to the container.
    pub fn add_entities<T: Fetchable + 'static>(&mut self, entities: Vec<T>) {
        if !entities.is_empty() {
            self.entities.push(Box::new(entities));
        }
    }
}

/// An enum representing the data types a fetcher can return.
pub enum FetchResponse {
    /// All data required to update the knowledge graph.
    GraphData(GraphData),
    /// Tabular data for offline analysis.
    PanelData {
        table_name: String,
        batch: RecordBatch,
    },
}

use crate::embedding::EmbeddingProvider;
use std::sync::Arc;

/// The evolved Fetcher trait, capable of returning a unified graph update package.
#[async_trait]
pub trait Fetcher: Send + Sync {
    fn name(&self) -> &'static str;
    async fn fetch(&self, params: serde_json::Value, embedding_provider: Arc<dyn EmbeddingProvider>) -> Result<FetchResponse>;
}
