use crate::errors::Result;
use async_trait::async_trait;
use deltalake::arrow::record_batch::RecordBatch;

/// A trait for types that can be fetched from an external source and converted to a RecordBatch.
/// This trait is the bridge between schema-defined structs and the data lake.
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

// A helper trait to allow calling to_record_batch on a type-erased object.
pub trait AnyFetchable: Send {
    fn to_record_batch_any(&self) -> Result<RecordBatch>;
    fn entity_type_any(&self) -> &'static str;
}

// Implement this trait for any vector of items that are Fetchable.
impl<T: Fetchable + 'static> AnyFetchable for Vec<T> {
    fn to_record_batch_any(&self) -> Result<RecordBatch> {
        T::to_record_batch(self.iter().cloned())
    }
    fn entity_type_any(&self) -> &'static str {
        T::ENTITY_TYPE
    }
}

/// A container for a heterogeneous collection of Fetchable items (nodes and edges).
#[derive(Default)]
pub struct GraphData {
    // The Box holds a Vec<T> where T: Fetchable
    data: Vec<Box<dyn AnyFetchable>>,
}

impl GraphData {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a vector of a specific Fetchable type to the container.
    pub fn add_entities<T: Fetchable + 'static>(&mut self, entities: Vec<T>) {
        if !entities.is_empty() {
            self.data.push(Box::new(entities));
        }
    }

    /// Consumes the container and returns the inner collection of fetchable data.
    pub fn into_inner(self) -> Vec<Box<dyn AnyFetchable>> {
        self.data
    }
}

/// An enum representing the varied data types a fetcher can return.
pub enum FetchResponse {
    /// Contains multiple types of nodes and edges for updating the knowledge graph.
    GraphData(GraphData),

    /// Text content specifically for vectorization and indexing.
    TextForVectorization {
        node_uri: String, // The URI of the graph node this text is associated with.
        text: String,
        metadata: serde_json::Value,
    },

    /// Tabular data intended for panel data analysis.
    PanelData {
        table_name: String,
        batch: RecordBatch, // Directly use Arrow RecordBatch for universal compatibility.
    },
}

/// The evolved Fetcher trait, capable of returning multiple data types from a single operation.
#[async_trait]
pub trait Fetcher: Send + Sync {
    /// Returns a unique name for the fetcher, used for registration and identification.
    fn name(&self) -> &'static str;

    /// Fetches data based on flexible input parameters and returns a unified FetchResponse.
    async fn fetch(&self, params: serde_json::Value) -> Result<FetchResponse>;
}
