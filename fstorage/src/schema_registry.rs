use std::collections::HashMap;

use once_cell::sync::Lazy;

use crate::fetch::EntityCategory;
use crate::schemas::generated_schemas::{
    EdgeMetaRecord, EntityMetaRecord, StableIdStrategy, VectorEdgeRuleRecord,
    VectorKeyMappingRecord, VectorSourceRecord, VectorSourceTypeRecord, GENERATED_EDGE_METADATA,
    GENERATED_ENTITY_METADATA, GENERATED_VECTOR_EDGE_RULES,
};

#[derive(Debug, Clone)]
pub struct EntityMetadata {
    pub entity_type: &'static str,
    pub category: EntityCategory,
    pub table_name: &'static str,
    pub primary_keys: &'static [&'static str],
    pub fields: &'static [&'static str],
    pub stable_id: StableIdStrategy,
}

#[derive(Debug, Clone)]
pub struct EdgeMetadata {
    pub edge_type: &'static str,
    pub from_entity: &'static str,
    pub to_entity: &'static str,
}

#[derive(Debug)]
pub struct SchemaRegistry {
    entities: HashMap<&'static str, EntityMetadata>,
    edges: HashMap<&'static str, Vec<EdgeMetadata>>,
}

impl SchemaRegistry {
    fn from_generated() -> Self {
        let entities = GENERATED_ENTITY_METADATA
            .iter()
            .map(|record| (record.entity_type, EntityMetadata::from(record)))
            .collect();

        let mut edge_map: HashMap<&'static str, Vec<EdgeMetadata>> = HashMap::new();
        for record in GENERATED_EDGE_METADATA.iter() {
            edge_map
                .entry(record.edge_type)
                .or_default()
                .push(EdgeMetadata::from(record));
        }

        Self {
            entities,
            edges: edge_map,
        }
    }

    pub fn entity(&self, entity_type: &str) -> Option<&EntityMetadata> {
        self.entities.get(entity_type)
    }

    pub fn edge(&self, edge_type: &str) -> Option<&[EdgeMetadata]> {
        self.edges.get(edge_type).map(|vec| vec.as_slice())
    }

    pub fn entities(&self) -> impl Iterator<Item = &EntityMetadata> {
        self.entities.values()
    }
}

impl From<&'static EntityMetaRecord> for EntityMetadata {
    fn from(record: &'static EntityMetaRecord) -> Self {
        EntityMetadata {
            entity_type: record.entity_type,
            category: record.category,
            table_name: record.table_name,
            primary_keys: record.primary_keys,
            fields: record.fields,
            stable_id: record.stable_id,
        }
    }
}

impl From<&'static EdgeMetaRecord> for EdgeMetadata {
    fn from(record: &'static EdgeMetaRecord) -> Self {
        EdgeMetadata {
            edge_type: record.edge_type,
            from_entity: record.from_entity,
            to_entity: record.to_entity,
        }
    }
}

pub static SCHEMA_REGISTRY: Lazy<SchemaRegistry> = Lazy::new(SchemaRegistry::from_generated);

#[derive(Debug, Clone)]
pub struct VectorKeyMapping {
    pub vector_column: &'static str,
    pub primary_key: &'static str,
}

#[derive(Debug, Clone)]
pub enum SourceNodeId {
    PrimaryKey {
        entity_type: &'static str,
        mappings: Vec<VectorKeyMapping>,
    },
    DirectColumn {
        column: &'static str,
    },
}

#[derive(Debug, Clone)]
pub enum SourceNodeType {
    Literal(&'static str),
    FromKeyPattern(&'static str),
}

#[derive(Debug, Clone)]
pub struct VectorEdgeRule {
    pub edge_type: &'static str,
    pub source: SourceNodeId,
    pub source_node_type: SourceNodeType,
    pub target_node_type: &'static str,
}

#[derive(Debug, Clone)]
pub struct VectorRules {
    pub vector_entity: &'static str,
    pub rules: Vec<VectorEdgeRule>,
}

pub static VECTOR_EDGE_RULES: Lazy<HashMap<&'static str, VectorRules>> = Lazy::new(|| {
    let mut map: HashMap<&'static str, VectorRules> = HashMap::new();
    for record in GENERATED_VECTOR_EDGE_RULES.iter() {
        let entry = map
            .entry(record.vector_entity)
            .or_insert_with(|| VectorRules {
                vector_entity: record.vector_entity,
                rules: Vec::new(),
            });
        entry.rules.push(convert_vector_rule(record));
    }
    map
});

pub fn vector_rules(entity_type: &str) -> Option<&VectorRules> {
    VECTOR_EDGE_RULES.get(entity_type)
}

fn convert_vector_rule(record: &VectorEdgeRuleRecord) -> VectorEdgeRule {
    let source = match &record.source {
        VectorSourceRecord::PrimaryKey {
            entity_type,
            mappings,
        } => SourceNodeId::PrimaryKey {
            entity_type,
            mappings: mappings
                .iter()
                .map(|mapping: &VectorKeyMappingRecord| VectorKeyMapping {
                    vector_column: mapping.vector_column,
                    primary_key: mapping.primary_key,
                })
                .collect(),
        },
        VectorSourceRecord::DirectColumn { column } => SourceNodeId::DirectColumn { column },
    };

    let source_node_type = match &record.source_node_type {
        VectorSourceTypeRecord::Literal(value) => SourceNodeType::Literal(value),
        VectorSourceTypeRecord::FromKeyPattern(column) => {
            SourceNodeType::FromKeyPattern(column)
        }
    };

    VectorEdgeRule {
        edge_type: record.edge_type,
        source,
        source_node_type,
        target_node_type: record.target_node_type,
    }
}
