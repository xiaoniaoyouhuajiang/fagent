use heck::ToUpperCamelCase;
use helix_db::helixc::parser::{
    self,
    types::{FieldType, HxFile},
};
use serde::Deserialize;
use serde_json;
use std::collections::HashSet;
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    println!("cargo:rerun-if-changed=../helixdb-cfg/schema.hx");
    println!("cargo:rerun-if-changed=../helixdb-cfg/vector_rules.json");
    println!("cargo:rerun-if-changed=build.rs");

    let schema_path = Path::new("../helixdb-cfg/schema.hx");
    let schema_content = fs::read_to_string(schema_path)?;

    let hx_file = HxFile {
        name: schema_path.to_string_lossy().into_owned(),
        content: schema_content,
    };

    let content = parser::types::Content {
        content: String::new(),
        files: vec![hx_file],
        source: Default::default(),
    };

    let ast = parser::HelixParser::parse_source(&content)
        .map_err(|e| anyhow::anyhow!("Parser error: {:?}", e))?;

    let vector_rules_config = load_vector_rules(Path::new("../helixdb-cfg/vector_rules.json"))?;

    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("generated_schemas.rs");
    let mut file = File::create(&dest_path)?;

    writeln!(file, "use serde::{{Deserialize, Serialize}};")?;
    writeln!(file, "use chrono::{{DateTime, Utc}};")?;
    writeln!(file, "use helix_db::utils::id::ID;")?;
    writeln!(file, "use crate::fetch::Fetchable;")?;
    writeln!(file, "use crate::fetch::EntityCategory;")?;
    writeln!(file, "use std::sync::Arc;")?;
    writeln!(file, "")?;

    writeln!(
        file,
        "#[derive(Debug, Clone, Copy, PartialEq, Eq)]\npub enum StableIdStrategy {{\n    None,\n    PrimaryKeyHash,\n}}\n"
    )?;

    writeln!(
        file,
        "#[derive(Debug, Clone)]\npub struct EntityMetaRecord {{\n    pub entity_type: &'static str,\n    pub category: EntityCategory,\n    pub table_name: &'static str,\n    pub primary_keys: &'static [&'static str],\n    pub fields: &'static [&'static str],\n    pub stable_id: StableIdStrategy,\n}}\n"
    )?;

    writeln!(
        file,
        "#[derive(Debug, Clone)]\npub struct EdgeMetaRecord {{\n    pub edge_type: &'static str,\n    pub from_entity: &'static str,\n    pub to_entity: &'static str,\n}}\n"
    )?;

    writeln!(
        file,
        "#[derive(Debug, Clone)]\npub struct VectorKeyMappingRecord {{\n    pub vector_column: &'static str,\n    pub primary_key: &'static str,\n}}\n"
    )?;

    writeln!(
        file,
        "#[derive(Debug, Clone)]\npub enum VectorSourceRecord {{\n    PrimaryKey {{ entity_type: &'static str, mappings: &'static [VectorKeyMappingRecord] }},\n    DirectColumn {{ column: &'static str }},\n}}\n"
    )?;

    writeln!(
        file,
        "#[derive(Debug, Clone)]\npub enum VectorSourceTypeRecord {{\n    Literal(&'static str),\n    FromKeyPattern(&'static str),\n}}\n"
    )?;

    writeln!(
        file,
        "#[derive(Debug, Clone)]\npub struct VectorEdgeRuleRecord {{\n    pub vector_entity: &'static str,\n    pub edge_type: &'static str,\n    pub target_node_type: &'static str,\n    pub source: VectorSourceRecord,\n    pub source_node_type: VectorSourceTypeRecord,\n}}\n"
    )?;

    writeln!(
        file,
        "#[derive(Debug, Clone)]\npub struct VectorIndexRecord {{\n    pub vector_entity: &'static str,\n    pub id_column: &'static str,\n    pub index_table: &'static str,\n}}\n"
    )?;

    let mut entity_meta_entries: Vec<String> = Vec::new();
    let mut edge_meta_entries: Vec<String> = Vec::new();
    let mut vector_mapping_defs: Vec<String> = Vec::new();
    let mut vector_rule_entries: Vec<String> = Vec::new();
    let mut vector_index_entries: Vec<String> = Vec::new();

    if let Some(latest_schema) = ast.get_schemas_in_order().last() {
        for node_schema in &latest_schema.node_schemas {
            let struct_name = &node_schema.name.1.to_upper_camel_case();
            let entity_type = struct_name.to_lowercase();
            let table_name = format!("silver/entities/{}", entity_type);
            let primary_keys: Vec<String> = node_schema
                .fields
                .iter()
                .filter(|field| field.is_indexed())
                .map(|field| field.name.clone())
                .collect();
            let fields: Vec<String> = node_schema
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect();
            let stable_strategy = if primary_keys.is_empty() {
                "StableIdStrategy::None"
            } else {
                "StableIdStrategy::PrimaryKeyHash"
            };
            entity_meta_entries.push(format!(
                "EntityMetaRecord {{ entity_type: \"{entity}\", category: EntityCategory::Node, table_name: \"{table}\", primary_keys: &[{pks}], fields: &[{fields}], stable_id: {stable} }}",
                entity = entity_type,
                table = table_name,
                pks = if primary_keys.is_empty() {
                    String::new()
                } else {
                    primary_keys
                        .iter()
                        .map(|pk| format!("\"{}\"", pk))
                        .collect::<Vec<_>>()
                        .join(", ")
                },
                fields = if fields.is_empty() {
                    String::new()
                } else {
                    fields
                        .iter()
                        .map(|f| format!("\"{}\"", f))
                        .collect::<Vec<_>>()
                        .join(", ")
                },
                stable = stable_strategy
            ));

            writeln!(file, "#[derive(Debug, Serialize, Deserialize, Clone)]")?;
            writeln!(file, "pub struct {} {{", struct_name)?;
            for field in &node_schema.fields {
                let field_name = &field.name;
                let field_type = map_type_to_rust(&field.field_type);
                writeln!(file, "    pub {}: {},", field_name, field_type)?;
            }
            writeln!(file, "}}")?;
            writeln!(file, "")?;

            // Generate Fetchable implementation for this struct
            generate_fetchable_impl(&mut file, struct_name, &node_schema.fields)?;
            writeln!(file, "")?;
        }

        // Generate edge schemas
        let mut generated_edge_names: HashSet<String> = HashSet::new();
        for edge_schema in &latest_schema.edge_schemas {
            let edge_struct_name = edge_schema.name.1.to_upper_camel_case();
            if !generated_edge_names.insert(edge_struct_name.clone()) {
                continue;
            }
            let edge_type = edge_struct_name.to_lowercase();
            let from_entity = edge_schema.from.1.to_upper_camel_case().to_lowercase();
            let to_entity = edge_schema.to.1.to_upper_camel_case().to_lowercase();
            edge_meta_entries.push(format!(
                "EdgeMetaRecord {{ edge_type: \"{edge_type}\", from_entity: \"{from_entity}\", to_entity: \"{to_entity}\" }}",
                edge_type = format!("edge_{}", edge_type),
                from_entity = from_entity,
                to_entity = to_entity
            ));
            generate_edge_struct(&mut file, edge_schema)?;
            writeln!(file, "")?;
        }

        for vector_schema in &latest_schema.vector_schemas {
            let struct_name = vector_schema.name.to_upper_camel_case();
            let entity_type = struct_name.to_lowercase();
            let table_name = format!("silver/vectors/{}", entity_type);
            let mut fields: Vec<String> = vector_schema
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect();
            fields.extend(
                [
                    "text",
                    "embedding",
                    "embedding_model",
                    "embedding_id",
                    "token_count",
                    "chunk_order",
                    "created_at",
                    "updated_at",
                ]
                .into_iter()
                .map(|s| s.to_string()),
            );
            entity_meta_entries.push(format!(
                "EntityMetaRecord {{ entity_type: \"{entity}\", category: EntityCategory::Vector, table_name: \"{table}\", primary_keys: &[\"id\"], fields: &[{fields}], stable_id: StableIdStrategy::None }}",
                entity = entity_type,
                table = table_name,
                fields = fields
                    .iter()
                    .map(|f| format!("\"{}\"", f))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            generate_vector_struct(&mut file, vector_schema)?;
            writeln!(file, "")?;
        }

        if let Some(config) = &vector_rules_config {
            let mut mapping_counter = 0usize;
            for vector_entry in &config.vectors {
                vector_index_entries.push(format!(
                    "VectorIndexRecord {{ vector_entity: \"{}\", id_column: \"{}\", index_table: \"silver/index_vector/{}\" }}",
                    vector_entry.vector_entity,
                    vector_entry.id_column,
                    vector_entry.vector_entity
                ));
                for edge_entry in &vector_entry.edges {
                    let source_code = match &edge_entry.source {
                        VectorSourceConfig::PrimaryKey {
                            entity_type,
                            mappings,
                        } => {
                            let mapping_ident = format!("VECTOR_RULE_MAPPING_{}", mapping_counter);
                            mapping_counter += 1;
                            let mapping_values: Vec<String> = mappings
                                .iter()
                                .map(|mapping| {
                                    format!(
                                        "VectorKeyMappingRecord {{ vector_column: \"{}\", primary_key: \"{}\" }}",
                                        mapping.vector_column, mapping.primary_key
                                    )
                                })
                                .collect();
                            vector_mapping_defs.push(format!(
                                "pub const {}: &[VectorKeyMappingRecord] = &[{}];",
                                mapping_ident,
                                mapping_values.join(", ")
                            ));
                            format!(
                                "VectorSourceRecord::PrimaryKey {{ entity_type: \"{}\", mappings: {} }}",
                                entity_type, mapping_ident
                            )
                        }
                        VectorSourceConfig::DirectColumn { column } => format!(
                            "VectorSourceRecord::DirectColumn {{ column: \"{}\" }}",
                            column
                        ),
                    };

                    let node_type_code = match &edge_entry.source_node_type {
                        VectorSourceNodeTypeConfig::Literal { value } => {
                            format!("VectorSourceTypeRecord::Literal(\"{}\")", value)
                        }
                        VectorSourceNodeTypeConfig::FromKey { column } => {
                            format!("VectorSourceTypeRecord::FromKeyPattern(\"{}\")", column)
                        }
                    };

                    vector_rule_entries.push(format!(
                        "VectorEdgeRuleRecord {{ vector_entity: \"{}\", edge_type: \"{}\", target_node_type: \"{}\", source: {}, source_node_type: {} }}",
                        vector_entry.vector_entity,
                        edge_entry.edge_type,
                        edge_entry.target_node_type,
                        source_code,
                        node_type_code
                    ));
                }
            }
        }

        // Generate meta arrays
        writeln!(
            file,
            "pub const GENERATED_ENTITY_METADATA: &[EntityMetaRecord] = &[\n    {}\n];",
            entity_meta_entries.join(",\n    ")
        )?;

        if !edge_meta_entries.is_empty() {
            writeln!(
                file,
                "pub const GENERATED_EDGE_METADATA: &[EdgeMetaRecord] = &[\n    {}\n];",
                edge_meta_entries.join(",\n    ")
            )?;
        } else {
            writeln!(
                file,
                "pub const GENERATED_EDGE_METADATA: &[EdgeMetaRecord] = &[];"
            )?;
        }
    }

    for def in &vector_mapping_defs {
        writeln!(file, "{}", def)?;
    }

    if vector_rule_entries.is_empty() {
        writeln!(
            file,
            "pub const GENERATED_VECTOR_EDGE_RULES: &[VectorEdgeRuleRecord] = &[];"
        )?;
    } else {
        writeln!(
            file,
            "pub const GENERATED_VECTOR_EDGE_RULES: &[VectorEdgeRuleRecord] = &[\n    {}\n];",
            vector_rule_entries.join(",\n    ")
        )?;
    }

    if vector_index_entries.is_empty() {
        writeln!(
            file,
            "pub const GENERATED_VECTOR_INDEX_RULES: &[VectorIndexRecord] = &[];"
        )?;
    } else {
        writeln!(
            file,
            "pub const GENERATED_VECTOR_INDEX_RULES: &[VectorIndexRecord] = &[\n    {}\n];",
            vector_index_entries.join(",\n    ")
        )?;
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
struct VectorRulesConfig {
    vectors: Vec<VectorVectorConfig>,
}

#[derive(Debug, Deserialize)]
struct VectorVectorConfig {
    vector_entity: String,
    id_column: String,
    edges: Vec<VectorEdgeConfig>,
}

#[derive(Debug, Deserialize)]
struct VectorEdgeConfig {
    edge_type: String,
    target_node_type: String,
    source: VectorSourceConfig,
    #[serde(rename = "source_node_type")]
    source_node_type: VectorSourceNodeTypeConfig,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind")]
enum VectorSourceConfig {
    #[serde(rename = "primary_key")]
    PrimaryKey {
        entity_type: String,
        mappings: Vec<VectorMappingConfig>,
    },
    #[serde(rename = "direct_column")]
    DirectColumn { column: String },
}

#[derive(Debug, Deserialize)]
struct VectorMappingConfig {
    vector_column: String,
    primary_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind")]
enum VectorSourceNodeTypeConfig {
    #[serde(rename = "literal")]
    Literal { value: String },
    #[serde(rename = "from_key")]
    FromKey { column: String },
}

fn load_vector_rules(path: &Path) -> anyhow::Result<Option<VectorRulesConfig>> {
    if !path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(path)?;
    let config: VectorRulesConfig = serde_json::from_str(&content).map_err(|err| {
        anyhow::anyhow!(
            "Failed to parse vector rules JSON '{}': {}",
            path.display(),
            err
        )
    })?;
    Ok(Some(config))
}

fn generate_edge_struct(
    file: &mut File,
    edge_schema: &helix_db::helixc::parser::types::EdgeSchema,
) -> anyhow::Result<()> {
    let edge_name = &edge_schema.name.1.to_upper_camel_case();
    let from_type = &edge_schema.from.1;
    let to_type = &edge_schema.to.1;

    // Generate edge struct with base fields
    writeln!(file, "#[derive(Debug, Serialize, Deserialize, Clone)]")?;
    writeln!(file, "pub struct {} {{", edge_name)?;
    writeln!(file, "    pub id: Option<String>,")?;
    writeln!(file, "    pub from_node_id: Option<String>,")?;
    writeln!(file, "    pub to_node_id: Option<String>,")?;
    writeln!(file, "    pub from_node_type: Option<String>,")?;
    writeln!(file, "    pub to_node_type: Option<String>,")?;
    writeln!(file, "    pub created_at: Option<DateTime<Utc>>,")?;
    writeln!(file, "    pub updated_at: Option<DateTime<Utc>>,")?;

    // Add edge-specific properties if any
    if let Some(properties) = &edge_schema.properties {
        for field in properties {
            let field_type = map_type_to_rust(&field.field_type);
            writeln!(file, "    pub {}: {},", field.name, field_type)?;
        }
    }

    writeln!(file, "}}")?;
    writeln!(file, "")?;

    // Generate Fetchable implementation for edge
    generate_edge_fetchable_impl(file, edge_name, from_type, to_type, &edge_schema.properties)?;

    Ok(())
}

fn generate_vector_struct(
    file: &mut File,
    vector_schema: &helix_db::helixc::parser::types::VectorSchema,
) -> anyhow::Result<()> {
    let struct_name = vector_schema.name.to_upper_camel_case();
    writeln!(file, "#[derive(Debug, Serialize, Deserialize, Clone)]")?;
    writeln!(file, "pub struct {} {{", struct_name)?;
    for field in &vector_schema.fields {
        let field_name = &field.name;
        let field_type = map_type_to_rust(&field.field_type);
        writeln!(file, "    pub {}: {},", field_name, field_type)?;
    }
    writeln!(file, "    pub text: Option<String>,")?;
    writeln!(file, "    pub embedding: Option<Vec<f32>>,")?;
    writeln!(file, "    pub embedding_model: Option<String>,")?;
    writeln!(file, "    pub embedding_id: Option<String>,")?;
    writeln!(file, "    pub token_count: Option<i32>,")?;
    writeln!(file, "    pub chunk_order: Option<i32>,")?;
    writeln!(file, "    pub created_at: Option<DateTime<Utc>>,")?;
    writeln!(file, "    pub updated_at: Option<DateTime<Utc>>,")?;
    writeln!(file, "}}")?;
    writeln!(file, "")?;

    generate_vector_fetchable_impl(file, &struct_name, &vector_schema.fields)?;
    Ok(())
}

fn generate_vector_fetchable_impl(
    file: &mut File,
    struct_name: &str,
    fields: &[helix_db::helixc::parser::types::Field],
) -> anyhow::Result<()> {
    let extra_fields = [
        ("text", "Option<String>", "DataType::Utf8"),
        (
            "embedding",
            "Option<Vec<f32>>",
            "DataType::List(Arc::new(Field::new(\"item\", DataType::Float32, true)))",
        ),
        ("embedding_model", "Option<String>", "DataType::Utf8"),
        ("embedding_id", "Option<String>", "DataType::Utf8"),
        ("token_count", "Option<i32>", "DataType::Int32"),
        ("chunk_order", "Option<i32>", "DataType::Int32"),
        (
            "created_at",
            "Option<DateTime<Utc>>",
            "DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, Some(\"UTC\".into()))",
        ),
        (
            "updated_at",
            "Option<DateTime<Utc>>",
            "DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, Some(\"UTC\".into()))",
        ),
    ];

    writeln!(file, "impl Fetchable for {} {{", struct_name)?;
    writeln!(
        file,
        "    const ENTITY_TYPE: &'static str = \"{}\";",
        struct_name.to_lowercase()
    )?;
    writeln!(file, "")?;

    writeln!(file, "    fn table_name() -> String {{")?;
    writeln!(
        file,
        "        format!(\"silver/vectors/{{}}\", Self::ENTITY_TYPE)"
    )?;
    writeln!(file, "    }}")?;
    writeln!(file, "")?;

    writeln!(file, "    fn primary_keys() -> Vec<&'static str> {{")?;
    writeln!(file, "        vec![\"id\"]")?;
    writeln!(file, "    }}")?;
    writeln!(file, "")?;

    writeln!(file, "    fn category() -> EntityCategory {{")?;
    writeln!(file, "        EntityCategory::Vector")?;
    writeln!(file, "    }}")?;
    writeln!(file, "")?;

    writeln!(
        file,
        "    fn to_record_batch(data: impl IntoIterator<Item = Self>) -> crate::errors::Result<deltalake::arrow::record_batch::RecordBatch> {{"
    )?;
    writeln!(
        file,
        "        let data: Vec<Self> = data.into_iter().collect();"
    )?;
    writeln!(file, "        if data.is_empty() {{")?;
    writeln!(
        file,
        "            use deltalake::arrow::datatypes::{{Field, Schema, DataType}};"
    )?;
    writeln!(file, "            let schema = Schema::new(vec![")?;
    for field in fields {
        let arrow_type = map_to_arrow_type(&field.field_type);
        writeln!(
            file,
            "                Field::new(\"{0}\", {1}, true),",
            field.name, arrow_type
        )?;
    }
    for (name, _, arrow_type) in &extra_fields {
        writeln!(
            file,
            "                Field::new(\"{0}\", {1}, true),",
            name, arrow_type
        )?;
    }
    writeln!(file, "            ]);")?;
    writeln!(
        file,
        "            return Ok(deltalake::arrow::record_batch::RecordBatch::new_empty(Arc::new(schema)));"
    )?;
    writeln!(file, "        }}")?;
    writeln!(file, "")?;

    writeln!(file, "        // Collect field data")?;
    for field in fields {
        let field_type_str = map_type_to_rust(&field.field_type);
        writeln!(
            file,
            "        let {0}_data: Vec<{1}> = data.iter().map(|item| item.{0}.clone()).collect();",
            field.name, field_type_str
        )?;
    }
    for (name, rust_type, _) in &extra_fields {
        writeln!(
            file,
            "        let {0}_data: Vec<{1}> = data.iter().map(|item| item.{0}.clone()).collect();",
            name, rust_type
        )?;
    }
    writeln!(file, "")?;

    writeln!(file, "        // Create Arrow arrays")?;
    writeln!(file, "        let mut arrays = Vec::new();")?;
    for field in fields {
        writeln!(
            file,
            "        arrays.push(crate::auto_fetchable::to_arrow_array({}_data)?);",
            field.name
        )?;
    }
    for (name, _, _) in &extra_fields {
        writeln!(
            file,
            "        arrays.push(crate::auto_fetchable::to_arrow_array({}_data)?);",
            name
        )?;
    }
    writeln!(file, "")?;

    writeln!(
        file,
        "        use deltalake::arrow::datatypes::{{Field, Schema, DataType}};"
    )?;
    writeln!(file, "        let schema = Schema::new(vec![")?;
    for field in fields {
        let arrow_type = map_to_arrow_type(&field.field_type);
        writeln!(
            file,
            "            Field::new(\"{0}\", {1}, true),",
            field.name, arrow_type
        )?;
    }
    for (name, _, arrow_type) in &extra_fields {
        writeln!(
            file,
            "            Field::new(\"{0}\", {1}, true),",
            name, arrow_type
        )?;
    }
    writeln!(file, "        ]);")?;
    writeln!(file, "")?;

    writeln!(
        file,
        "        deltalake::arrow::record_batch::RecordBatch::try_new(Arc::new(schema), arrays)"
    )?;
    writeln!(
        file,
        "            .map_err(|e| crate::errors::StorageError::Arrow(e.into()))"
    )?;
    writeln!(file, "    }}")?;
    writeln!(file, "}}")?;
    Ok(())
}

fn generate_edge_fetchable_impl(
    file: &mut File,
    edge_name: &str,
    _from_type: &str,
    _to_type: &str,
    properties: &Option<Vec<helix_db::helixc::parser::types::Field>>,
) -> anyhow::Result<()> {
    writeln!(file, "impl Fetchable for {} {{", edge_name)?;
    writeln!(
        file,
        "    const ENTITY_TYPE: &'static str = \"edge_{}\";",
        edge_name.to_lowercase()
    )?;
    writeln!(file, "")?;

    // Generate primary_keys method - use id field
    writeln!(file, "    fn primary_keys() -> Vec<&'static str> {{")?;
    writeln!(file, "        vec![\"id\"]")?;
    writeln!(file, "    }}")?;
    writeln!(file, "")?;

    writeln!(file, "    fn category() -> EntityCategory {{")?;
    writeln!(file, "        EntityCategory::Edge")?;
    writeln!(file, "    }}")?;
    writeln!(file, "")?;

    // Generate to_record_batch method
    writeln!(
        file,
        "    fn to_record_batch(data: impl IntoIterator<Item = Self>) -> crate::errors::Result<deltalake::arrow::record_batch::RecordBatch> {{"
    )?;
    writeln!(
        file,
        "        let data: Vec<Self> = data.into_iter().collect();"
    )?;
    writeln!(file, "        if data.is_empty() {{")?;
    writeln!(
        file,
        "            // Return empty record batch with correct schema"
    )?;
    writeln!(
        file,
        "            use deltalake::arrow::datatypes::{{Field, Schema, DataType}};"
    )?;
    writeln!(file, "            let schema = Schema::new(vec![")?;
    writeln!(
        file,
        "                Field::new(\"id\", DataType::Utf8, false),"
    )?;
    writeln!(
        file,
        "                Field::new(\"from_node_id\", DataType::Utf8, false),"
    )?;
    writeln!(
        file,
        "                Field::new(\"to_node_id\", DataType::Utf8, false),"
    )?;
    writeln!(
        file,
        "                Field::new(\"from_node_type\", DataType::Utf8, false),"
    )?;
    writeln!(
        file,
        "                Field::new(\"to_node_type\", DataType::Utf8, false),"
    )?;
    writeln!(
        file,
        "                Field::new(\"created_at\", DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, Some(\"UTC\".into())), true),"
    )?;
    writeln!(
        file,
        "                Field::new(\"updated_at\", DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, Some(\"UTC\".into())), true),"
    )?;

    // Add property fields
    if let Some(properties) = properties {
        for field in properties {
            let arrow_type = map_to_arrow_type(&field.field_type);
            let nullable = !matches!(
                field.field_type,
                FieldType::String
                    | FieldType::I64
                    | FieldType::I32
                    | FieldType::I16
                    | FieldType::I8
                    | FieldType::U64
                    | FieldType::U32
                    | FieldType::U16
                    | FieldType::U8
                    | FieldType::F64
                    | FieldType::F32
                    | FieldType::Boolean
            );
            writeln!(
                file,
                "                Field::new(\"{}\", {}, {}),",
                field.name, arrow_type, nullable
            )?;
        }
    }

    writeln!(file, "            ]);")?;
    writeln!(
        file,
        "            return Ok(deltalake::arrow::record_batch::RecordBatch::new_empty(Arc::new(schema)));"
    )?;
    writeln!(file, "        }}")?;
    writeln!(file, "")?;

    // Collect field data
    writeln!(file, "        // Collect field data")?;
    writeln!(
        file,
        "        let id_data: Vec<Option<String>> = data.iter().map(|item| item.id.clone()).collect();"
    )?;
    writeln!(
        file,
        "        let from_node_id_data: Vec<Option<String>> = data.iter().map(|item| item.from_node_id.clone()).collect();"
    )?;
    writeln!(
        file,
        "        let to_node_id_data: Vec<Option<String>> = data.iter().map(|item| item.to_node_id.clone()).collect();"
    )?;
    writeln!(
        file,
        "        let from_node_type_data: Vec<Option<String>> = data.iter().map(|item| item.from_node_type.clone()).collect();"
    )?;
    writeln!(
        file,
        "        let to_node_type_data: Vec<Option<String>> = data.iter().map(|item| item.to_node_type.clone()).collect();"
    )?;
    writeln!(
        file,
        "        let created_at_data: Vec<Option<DateTime<Utc>>> = data.iter().map(|item| item.created_at.clone()).collect();"
    )?;
    writeln!(
        file,
        "        let updated_at_data: Vec<Option<DateTime<Utc>>> = data.iter().map(|item| item.updated_at.clone()).collect();"
    )?;

    // Collect property data
    if let Some(properties) = properties {
        for field in properties {
            let field_name = &field.name;
            let field_type_str = map_type_to_rust(&field.field_type);
            writeln!(
                file,
                "        let {}_data: Vec<{}> = data.iter().map(|item| item.{}.clone()).collect();",
                field_name, field_type_str, field_name
            )?;
        }
    }

    writeln!(file, "")?;

    // Create Arrow arrays
    writeln!(file, "        // Create Arrow arrays")?;
    writeln!(file, "        let mut arrays = Vec::new();")?;
    writeln!(
        file,
        "        arrays.push(crate::auto_fetchable::to_arrow_array(id_data)?);"
    )?;
    writeln!(
        file,
        "        arrays.push(crate::auto_fetchable::to_arrow_array(from_node_id_data)?);"
    )?;
    writeln!(
        file,
        "        arrays.push(crate::auto_fetchable::to_arrow_array(to_node_id_data)?);"
    )?;
    writeln!(
        file,
        "        arrays.push(crate::auto_fetchable::to_arrow_array(from_node_type_data)?);"
    )?;
    writeln!(
        file,
        "        arrays.push(crate::auto_fetchable::to_arrow_array(to_node_type_data)?);"
    )?;
    writeln!(
        file,
        "        arrays.push(crate::auto_fetchable::to_arrow_array(created_at_data)?);"
    )?;
    writeln!(
        file,
        "        arrays.push(crate::auto_fetchable::to_arrow_array(updated_at_data)?);"
    )?;

    // Add property arrays
    if let Some(properties) = properties {
        for field in properties {
            writeln!(
                file,
                "        arrays.push(crate::auto_fetchable::to_arrow_array({}_data)?);",
                field.name
            )?;
        }
    }

    writeln!(file, "")?;

    // Create schema
    writeln!(file, "        // Create schema")?;
    writeln!(
        file,
        "        use deltalake::arrow::datatypes::{{Field, Schema, DataType}};"
    )?;
    writeln!(file, "        let schema = Schema::new(vec![")?;
    writeln!(
        file,
        "            Field::new(\"id\", DataType::Utf8, false),"
    )?;
    writeln!(
        file,
        "            Field::new(\"from_node_id\", DataType::Utf8, false),"
    )?;
    writeln!(
        file,
        "            Field::new(\"to_node_id\", DataType::Utf8, false),"
    )?;
    writeln!(
        file,
        "            Field::new(\"from_node_type\", DataType::Utf8, false),"
    )?;
    writeln!(
        file,
        "            Field::new(\"to_node_type\", DataType::Utf8, false),"
    )?;
    writeln!(
        file,
        "            Field::new(\"created_at\", DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, Some(\"UTC\".into())), true),"
    )?;
    writeln!(
        file,
        "            Field::new(\"updated_at\", DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, Some(\"UTC\".into())), true),"
    )?;

    // Add property fields to schema
    if let Some(properties) = properties {
        for field in properties {
            let arrow_type = map_to_arrow_type(&field.field_type);
            writeln!(
                file,
                "            Field::new(\"{}\", {}, true),",
                field.name, arrow_type
            )?;
        }
    }

    writeln!(file, "        ]);")?;
    writeln!(file, "")?;

    // Create RecordBatch
    writeln!(
        file,
        "        deltalake::arrow::record_batch::RecordBatch::try_new(Arc::new(schema), arrays)"
    )?;
    writeln!(
        file,
        "            .map_err(|e| crate::errors::StorageError::Arrow(e.into()))"
    )?;
    writeln!(file, "    }}")?;
    writeln!(file, "}}")?;

    Ok(())
}

fn generate_fetchable_impl(
    file: &mut File,
    struct_name: &str,
    fields: &[helix_db::helixc::parser::types::Field],
) -> anyhow::Result<()> {
    writeln!(file, "impl Fetchable for {} {{", struct_name)?;
    writeln!(
        file,
        "    const ENTITY_TYPE: &'static str = \"{}\";",
        struct_name.to_lowercase()
    )?;
    writeln!(file, "")?;

    // Generate primary_keys method
    writeln!(file, "    fn primary_keys() -> Vec<&'static str> {{")?;
    let index_fields: Vec<&String> = fields
        .iter()
        .filter(|f| f.is_indexed())
        .map(|f| &f.name)
        .collect();

    if index_fields.is_empty() {
        // If no INDEX field, use the first field
        if let Some(first_field) = fields.first() {
            writeln!(file, "        vec![\"{}\"]", first_field.name)?;
        } else {
            writeln!(file, "        vec![]")?;
        }
    } else {
        writeln!(
            file,
            "        vec![{}]",
            index_fields
                .iter()
                .map(|name| format!("\"{}\"", name))
                .collect::<Vec<_>>()
                .join(", ")
        )?;
    }
    writeln!(file, "    }}")?;
    writeln!(file, "")?;

    writeln!(file, "    fn category() -> EntityCategory {{")?;
    writeln!(file, "        EntityCategory::Node")?;
    writeln!(file, "    }}")?;
    writeln!(file, "")?;

    // 生成 to_record_batch 方法
    writeln!(
        file,
        "    fn to_record_batch(data: impl IntoIterator<Item = Self>) -> crate::errors::Result<deltalake::arrow::record_batch::RecordBatch> {{"
    )?;
    writeln!(
        file,
        "        let data: Vec<Self> = data.into_iter().collect();"
    )?;
    writeln!(file, "        if data.is_empty() {{")?;
    writeln!(
        file,
        "            // Return empty record batch with correct schema"
    )?;
    writeln!(
        file,
        "            use deltalake::arrow::datatypes::{{Field, Schema, DataType}};"
    )?;
    writeln!(file, "            let schema = Schema::new(vec![")?;
    for field in fields {
        let arrow_type = map_to_arrow_type(&field.field_type);
        writeln!(
            file,
            "                Field::new(\"{0}\", {1}, true),",
            field.name, arrow_type
        )?;
    }
    writeln!(file, "            ]);")?;
    writeln!(
        file,
        "            return Ok(deltalake::arrow::record_batch::RecordBatch::new_empty(Arc::new(schema)));"
    )?;
    writeln!(file, "        }}")?;
    writeln!(file, "")?;

    // 为每个字段生成数据收集代码
    writeln!(file, "        // Collect field data")?;
    for field in fields {
        let field_type_str = map_type_to_rust(&field.field_type);
        writeln!(
            file,
            "        let {0}_data: Vec<{1}> = data.iter().map(|item| item.{0}.clone()).collect();",
            field.name, field_type_str
        )?;
    }
    writeln!(file, "")?;

    // 生成 Arrow arrays
    writeln!(file, "        // Create Arrow arrays")?;
    writeln!(file, "        let mut arrays = Vec::new();")?;
    for field in fields {
        writeln!(
            file,
            "        arrays.push(crate::auto_fetchable::to_arrow_array({}_data)?);",
            field.name
        )?;
    }
    writeln!(file, "")?;

    // 创建 Schema
    writeln!(file, "        // Create schema")?;
    writeln!(
        file,
        "        use deltalake::arrow::datatypes::{{Field, Schema, DataType}};"
    )?;
    writeln!(file, "        let schema = Schema::new(vec![")?;
    for field in fields {
        let arrow_type = map_to_arrow_type(&field.field_type);
        writeln!(
            file,
            "            Field::new(\"{0}\", {1}, true),",
            field.name, arrow_type
        )?;
    }
    writeln!(file, "        ]);")?;
    writeln!(file, "")?;

    // 创建 RecordBatch
    writeln!(
        file,
        "        deltalake::arrow::record_batch::RecordBatch::try_new(Arc::new(schema), arrays)"
    )?;
    writeln!(
        file,
        "            .map_err(|e| crate::errors::StorageError::Arrow(e.into()))"
    )?;
    writeln!(file, "    }}")?;
    writeln!(file, "}}")?;

    Ok(())
}

fn map_to_arrow_type(field_type: &FieldType) -> String {
    match field_type {
        FieldType::String => "DataType::Utf8".to_string(),
        FieldType::F32 => "DataType::Float32".to_string(),
        FieldType::F64 => "DataType::Float64".to_string(),
        FieldType::I8 => "DataType::Int8".to_string(),
        FieldType::I16 => "DataType::Int16".to_string(),
        FieldType::I32 => "DataType::Int32".to_string(),
        FieldType::I64 => "DataType::Int64".to_string(),
        FieldType::U8 => "DataType::UInt8".to_string(),
        FieldType::U16 => "DataType::UInt16".to_string(),
        FieldType::U32 => "DataType::UInt32".to_string(),
        FieldType::U64 => "DataType::UInt64".to_string(),
        FieldType::U128 => "DataType::Utf8".to_string(),
        FieldType::Boolean => "DataType::Boolean".to_string(),
        FieldType::Uuid => "DataType::Utf8".to_string(), // UUID 作为字符串存储
        FieldType::Date => "DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, Some(\"UTC\".into()))".to_string(),
        FieldType::Array(_) => "DataType::List(Arc::new(Field::new(\"item\", DataType::Utf8, true)))".to_string(), // 简化处理
        FieldType::Object(_) => "DataType::Utf8".to_string(), // JSON 对象作为字符串存储
        FieldType::Identifier(_) => "DataType::Utf8".to_string(), // 标识符作为字符串存储
    }
}

fn map_type_to_rust(field_type: &FieldType) -> String {
    match field_type {
        FieldType::String => "Option<String>".to_string(),
        FieldType::F32 => "Option<f32>".to_string(),
        FieldType::F64 => "Option<f64>".to_string(),
        FieldType::I8 => "Option<i8>".to_string(),
        FieldType::I16 => "Option<i16>".to_string(),
        FieldType::I32 => "Option<i32>".to_string(),
        FieldType::I64 => "Option<i64>".to_string(),
        FieldType::U8 => "Option<u8>".to_string(),
        FieldType::U16 => "Option<u16>".to_string(),
        FieldType::U32 => "Option<u32>".to_string(),
        FieldType::U64 => "Option<u64>".to_string(),
        FieldType::U128 => "Option<String>".to_string(),
        FieldType::Boolean => "Option<bool>".to_string(),
        FieldType::Uuid => "Option<ID>".to_string(),
        FieldType::Date => "Option<DateTime<Utc>>".to_string(),
        FieldType::Array(inner) => format!("Option<Vec<{}>>", map_type_to_rust_inner(inner)),
        FieldType::Object(_) => "Option<serde_json::Value>".to_string(),
        FieldType::Identifier(s) => format!("Option<{}>", s),
    }
}

fn map_type_to_rust_inner(field_type: &FieldType) -> String {
    match field_type {
        FieldType::String => "String".to_string(),
        FieldType::F32 => "f32".to_string(),
        FieldType::F64 => "f64".to_string(),
        FieldType::I8 => "i8".to_string(),
        FieldType::I16 => "i16".to_string(),
        FieldType::I32 => "i32".to_string(),
        FieldType::I64 => "i64".to_string(),
        FieldType::U8 => "u8".to_string(),
        FieldType::U16 => "u16".to_string(),
        FieldType::U32 => "u32".to_string(),
        FieldType::U64 => "u64".to_string(),
        FieldType::U128 => "String".to_string(),
        FieldType::Boolean => "bool".to_string(),
        FieldType::Uuid => "ID".to_string(),
        FieldType::Date => "DateTime<Utc>".to_string(),
        FieldType::Array(inner) => format!("Vec<{}>", map_type_to_rust_inner(inner)),
        FieldType::Object(_) => "serde_json::Value".to_string(),
        FieldType::Identifier(s) => s.clone(),
    }
}
