use heck::ToUpperCamelCase;
use helix_db::helixc::parser::{
    self,
    types::{FieldType, HxFile},
};
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    println!("cargo:rerun-if-changed=../helixdb-cfg/schema.hx");
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

    if let Some(latest_schema) = ast.get_schemas_in_order().last() {
        for node_schema in &latest_schema.node_schemas {
            let struct_name = &node_schema.name.1.to_upper_camel_case();
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
        for edge_schema in &latest_schema.edge_schemas {
            generate_edge_struct(&mut file, edge_schema)?;
            writeln!(file, "")?;
        }
    }

    Ok(())
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
