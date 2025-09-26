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

    let ast = parser::HelixParser::parse_source(&content).map_err(|e| anyhow::anyhow!("Parser error: {:?}", e))?;

    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("generated_schemas.rs");
    let mut file = File::create(&dest_path)?;

    writeln!(file, "use serde::{{Deserialize, Serialize}};")?;
    writeln!(file, "use chrono::{{DateTime, Utc}};")?;
    writeln!(file, "use helix_db::utils::id::ID;")?;
    writeln!(file, "")?;

    if let Some(latest_schema) = ast.get_schemas_in_order().last() {
        for node_schema in &latest_schema.node_schemas {
            writeln!(
                file,
                "#[derive(Debug, Serialize, Deserialize, Clone)]"
            )?;
            writeln!(file, "pub struct {} {{", node_schema.name.1)?;
            for field in &node_schema.fields {
                let field_name = &field.name;
                let field_type = map_type_to_rust(&field.field_type);
                writeln!(file, "    pub {}: {},", field_name, field_type)?;
            }
            writeln!(file, "}}")?;
            writeln!(file, "")?;
        }

        for edge_schema in &latest_schema.edge_schemas {
            writeln!(
                file,
                "#[derive(Debug, Serialize, Deserialize, Clone)]"
            )?;
            writeln!(file, "pub struct {} {{", edge_schema.name.1)?;
            if let Some(properties) = &edge_schema.properties {
                for field in properties {
                    let field_name = &field.name;
                    let field_type = map_type_to_rust(&field.field_type);
                    writeln!(file, "    pub {}: {},", field_name, field_type)?;
                }
            }
            writeln!(file, "}}")?;
            writeln!(file, "")?;
        }
    }

    Ok(())
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
        FieldType::U128 => "Option<u128>".to_string(),
        FieldType::Boolean => "Option<bool>".to_string(),
        FieldType::Uuid => "Option<ID>".to_string(),
        FieldType::Date => "Option<DateTime<Utc>>".to_string(),
        FieldType::Array(inner) => format!("Option<Vec<{}>>", map_type_to_rust_inner(inner)),
        FieldType::Object(_) => {
            "Option<serde_json::Value>".to_string()
        }
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
        FieldType::U128 => "u128".to_string(),
        FieldType::Boolean => "bool".to_string(),
        FieldType::Uuid => "ID".to_string(),
        FieldType::Date => "DateTime<Utc>".to_string(),
        FieldType::Array(inner) => format!("Vec<{}>", map_type_to_rust_inner(inner)),
        FieldType::Object(_) => "serde_json::Value".to_string(),
        FieldType::Identifier(s) => s.clone(),
    }
}
