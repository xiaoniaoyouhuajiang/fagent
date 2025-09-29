use crate::errors::Result;
use deltalake::arrow::array::*;
use deltalake::arrow::datatypes::DataType;
use std::sync::Arc;

/// 通用类型转换特性
trait ToArrowArray {
    fn to_arrow_array(values: Vec<Option<Self>>) -> Arc<dyn Array>
    where
        Self: Sized;
}

macro_rules! impl_to_arrow_array {
    ($rust_type:ty, $arrow_type:ty, $constructor:path) => {
        impl ToArrowArray for $rust_type {
            fn to_arrow_array(values: Vec<Option<Self>>) -> Arc<dyn Array> {
                Arc::new($constructor(values))
            }
        }
    };
}

// 为基本类型实现转换
impl_to_arrow_array!(String, StringArray, StringArray::from);
impl_to_arrow_array!(i64, Int64Array, Int64Array::from);
impl_to_arrow_array!(i32, Int32Array, Int32Array::from);
impl_to_arrow_array!(i16, Int16Array, Int16Array::from);
impl_to_arrow_array!(i8, Int8Array, Int8Array::from);
impl_to_arrow_array!(u64, UInt64Array, UInt64Array::from);
impl_to_arrow_array!(u32, UInt32Array, UInt32Array::from);
impl_to_arrow_array!(u16, UInt16Array, UInt16Array::from);
impl_to_arrow_array!(u8, UInt8Array, UInt8Array::from);
impl_to_arrow_array!(f64, Float64Array, Float64Array::from);
impl_to_arrow_array!(f32, Float32Array, Float32Array::from);
impl_to_arrow_array!(bool, BooleanArray, BooleanArray::from);

// 为特殊类型实现转换
impl ToArrowArray for chrono::DateTime<chrono::Utc> {
    fn to_arrow_array(values: Vec<Option<Self>>) -> Arc<dyn Array> {
        let timestamps: Vec<Option<i64>> = values
            .into_iter()
            .map(|dt| dt.map(|dt| dt.timestamp_micros()))
            .collect();
        Arc::new(TimestampMicrosecondArray::from(timestamps).with_timezone("UTC"))
    }
}

impl ToArrowArray for helix_db::utils::id::ID {
    fn to_arrow_array(values: Vec<Option<Self>>) -> Arc<dyn Array> {
        let strings: Vec<Option<String>> = values
            .into_iter()
            .map(|id| id.map(|id| id.to_string()))
            .collect();
        Arc::new(StringArray::from(strings))
    }
}

/// 将 HelixQL 类型映射到 Arrow 数据类型
fn map_helixql_type_to_arrow(field_type: &str) -> DataType {
    match field_type {
        "String" => DataType::Utf8,
        "Option<String>" => DataType::Utf8,
        "I64" | "Option<I64>" => DataType::Int64,
        "I32" | "Option<I32>" => DataType::Int32,
        "I16" | "Option<I16>" => DataType::Int16,
        "I8" | "Option<I8>" => DataType::Int8,
        "U64" | "Option<U64>" => DataType::UInt64,
        "U32" | "Option<U32>" => DataType::UInt32,
        "U16" | "Option<U16>" => DataType::UInt16,
        "U8" | "Option<U8>" => DataType::UInt8,
        "F64" | "Option<F64>" => DataType::Float64,
        "F32" | "Option<F32>" => DataType::Float32,
        "Boolean" | "Option<Boolean>" => DataType::Boolean,
        "DateTime<Utc>" | "Option<DateTime<Utc>>" => {
            DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        }
        "ID" | "Option<ID>" => DataType::Utf8,
        // 对于复杂类型，使用 JSON 字符串
        _ => DataType::Utf8,
    }
}

/// 检查类型是否可空
fn is_nullable_type(field_type: &str) -> bool {
    field_type.starts_with("Option<")
}

/// 提取基础类型名称
fn extract_base_type(field_type: &str) -> &str {
    if let Some(start) = field_type.find('<') {
        if let Some(end) = field_type.rfind('>') {
            return &field_type[start + 1..end];
        }
    }
    field_type
}

/// 宏：自动为结构体生成 Fetchable 实现
macro_rules! impl_fetchable_for_struct {
    ($struct_name:ident, $entity_type:literal, $($field_name:ident: $field_type:ty),* $(,)?) => {
        impl Fetchable for $struct_name {
            const ENTITY_TYPE: &'static str = $entity_type;

            fn primary_keys() -> Vec<&'static str> {
                // 第一个 INDEX 字段作为主键
                vec![$(stringify!($field_name)),*].first().map(|s| vec![*s]).unwrap_or_default()
            }

            fn to_record_batch(data: impl IntoIterator<Item = Self>) -> Result<RecordBatch> {
                let data: Vec<$struct_name> = data.into_iter().collect();
                
                // 收集所有字段的数据
                let mut arrays = Vec::new();
                let mut fields = Vec::new();
                
                $(
                    let field_values: Vec<Option<$field_type>> = data.iter()
                        .map(|item| item.$field_name.clone())
                        .collect();
                    
                    let arrow_type = map_helixql_type_to_arrow(stringify!($field_type));
                    let is_nullable = is_nullable_type(stringify!($field_type));
                    
                    fields.push(Field::new(stringify!($field_name), arrow_type, is_nullable));
                    arrays.push(ToArrowArray::to_arrow_array(field_values));
                )*

                let schema = Schema::new(fields);
                
                RecordBatch::try_new(Arc::new(schema), arrays)
                    .map_err(|e| crate::errors::StorageError::Arrow(e.into()))
            }
        }
    };
}

// 通用类型转换函数
pub fn to_arrow_array<T: ToArrowArray>(values: Vec<Option<T>>) -> Result<Arc<dyn Array>> {
    Ok(T::to_arrow_array(values))
}

// 为 Vec 类型实现转换 - 仅支持基本类型
impl ToArrowArray for Vec<String> {
    fn to_arrow_array(values: Vec<Option<Self>>) -> Arc<dyn Array> {
        // 简化处理：将 Vec 转换为 JSON 字符串
        let json_strings: Vec<Option<String>> = values
            .into_iter()
            .map(|vec| vec.map(|v| format!("[\"{}\"]", v.join("\", \""))))
            .collect();
        StringArray::from(json_strings).into_arc()
    }
}

impl ToArrowArray for Vec<i64> {
    fn to_arrow_array(values: Vec<Option<Self>>) -> Arc<dyn Array> {
        let json_strings: Vec<Option<String>> = values
            .into_iter()
            .map(|vec| vec.map(|v| {
                let string_vec: Vec<String> = v.iter().map(|i| i.to_string()).collect();
                format!("[{}]", string_vec.join(", "))
            }))
            .collect();
        StringArray::from(json_strings).into_arc()
    }
}

// 为 serde_json::Value 实现转换
impl ToArrowArray for serde_json::Value {
    fn to_arrow_array(values: Vec<Option<Self>>) -> Arc<dyn Array> {
        let json_strings: Vec<Option<String>> = values
            .into_iter()
            .map(|value| value.map(|v| v.to_string()))
            .collect();
        StringArray::from(json_strings).into_arc()
    }
}

// 为 Arc<dyn Array> 实现 From trait 以方便使用
trait IntoArc {
    fn into_arc(self) -> Arc<dyn Array>;
}

impl<T: Array + 'static> IntoArc for T {
    fn into_arc(self) -> Arc<dyn Array> {
        Arc::new(self)
    }
}