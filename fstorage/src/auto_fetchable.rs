use crate::errors::Result;
use deltalake::arrow::array::*;
use deltalake::arrow::datatypes::DataType;
use std::sync::Arc;

/// 通用类型转换特性
pub trait ToArrowArray {
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

// 通用类型转换函数
pub fn to_arrow_array<T: ToArrowArray>(values: Vec<Option<T>>) -> Result<Arc<dyn Array>> {
    Ok(T::to_arrow_array(values))
}

/// 从 Arrow Array 中获取标量值并转换为 serde_json::Value
pub fn get_scalar_value(column: &dyn Array, row_idx: usize) -> Option<serde_json::Value> {
    if column.is_null(row_idx) {
        return None;
    }

    let data_type = column.data_type();
    let value = match data_type {
        DataType::Utf8 => {
            let array = column.as_any().downcast_ref::<StringArray>()?;
            array.value(row_idx).to_string().into()
        }
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<Int64Array>()?;
            array.value(row_idx).into()
        }
        DataType::Int32 => {
            let array = column.as_any().downcast_ref::<Int32Array>()?;
            array.value(row_idx).into()
        }
        DataType::Int16 => {
            let array = column.as_any().downcast_ref::<Int16Array>()?;
            array.value(row_idx).into()
        }
        DataType::Int8 => {
            let array = column.as_any().downcast_ref::<Int8Array>()?;
            array.value(row_idx).into()
        }
        DataType::UInt64 => {
            let array = column.as_any().downcast_ref::<UInt64Array>()?;
            array.value(row_idx).into()
        }
        DataType::UInt32 => {
            let array = column.as_any().downcast_ref::<UInt32Array>()?;
            array.value(row_idx).into()
        }
        DataType::UInt16 => {
            let array = column.as_any().downcast_ref::<UInt16Array>()?;
            array.value(row_idx).into()
        }
        DataType::UInt8 => {
            let array = column.as_any().downcast_ref::<UInt8Array>()?;
            array.value(row_idx).into()
        }
        DataType::Float64 => {
            let array = column.as_any().downcast_ref::<Float64Array>()?;
            array.value(row_idx).into()
        }
        DataType::Float32 => {
            let array = column.as_any().downcast_ref::<Float32Array>()?;
            array.value(row_idx).into()
        }
        DataType::Boolean => {
            let array = column.as_any().downcast_ref::<BooleanArray>()?;
            array.value(row_idx).into()
        }
        DataType::Timestamp(deltalake::arrow::datatypes::TimeUnit::Microsecond, _) => {
            let array = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()?;
            array.value(row_idx).to_string().into()
        }
        _ => serde_json::Value::Null,
    };

    Some(value)
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
            .map(|vec| {
                vec.map(|v| {
                    let string_vec: Vec<String> = v.iter().map(|i| i.to_string()).collect();
                    format!("[{}]", string_vec.join(", "))
                })
            })
            .collect();
        StringArray::from(json_strings).into_arc()
    }
}

impl ToArrowArray for Vec<f32> {
    fn to_arrow_array(values: Vec<Option<Self>>) -> Arc<dyn Array> {
        let mut builder = ListBuilder::new(Float32Builder::new());
        for value in values {
            match value {
                Some(vec) => {
                    {
                        let values_builder = builder.values();
                        for v in vec {
                            values_builder.append_value(v);
                        }
                    }
                    builder.append(true);
                }
                None => {
                    builder.append(false);
                }
            }
        }
        Arc::new(builder.finish())
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
