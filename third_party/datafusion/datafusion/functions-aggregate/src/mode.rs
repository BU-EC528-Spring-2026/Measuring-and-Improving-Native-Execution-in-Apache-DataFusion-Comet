use std::any::Any;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, FieldRef};

use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    utils::format_state_name,
    Accumulator, AggregateUDFImpl, Documentation, Signature, Volatility,
};
use datafusion_macros::user_doc;

make_udaf_expr_and_func!(
    Mode,
    mode,
    expr,
    "Returns most frequent value",
    mode_udaf
);

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the most frequent non-null value",
    syntax_example = "mode(expression)"
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Mode {
    signature: Signature,
}

impl Default for Mode {
    fn default() -> Self {
        Self::new()
    }
}

impl Mode {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for Mode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "mode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(DataFusionError::Internal(
                "mode expects exactly one argument".to_string(),
            ));
        }

        if !is_supported_mode_type(&arg_types[0]) {
            return Err(DataFusionError::NotImplemented(format!(
                "mode is currently supported only for integer types, got {}",
                arg_types[0]
            )));
        }

        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = args.expr_fields[0].data_type().clone();

        if !is_supported_mode_type(&data_type) {
            return Err(DataFusionError::NotImplemented(format!(
                "mode is currently supported only for integer types, got {}",
                data_type
            )));
        }

        Ok(Box::new(ModeAccumulator {
            data_type,
            counts: HashMap::new(),
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "mode"),
            DataType::Binary,
            true,
        ))])
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[derive(Debug)]
struct ModeAccumulator {
    data_type: DataType,
    counts: HashMap<i64, i64>,
}

impl Accumulator for ModeAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let bytes = serialize_counts(&self.counts);
        Ok(vec![ScalarValue::Binary(Some(bytes))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = &values[0];

        match &self.data_type {
            DataType::Int8 => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| type_mismatch("Int8"))?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        *self.counts.entry(arr.value(i) as i64).or_insert(0) += 1;
                    }
                }
            }
            DataType::Int16 => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| type_mismatch("Int16"))?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        *self.counts.entry(arr.value(i) as i64).or_insert(0) += 1;
                    }
                }
            }
            DataType::Int32 => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| type_mismatch("Int32"))?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        *self.counts.entry(arr.value(i) as i64).or_insert(0) += 1;
                    }
                }
            }
            DataType::Int64 => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| type_mismatch("Int64"))?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        *self.counts.entry(arr.value(i)).or_insert(0) += 1;
                    }
                }
            }
            DataType::UInt8 => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .ok_or_else(|| type_mismatch("UInt8"))?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        *self.counts.entry(arr.value(i) as i64).or_insert(0) += 1;
                    }
                }
            }
            DataType::UInt16 => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .ok_or_else(|| type_mismatch("UInt16"))?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        *self.counts.entry(arr.value(i) as i64).or_insert(0) += 1;
                    }
                }
            }
            DataType::UInt32 => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| type_mismatch("UInt32"))?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        *self.counts.entry(arr.value(i) as i64).or_insert(0) += 1;
                    }
                }
            }
            DataType::UInt64 => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| type_mismatch("UInt64"))?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        let v = i64::try_from(arr.value(i)).map_err(|_| {
                            DataFusionError::Execution(
                                "mode UInt64 value exceeds i64 range".to_string(),
                            )
                        })?;
                        *self.counts.entry(v).or_insert(0) += 1;
                    }
                }
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "mode update_batch does not support type {}",
                    other
                )));
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let arr = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "mode merge_batch expected BinaryArray state".to_string(),
                )
            })?;

        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }

            let incoming = deserialize_counts(arr.value(i))?;
            for (k, v) in incoming {
                *self.counts.entry(k).or_insert(0) += v;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.counts.is_empty() {
            return Ok(match &self.data_type {
                DataType::Int8 => ScalarValue::Int8(None),
                DataType::Int16 => ScalarValue::Int16(None),
                DataType::Int32 => ScalarValue::Int32(None),
                DataType::Int64 => ScalarValue::Int64(None),
                DataType::UInt8 => ScalarValue::UInt8(None),
                DataType::UInt16 => ScalarValue::UInt16(None),
                DataType::UInt32 => ScalarValue::UInt32(None),
                DataType::UInt64 => ScalarValue::UInt64(None),
                _ => ScalarValue::Null,
            });
        }        


        let mut best_key = 0i64;
        let mut best_count = i64::MIN;
        let mut initialized = false;

        for (k, c) in &self.counts {
            if !initialized || *c > best_count || (*c == best_count && *k < best_key) {
                best_key = *k;
                best_count = *c;
                initialized = true;
            }
        }

        scalar_from_i64(best_key, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.counts.len() * size_of::<(i64, i64)>()
    }
}

fn is_supported_mode_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn scalar_from_i64(value: i64, data_type: &DataType) -> Result<ScalarValue> {
    match data_type {
        DataType::Int8 => Ok(ScalarValue::Int8(Some(value as i8))),
        DataType::Int16 => Ok(ScalarValue::Int16(Some(value as i16))),
        DataType::Int32 => Ok(ScalarValue::Int32(Some(value as i32))),
        DataType::Int64 => Ok(ScalarValue::Int64(Some(value))),
        DataType::UInt8 => Ok(ScalarValue::UInt8(Some(value as u8))),
        DataType::UInt16 => Ok(ScalarValue::UInt16(Some(value as u16))),
        DataType::UInt32 => Ok(ScalarValue::UInt32(Some(value as u32))),
        DataType::UInt64 => Ok(ScalarValue::UInt64(Some(value as u64))),
        other => Err(DataFusionError::NotImplemented(format!(
            "mode evaluate does not support type {}",
            other
        ))),
    }
}

fn serialize_counts(counts: &HashMap<i64, i64>) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + counts.len() * 16);
    out.extend_from_slice(&(counts.len() as u32).to_le_bytes());

    for (k, v) in counts {
        out.extend_from_slice(&k.to_le_bytes());
        out.extend_from_slice(&v.to_le_bytes());
    }

    out
}

fn deserialize_counts(bytes: &[u8]) -> Result<HashMap<i64, i64>> {
    if bytes.len() < 4 {
        return Err(DataFusionError::Execution(
            "mode state is too short".to_string(),
        ));
    }

    let mut pos = 0usize;

    let len = u32::from_le_bytes(
        bytes[pos..pos + 4]
            .try_into()
            .map_err(|_| DataFusionError::Execution("invalid mode state header".to_string()))?,
    ) as usize;
    pos += 4;

    let expected = 4 + len * 16;
    if bytes.len() != expected {
        return Err(DataFusionError::Execution(format!(
            "invalid mode state length: expected {}, got {}",
            expected,
            bytes.len()
        )));
    }

    let mut counts = HashMap::with_capacity(len);

    for _ in 0..len {
        let key = i64::from_le_bytes(
            bytes[pos..pos + 8]
                .try_into()
                .map_err(|_| DataFusionError::Execution("invalid mode key".to_string()))?,
        );
        pos += 8;

        let value = i64::from_le_bytes(
            bytes[pos..pos + 8]
                .try_into()
                .map_err(|_| DataFusionError::Execution("invalid mode count".to_string()))?,
        );
        pos += 8;

        counts.insert(key, value);
    }

    Ok(counts)
}

fn type_mismatch(expected: &str) -> DataFusionError {
    DataFusionError::Internal(format!(
        "mode failed to downcast input array to {}",
        expected
    ))
}
