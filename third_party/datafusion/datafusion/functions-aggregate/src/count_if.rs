use std::any::Any;

use std::mem::size_of;

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    utils::format_state_name,
    Accumulator, AggregateUDFImpl, Documentation, Signature, Volatility,
};
use datafusion_macros::user_doc;

make_udaf_expr_and_func!(
    CountIf,
    count_if,
    expr,
    "Returns number of TRUE values",
    count_if_udaf
);

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the number of TRUE values",
    syntax_example = "count_if(expression)"
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CountIf {
    signature: Signature,
}

impl Default for CountIf {
    fn default() -> Self {
        Self::new()
    }
}

impl CountIf {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Boolean], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for CountIf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "count_if"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CountIfAccumulator { count: 0 }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new(
            format_state_name(args.name, "count_if"),
            DataType::Int64,
            false,
        )
        .into()])
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[derive(Debug)]
struct CountIfAccumulator {
    count: i64,
}

impl Accumulator for CountIfAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = values[0].as_any().downcast_ref::<BooleanArray>().unwrap();

        for i in 0..arr.len() {
            if !arr.is_null(i) && arr.value(i) {
                self.count += 1;
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let arr = states[0]
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        for i in 0..arr.len() {
            if !arr.is_null(i) {
                self.count += arr.value(i);
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }

    fn size(&self) -> usize {
       size_of::<Self>()
    }
}
