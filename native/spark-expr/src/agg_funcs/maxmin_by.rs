use arrow::array::ArrayRef;
use arrow::datatypes::FieldRef;
use arrow::datatypes::{DataType, Field};
use datafusion::common::{internal_err, not_impl_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::expressions::format_state_name;
use std::any::Any;
use std::cmp::Ordering;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MaxMinByKind {
    Max,
    Min,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaxMinBy {
    name: String,
    signature: Signature,
    value_type: DataType,
    order_type: DataType,
    kind: MaxMinByKind,
}

impl std::hash::Hash for MaxMinBy {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
        self.value_type.hash(state);
        self.order_type.hash(state);
        self.kind.hash(state);
    }
}

impl MaxMinBy {
    pub fn new(
        name: impl Into<String>,
        value_type: DataType,
        order_type: DataType,
        kind: MaxMinByKind,
    ) -> Self {
        Self {
            name: name.into(),
            signature: Signature::any(2, Volatility::Immutable),
            value_type,
            order_type,
            kind,
        }
    }
}

impl AggregateUDFImpl for MaxMinBy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.value_type.clone())
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        ScalarValue::try_from(data_type)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MaxMinByAccumulator::new(
            self.value_type.clone(),
            self.order_type.clone(),
            self.kind,
        )))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(&self.name, "value"),
                self.value_type.clone(),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(&self.name, "ordering"),
                self.order_type.clone(),
                true,
            )),
        ])
    }
}

#[derive(Debug)]
struct MaxMinByAccumulator {
    value_type: DataType,
    order_type: DataType,
    kind: MaxMinByKind,
    best_value: Option<ScalarValue>,
    best_order: Option<ScalarValue>,
}

impl MaxMinByAccumulator {
    fn new(value_type: DataType, order_type: DataType, kind: MaxMinByKind) -> Self {
        Self {
            value_type,
            order_type,
            kind,
            best_value: None,
            best_order: None,
        }
    }

    fn typed_null(dt: &DataType) -> Result<ScalarValue> {
        ScalarValue::try_from(dt)
    }

    fn should_replace(&self, old_order: &ScalarValue, new_order: &ScalarValue) -> Result<bool> {
        match new_order.partial_cmp(old_order) {
            Some(Ordering::Greater) => Ok(matches!(self.kind, MaxMinByKind::Max)),
            Some(Ordering::Less) => Ok(matches!(self.kind, MaxMinByKind::Min)),
            Some(Ordering::Equal) => Ok(true), // Spark semantics: ties pick the newer/right value
            None => internal_err!(
                "max_by/min_by encountered non-orderable values: old={old_order:?}, new={new_order:?}"
            ),
        }
    }

    fn update_pair(&mut self, value: ScalarValue, order: ScalarValue) -> Result<()> {
        match (&self.best_order, order.is_null()) {
            (None, true) => {
                // both current and incoming ordering are null => keep null state
                Ok(())
            }
            (None, false) => {
                self.best_value = Some(value);
                self.best_order = Some(order);
                Ok(())
            }
            (Some(_), true) => {
                // incoming ordering is null => keep current
                Ok(())
            }
            (Some(current_order), false) => {
                if self.should_replace(current_order, &order)? {
                    self.best_value = Some(value);
                    self.best_order = Some(order);
                }
                Ok(())
            }
        }
    }
}

impl Accumulator for MaxMinByAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            self.best_value
                .clone()
                .unwrap_or(Self::typed_null(&self.value_type)?),
            self.best_order
                .clone()
                .unwrap_or(Self::typed_null(&self.order_type)?),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 2 {
            return internal_err!("max_by/min_by expects exactly 2 arguments");
        }

        for i in 0..values[0].len() {
            let value = ScalarValue::try_from_array(&values[0], i)?;
            let order = ScalarValue::try_from_array(&values[1], i)?;
            self.update_pair(value, order)?;
        }

        Ok(())
    }

    fn retract_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
        not_impl_err!("retract_batch is not implemented for max_by/min_by")
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 2 {
            return internal_err!("max_by/min_by state expects exactly 2 arrays");
        }

        for i in 0..states[0].len() {
            let value = ScalarValue::try_from_array(&states[0], i)?;
            let order = ScalarValue::try_from_array(&states[1], i)?;
            self.update_pair(value, order)?;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self
            .best_value
            .clone()
            .unwrap_or(Self::typed_null(&self.value_type)?))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
