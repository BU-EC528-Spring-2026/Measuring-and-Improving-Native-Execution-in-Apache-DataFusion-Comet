use datafusion_expr::AggregateUDF;
use std::sync::Arc;

pub fn count_if() -> Arc<AggregateUDF> {
    datafusion_functions_aggregate::count_if::count_if_udaf()
}
