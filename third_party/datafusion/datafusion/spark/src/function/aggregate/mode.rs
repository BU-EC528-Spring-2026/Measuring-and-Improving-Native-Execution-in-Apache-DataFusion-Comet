use datafusion_expr::AggregateUDF;
use std::sync::Arc;

pub fn mode() -> Arc<AggregateUDF> {
    datafusion_functions_aggregate::mode::mode_udaf()
}
