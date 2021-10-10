use datafusion::error::{Result};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_plan::{Expr, LogicalPlan};
use datafusion::optimizer::optimizer::OptimizerRule;
use datafusion::optimizer::utils;

pub struct SelectPushDown {}

impl OptimizerRule for SelectPushDown {
    fn optimize(&self, plan: &LogicalPlan, execution_props: &ExecutionProps) -> Result<LogicalPlan> {
        let predicate = get_predicate(plan);
        return optimize_plan(self, plan, predicate, execution_props);
    }

    fn name(&self) -> &str {
        return "selection_push_down";
    }
}

impl SelectPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn optimize_plan(
    optimizer: &SelectPushDown,
    plan: &LogicalPlan,
    predicate: Option<Expr>, // set of columns required up to this step
    execution_props: &ExecutionProps,
) -> Result<LogicalPlan> {
    match plan {
        // scans:
        // * remove un-used columns from the scan projection
        LogicalPlan::TableScan {
            table_name,
            source,
            projection,
            projected_schema,
            filters,
            limit,
        } => {
            // return the table scan with projection
            Ok(LogicalPlan::TableScan {
                table_name: table_name.to_string(),
                source: source.clone(),
                projection: projection.clone(),
                projected_schema: projected_schema.clone(),
                filters: filters.clone(),
                limit: limit.clone(),
            })
        }
        _ => {
            let expr = plan.expressions();

            // apply the optimization to all inputs of the plan
            let inputs = plan.inputs();
            let new_inputs = inputs
                .iter()
                .map(|plan| {
                    optimize_plan(optimizer, plan, predicate.clone(), execution_props)
                })
                .collect::<Result<Vec<_>>>()?;

            utils::from_plan(plan, &expr, &new_inputs)
        }
    }
}

fn get_predicate(
    plan: &LogicalPlan,
) -> Option<Expr> {
    match plan {
        LogicalPlan::Filter {
            predicate,
            ..
        } => {
            Some(predicate.clone())
        }
        _ => {
            let inputs = plan.inputs();

            if inputs.len() > 0 {
                let input = inputs[0];
                let expr = get_predicate(input);
                expr
            } else {
                None
            }
        }
    }
}
