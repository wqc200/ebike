use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use sqlparser::ast::{AlterTableOperation, Query};

use crate::core::core_util;
use crate::core::core_util::register_all_table;
use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::meta::initial;
use crate::meta::meta_def::{SparrowColumnDef, TableDef};
use crate::meta::meta_util;
use crate::meta::meta_util::load_all_table;
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::physical_plan;
use crate::core::output::ResultSet;

pub struct SelectFrom {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    datafusion_context: ExecutionContext,
}

impl SelectFrom {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        session_context: SessionContext,
        datafusion_context: ExecutionContext,
    ) -> Self {
        Self {
            global_context,
            session_context,
            datafusion_context,
        }
    }

    pub async fn execute(&mut self, query: &Query) -> MysqlResult<ResultSet> {
        let result = self.query_to_plan(query);
        let logical_plan = match result {
            Ok(logical_plan) => logical_plan,
            Err(error) => {
                return Err(MysqlError::from(error));
            }
        };

        let result = self.datafusion_context.optimize(&logical_plan);
        let logical_plan = match result {
            Ok(logical_plan) => logical_plan,
            Err(error) => {
                return Err(MysqlError::from(error));
            }
        };

        let result = self.datafusion_context.create_physical_plan(&logical_plan);
        let execution_plan = match result {
            Ok(execution_plan) => execution_plan,
            Err(error) => {
                return Err(MysqlError::from(error));
            }
        };

        let schema_ref = execution_plan.schema();
        let result = collect(execution_plan).await;
        match result {
            Ok(batches) => {
                let result_set = ResultSet::new(schema_ref, batches);
                Ok(result_set)
            }
            Err(error) => {
                Err(MysqlError::from(error))
            }
        }
    }

    fn query_to_plan(&mut self, query: &Query) -> MysqlResult<LogicalPlan> {
        let state = self.datafusion_context.state.lock().unwrap().clone();
        let query_planner = SqlToRel::new(&state);

        let result = query_planner.query_to_plan(query);
        let mut logical_plan = match result {
            Ok(logical_plan) => logical_plan,
            Err(error) => {
                return Err(MysqlError::from(error));
            }
        };

        return Ok(logical_plan);
    }
}
