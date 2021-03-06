use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use sqlparser::ast::{AlterTableOperation, Query};

use crate::core::core_util;
use crate::core::core_util::{register_all_table, check_table_exists};
use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::meta::initial;
use crate::meta::meta_def::{SparrowColumnDef, TableDef};
use crate::meta::meta_util;
use crate::meta::meta_util::load_all_table;
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::core::output::ResultSet;

pub struct SelectFrom {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl SelectFrom {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        session_context: SessionContext,
        execution_context: ExecutionContext,
    ) -> Self {
        Self {
            global_context,
            session_context,
            execution_context,
        }
    }

    pub async fn execute(&mut self, query: &Query) -> MysqlResult<ResultSet> {
        let result = check_table_exists(self.global_context.clone(), &mut self.session_context, &mut self.execution_context, query);
        if let Err(mysql_error) = result {
            return Err(mysql_error);
        }

        let result = self.query_to_plan(query);
        let logical_plan = match result {
            Ok(logical_plan) => logical_plan,
            Err(error) => {
                return Err(MysqlError::from(error));
            }
        };

        let result = self.execution_context.optimize(&logical_plan);
        let logical_plan = match result {
            Ok(logical_plan) => logical_plan,
            Err(error) => {
                return Err(MysqlError::from(error));
            }
        };

        let result = self.execution_context.create_physical_plan(&logical_plan).await;
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
                Ok(ResultSet::new(schema_ref, batches))
            }
            Err(error) => {
                Err(MysqlError::from(error))
            }
        }
    }

    fn query_to_plan(&mut self, query: &Query) -> MysqlResult<LogicalPlan> {
        let state = self.execution_context.state.lock().unwrap().clone();
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
