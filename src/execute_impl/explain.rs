use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use sqlparser::ast::{AlterTableOperation, Query, Statement};

use crate::core::core_util;
use crate::core::core_util::{check_table_exists, register_all_table};
use crate::core::global_context::GlobalContext;
use crate::core::output::ResultSet;
use crate::core::session_context::SessionContext;
use crate::meta::initial;
use crate::meta::meta_def::{SparrowColumnDef, TableDef};
use crate::meta::meta_util;
use crate::meta::meta_util::load_all_table;
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::physical_plan;

pub struct Explain {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl Explain {
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

    pub async fn execute(
        &mut self,
        verbose: bool,
        analyze: bool,
        statement: &Statement,
    ) -> MysqlResult<ResultSet> {
        let result = self.explain_statement_to_plan(verbose, analyze, &statement);
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

        let result = self.execution_context.create_physical_plan(&logical_plan);
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
            Err(error) => Err(MysqlError::from(error)),
        }
    }

    fn explain_statement_to_plan(
        &mut self,
        verbose: bool,
        analyze: bool,
        statement: &Statement,
    ) -> MysqlResult<LogicalPlan> {
        let state = self.execution_context.state.lock().unwrap().clone();
        let query_planner = SqlToRel::new(&state);

        let result = query_planner.explain_statement_to_plan(verbose, analyze, statement);
        let mut logical_plan = match result {
            Ok(logical_plan) => logical_plan,
            Err(error) => {
                return Err(MysqlError::from(error));
            }
        };

        return Ok(logical_plan);
    }
}
