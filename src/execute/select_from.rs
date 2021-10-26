use std::sync::{Arc, Mutex};

use datafusion::execution::context::ExecutionContext;
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use sqlparser::ast::{AlterTableOperation};

use crate::core::core_util::register_all_table;
use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::meta::initial;
use crate::meta::meta_def::{SparrowColumnDef, TableDef};
use crate::meta::meta_util;
use crate::meta::meta_util::load_all_table;
use crate::mysql::error::{MysqlResult};

pub struct SelectFrom {
    global_context: Arc<Mutex<GlobalContext>>,
    table: TableDef,
    operation: AlterTableOperation,
}

impl SelectFrom {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table: TableDef,
        operation: AlterTableOperation,
    ) -> Self {
        Self {
            global_context,
            table,
            operation,
        }
    }

    pub fn execute(&self, datafusion_context: &mut ExecutionContext, _: &mut SessionContext) -> MysqlResult<u64> {
        let state = datafusion_context.state.lock().unwrap().clone();
        let query_planner = SqlToRel::new(&state);

        let result = self.check_table_exists_in_statement(&statement);
        if let Err(mysql_error) = result {
            return Err(mysql_error);
        }

        let result = query_planner.query_to_plan(&query);
        let mut logical_plan = match result {
            Ok(logical_plan) => logical_plan,
            Err(datafusion_error) => {
                return Err(MysqlError::from(datafusion_error));
            }
        };

        let has_rowid = self.projection_has_rowid(&statement);
        if !has_rowid {
            logical_plan = core_util::remove_rowid_from_projection(&logical_plan);
        }

        Ok(1)
    }
}
