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
use crate::core::core_util;

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
        Ok(1)
    }
}
