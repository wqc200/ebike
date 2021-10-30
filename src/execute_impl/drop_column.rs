use std::sync::{Arc, Mutex};

use datafusion::execution::context::ExecutionContext;
use sqlparser::ast::{AlterTableOperation};

use crate::core::core_util::register_all_table;
use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::meta::initial;
use crate::meta::meta_def::{SparrowColumnDef, TableDef};
use crate::meta::meta_util;
use crate::meta::meta_util::load_all_table;
use crate::mysql::error::{MysqlResult};

pub struct DropColumn {
    global_context: Arc<Mutex<GlobalContext>>,
    table: TableDef,
}

impl DropColumn {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table: TableDef,
    ) -> Self {
        Self {
            global_context,
            table,
        }
    }

    pub fn execute(&self, datafusion_context: &mut ExecutionContext) -> MysqlResult<u64> {
        meta_util::cache_add_all_table(self.global_context.clone());

        let result = load_all_table(self.global_context.clone());
        if let Err(mysql_error) = result {
            return Err(mysql_error);
        }

        let result = register_all_table(self.global_context.clone(), datafusion_context);
        if let Err(mysql_error) = result {
            return Err(mysql_error);
        }

        Ok(1)
    }
}
