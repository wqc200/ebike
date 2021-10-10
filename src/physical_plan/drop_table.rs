use std::sync::{Mutex, Arc};

use datafusion::execution::context::ExecutionContext;

use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::mysql::error::{MysqlResult, MysqlError};

use crate::meta::meta_def::TableDef;

pub struct PhysicalPlanDropTable {
    global_context: Arc<Mutex<GlobalContext>>,
    table: TableDef,
}

impl PhysicalPlanDropTable {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table: TableDef,
    ) -> Self {
        Self {
            global_context,
            table,
        }
    }

    pub fn execute(&self, datafusion_context: &mut ExecutionContext, _: &mut SessionContext) -> MysqlResult<u64> {
        let mut gc = self.global_context.lock().unwrap();

        let full_table_name = self.table.option.full_table_name.clone();
        gc.meta_data.delete_table(full_table_name.clone());

        let result = datafusion_context.deregister_table(full_table_name.to_string().as_str());
        if let Err(e) = result {
            return Err(MysqlError::from(e));
        }

        Ok(1)
    }
}
