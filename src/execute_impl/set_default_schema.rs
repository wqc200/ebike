use std::sync::{Arc, Mutex};

use datafusion::execution::context::ExecutionContext;

use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::meta::{meta_util, meta_const};
use crate::mysql::error::{MysqlResult, MysqlError};

use sqlparser::ast::ObjectName;

pub struct SetDefaultSchema {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl SetDefaultSchema {
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

    pub fn execute(&mut self, schema_name: ObjectName) -> MysqlResult<u64> {
        let full_schema_name = meta_util::fill_up_schema_name(&mut self.session_context, schema_name.clone()).unwrap();

        let schema_ref_map = meta_util::read_all_schema(self.global_context.clone()).unwrap();
        if !schema_ref_map.contains_key(&full_schema_name) {
            return Err(MysqlError::new_server_error(
                1049,
                "42000",
                format!("Unknown database '{:?}'", schema_name.to_string()).as_str(),
            ));
        }

        self.execution_context.change_default_catalog_and_schema(meta_const::CATALOG_NAME.to_string(), schema_name.to_string());

        *self.session_context.current_schema.lock().expect("mutex poisoned") = Some(schema_name.to_string());

        Ok(1)
    }
}
