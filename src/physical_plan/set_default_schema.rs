use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use datafusion::error::{Result};
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::LogicalPlan;

use crate::core::core_util;
use crate::core::global_context::GlobalContext;
use crate::core::output::CoreOutput;
use crate::core::output::FinalCount;
use crate::core::session_context::SessionContext;
use crate::meta::{meta_util, meta_const};
use crate::mysql::error::{MysqlResult, MysqlError};

use crate::util;
use sqlparser::ast::ObjectName;


pub struct SetDefaultSchema {
    schema_name: ObjectName,
}

impl SetDefaultSchema {
    pub fn new(schema_name: ObjectName) -> Self {
        Self { schema_name}
    }

    pub fn execute(&self, datafusion_context: &mut ExecutionContext, global_context: Arc<Mutex<GlobalContext>>, session_context: &mut SessionContext) -> MysqlResult<u64> {
        let full_schema_name = meta_util::fill_up_schema_name(session_context, self.schema_name.clone()).unwrap();

        let schema_ref_map = meta_util::read_all_schema(global_context.clone()).unwrap();
        if !schema_ref_map.contains_key(&full_schema_name) {
            return Err(MysqlError::new_server_error(
                1049,
                "42000",
                format!("Unknown database '{:?}'", self.schema_name.to_string()).as_str(),
            ));
        }

        datafusion_context.change_default_catalog_and_schema(meta_const::CATALOG_NAME.to_string(), self.schema_name.to_string());

        *session_context.current_schema.lock().expect("mutex poisoned") = Some(self.schema_name.to_string());

        Ok(1)
    }
}
