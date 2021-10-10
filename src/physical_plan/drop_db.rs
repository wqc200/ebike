use std::sync::{Arc, Mutex};

use sqlparser::ast::{ObjectName};

use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::meta::initial;
use crate::meta::meta_util;
use crate::mysql::error::{MysqlResult};

pub struct DropDB {
    global_context: Arc<Mutex<GlobalContext>>,
    schema_name: ObjectName,
}

impl DropDB {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        schema_name: ObjectName,
    ) -> Self {
        Self {
            global_context,
            schema_name,
        }
    }

    pub fn execute(&self, session_context: &mut SessionContext) -> MysqlResult<u64> {
        let db_name = self.schema_name.clone();
        let full_db_name = meta_util::fill_up_schema_name(session_context, db_name).unwrap();

        let result = initial::delete_db_form_information_schema(self.global_context.clone(), full_db_name);
        if let Err(e) = result {
            return Err(e);
        }

        Ok(1)
    }
}
