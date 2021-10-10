use std::sync::{Mutex, Arc};

use sqlparser::ast::{ObjectName};

use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::meta::{meta_util};
use crate::mysql::error::{MysqlResult};

use crate::meta::meta_def::TableDef;

pub struct ComFieldList {
    global_context: Arc<Mutex<GlobalContext>>,
    table_name: ObjectName,
}

impl ComFieldList {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table_name: ObjectName,
    ) -> Self {
        Self {
            global_context,
            table_name,
        }
    }

    pub fn execute(&self, session_context: &mut SessionContext) -> MysqlResult<(ObjectName, ObjectName, TableDef)> {
        let full_table_name = meta_util::fill_up_table_name(session_context, self.table_name.clone()).unwrap();

        let schema_name = meta_util::cut_out_schema_name(full_table_name.clone());
        let table_name = meta_util::cut_out_table_name(full_table_name.clone());

        let table_def = self.global_context.lock().unwrap().meta_data.get_table(full_table_name.clone()).unwrap().clone();
        Ok((schema_name, table_name, table_def))
    }
}
