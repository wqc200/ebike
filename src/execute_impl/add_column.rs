use std::sync::{Arc, Mutex};

use datafusion::execution::context::ExecutionContext;
use sqlparser::ast::{AlterTableOperation, ColumnDef, ObjectName};

use crate::core::core_util::register_all_table;
use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::meta::initial;
use crate::meta::meta_def::{SparrowColumnDef, TableDef};
use crate::meta::meta_util;
use crate::meta::meta_util::load_all_table;
use crate::mysql::error::{MysqlResult};

pub struct AddColumn {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl AddColumn {
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

    pub fn execute(&mut self, table_name: ObjectName, column_def: ColumnDef) -> MysqlResult<u64> {
        let full_table_name =
            meta_util::fill_up_table_name(&mut self.session_context, table_name.clone()).unwrap();

        let table_map = self
            .global_context
            .lock()
            .unwrap()
            .meta_data
            .get_table_map();
        let table_def = match table_map.get(&full_table_name) {
            None => {
                return Err(meta_util::error_of_table_doesnt_exists(
                    full_table_name.clone(),
                ))
            }
            Some(table_def) => table_def.clone(),
        };

        let mut sparrow_column_list = vec![];

        let before_sparrow_column = table_def.column.get_last_sparrow_column().unwrap();
        let mut ordinal_position = before_sparrow_column.ordinal_position;
        let mut store_id = table_def.column.get_max_store_id();
        ordinal_position += 1;
        store_id += 1;
        let sparrow_column = SparrowColumnDef::new(store_id, ordinal_position, column_def.clone());
        sparrow_column_list.push(sparrow_column.clone());

        let result = initial::add_information_schema_columns(self.global_context.clone(), table_def.option.clone(), sparrow_column_list);
        if let Err(mysql_error) = result {
            return Err(mysql_error);
        }

        let result = load_all_table(self.global_context.clone());
        if let Err(mysql_error) = result {
            return Err(mysql_error);
        }

        let result = register_all_table(self.global_context.clone(), &mut self.execution_context.clone());
        if let Err(mysql_error) = result {
            return Err(mysql_error);
        }

        Ok(1)
    }
}
