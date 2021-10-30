use std::sync::{Arc, Mutex};

use datafusion::execution::context::ExecutionContext;
use sqlparser::ast::{AlterTableOperation, ColumnDef};

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
    table: TableDef,
    column: ColumnDef,
}

impl AddColumn {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table: TableDef,
        column: ColumnDef,
    ) -> Self {
        Self {
            global_context,
            table,
            column,
        }
    }

    pub fn execute(&self, datafusion_context: &mut ExecutionContext) -> MysqlResult<u64> {
        let mut sparrow_column_list = vec![];

        let before_sparrow_column = self.table.column.get_last_sparrow_column().unwrap();
        let mut ordinal_position = before_sparrow_column.ordinal_position;
        let mut store_id = self.table.column.get_max_store_id();
        ordinal_position += 1;
        store_id += 1;
        let sparrow_column = SparrowColumnDef::new(store_id, ordinal_position, self.column.clone());
        sparrow_column_list.push(sparrow_column.clone());

        let result = initial::add_information_schema_columns(self.global_context.clone(), self.table.option.clone(), sparrow_column_list);
        if let Err(mysql_error) = result {
            return Err(mysql_error);
        }

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
