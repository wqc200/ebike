use std::sync::{Arc, Mutex};

use datafusion::execution::context::ExecutionContext;
use sqlparser::ast::{ColumnDef as SQLColumnDef, ObjectName, SqlOption, TableConstraint};

use crate::core::core_util::register_all_table;
use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::meta::{meta_const, meta_util};
use crate::meta::initial;
use crate::meta::meta_def::{SparrowColumnDef, TableOptionDef};
use crate::meta::meta_util::load_all_table;
use crate::mysql::error::{MysqlResult};

pub struct CreateTable {
    global_context: Arc<Mutex<GlobalContext>>,
    table_name: ObjectName,
    sql_column_list: Vec<SQLColumnDef>,
    constraints: Vec<TableConstraint>,
    table_options: Vec<SqlOption>,
}

impl CreateTable {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table_name: ObjectName,
        sql_column_list: Vec<SQLColumnDef>,
        constraints: Vec<TableConstraint>,
        table_options: Vec<SqlOption>,
    ) -> Self {
        Self {
            global_context,
            table_name,
            sql_column_list,
            constraints,
            table_options,
        }
    }

    pub fn execute(&self, datafusion_context: &mut ExecutionContext, session_context: &mut SessionContext) -> MysqlResult<u64> {
        let full_table_name = meta_util::fill_up_table_name(session_context, self.table_name.clone()).unwrap();

        let catalog_name = meta_util::cut_out_catalog_name(full_table_name.clone());
        let schema_name = meta_util::cut_out_schema_name(full_table_name.clone());
        let table_name = meta_util::cut_out_table_name(full_table_name.clone());

        let mut sparrow_column_list = vec![];
        let mut ordinal_position = 0;
        let mut store_id = 0;
        for sql_column in self.sql_column_list.clone() {
            ordinal_position += 1;
            store_id += 1;

            let sparrow_column = SparrowColumnDef::new(store_id, ordinal_position, sql_column);
            sparrow_column_list.push(sparrow_column.clone());
        }
        let column_store_id = store_id;

        let mut table_option = TableOptionDef::new(catalog_name.to_string().as_str(), schema_name.to_string().as_str(), table_name.to_string().as_str());
        table_option.load_table_options(self.table_options.clone());
        table_option.with_table_type(meta_const::VALUE_OF_TABLE_OPTION_TABLE_TYPE_BASE_TABLE);
        table_option.with_column_max_store_id(column_store_id);
        if table_option.engine.is_empty() {
            let mutex_guard_global_context = self.global_context.lock().unwrap();
            table_option.with_engine(mutex_guard_global_context.my_config.server.engines.first().unwrap())
        }

        let result = initial::add_information_schema_tables(self.global_context.clone(), table_option.clone());
        if let Err(e) = result {
            return Err(e);
        }

        let result = initial::add_information_schema_columns(self.global_context.clone(), table_option.clone(), sparrow_column_list);
        if let Err(e) = result {
            return Err(e);
        }

        meta_util::save_table_constraint(self.global_context.clone(), table_option.clone(), self.constraints.clone());

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
