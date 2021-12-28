use std::sync::{Arc, Mutex};

use arrow::array::StringArray;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use sqlparser::ast::{Ident, ObjectName, TableConstraint, SqlOption, ColumnDef};

use crate::core::global_context::GlobalContext;
use crate::core::output::ResultSet;
use crate::core::session_context::SessionContext;
use crate::mysql::error::MysqlResult;
use crate::meta::{meta_util, initial, meta_const};
use crate::core::core_util;
use crate::meta::meta_def::{SparrowColumnDef, TableOptionDef};
use crate::meta::meta_util::load_all_table;
use crate::core::core_util::register_all_table;

pub struct CreateTable {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl CreateTable {
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

    pub fn execute(&mut self, table_name: ObjectName,
                   sql_column_list: Vec<ColumnDef>,
                   constraints: Vec<TableConstraint>,
                   table_options: Vec<SqlOption>) -> MysqlResult<u64> {
        let full_table_name = meta_util::fill_up_table_name(&mut self.session_context, table_name.clone()).unwrap();

        let catalog_name = meta_util::cut_out_catalog_name(full_table_name.clone());
        let schema_name = meta_util::cut_out_schema_name(full_table_name.clone());
        let table_name = meta_util::cut_out_table_name(full_table_name.clone());

        let mut sparrow_column_list = vec![];
        let mut ordinal_position = 0;
        let mut store_id = 0;
        for sql_column in sql_column_list.clone() {
            ordinal_position += 1;
            store_id += 1;

            let sparrow_column = SparrowColumnDef::new(store_id, ordinal_position, sql_column);
            sparrow_column_list.push(sparrow_column.clone());
        }
        let column_store_id = store_id;

        let mut table_option = TableOptionDef::new(catalog_name.to_string().as_str(), schema_name.to_string().as_str(), table_name.to_string().as_str());
        table_option.load_table_options(table_options.clone());
        table_option.with_table_type(meta_const::VALUE_OF_TABLE_OPTION_TABLE_TYPE_BASE_TABLE);
        table_option.with_column_max_store_id(column_store_id);
        if table_option.engine.is_empty() {
            let mutex_guard_global_context = self.global_context.lock().unwrap();
            table_option.with_engine(mutex_guard_global_context.my_config.server.engines.first().unwrap())
        }

        let result = initial::add_information_schema_columns(self.global_context.clone(), table_option.clone(), sparrow_column_list);
        if let Err(e) = result {
            return Err(e);
        }

        let result = meta_util::save_table_constraint(self.global_context.clone(), table_option.clone(), constraints.clone());
        if let Err(mysql_error) = result {
            return Err(mysql_error);
        }

        let result = initial::add_information_schema_tables(self.global_context.clone(), table_option.clone());
        if let Err(e) = result {
            return Err(e);
        }

        let result = load_all_table(self.global_context.clone());
        if let Err(mysql_error) = result {
            return Err(mysql_error);
        }

        let result = register_all_table(self.global_context.clone(), &mut self.execution_context);
        if let Err(mysql_error) = result {
            return Err(mysql_error);
        }

        Ok(1)
    }
}
