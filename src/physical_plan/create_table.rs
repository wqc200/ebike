use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::{Expr, LogicalPlan};
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{ColumnDef, Ident, ObjectName, SqlOption, TableConstraint, Value};

use crate::core::core_util;
use crate::core::global_context::GlobalContext;
use crate::core::output::CoreOutput;
use crate::core::output::FinalCount;
use crate::core::session_context::SessionContext;
use crate::datafusion_impl::datasource::rocksdb::RocksdbTable;
use crate::meta::{meta_const, meta_util};
use crate::meta::def::TableDef;
use crate::meta::initial::{information_schema, initial_util};
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::physical_plan::insert::PhysicalPlanInsert;

use crate::util;

pub struct CreateTable {
    global_context: Arc<Mutex<GlobalContext>>,
    table_name: ObjectName,
    columns: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    with_options: Vec<SqlOption>,
}

impl CreateTable {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table_name: ObjectName,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
        with_options: Vec<SqlOption>,
    ) -> Self {
        Self {
            global_context,
            table_name,
            columns,
            constraints,
            with_options,
        }
    }

    pub fn execute(&self, execution_context: &mut ExecutionContext, session_context: &mut SessionContext) -> MysqlResult<u64> {
        let full_table_name = meta_util::fill_up_table_name(session_context, self.table_name.clone()).unwrap();

        let catalog_name = meta_util::cut_out_catalog_name(full_table_name.clone());
        let schema_name = meta_util::cut_out_schema_name(full_table_name.clone());
        let table_name = meta_util::cut_out_table_name(full_table_name.clone());

        let mut with_options = self.with_options.clone();
        let has_engine = with_options
            .iter()
            .any(|x| x.name.to_string().to_uppercase() == meta_const::TABLE_OPTION_OF_ENGINE);
        if !has_engine {
            let sql_option = SqlOption { name: Ident { value: meta_const::TABLE_OPTION_OF_ENGINE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_ENGINE_NAME_ROCKSDB.to_string()) };
            with_options.push(sql_option)
        }
        let constraints = self.constraints.clone();

        meta_util::store_add_column_serial_number(self.global_context.clone(), full_table_name.clone(), self.columns.clone());
        meta_util::cache_add_column_serial_number(self.global_context.clone(), full_table_name.clone(), self.columns.clone());

        initial_util::add_information_schema_tables(self.global_context.clone(), full_table_name.clone(), with_options.clone());
        initial_util::add_information_schema_columns(self.global_context.clone(), full_table_name.clone(), self.columns.clone());
        meta_util::save_table_constraint(self.global_context.clone(), full_table_name.clone(), constraints.clone());

        let table_def_map = initial_util::read_all_table(self.global_context.clone()).unwrap();
        let table_def = match table_def_map.get(&full_table_name) {
            None => return Err(MysqlError::new_global_error(1105, format!(
                "Unknown error. An error occurred while get table def from table_def_map, table: {:?}",
                self.table_name.to_string()).as_str())),
            Some(table_def) => table_def.clone()
        };
        self.global_context.lock().unwrap().meta_cache.add_table(full_table_name.clone(), table_def.clone());

        let table_provider = Arc::new(RocksdbTable::new(self.global_context.clone(), table_def.clone(), full_table_name.clone()));
        core_util::register_table(execution_context, catalog_name.to_string().as_str() , schema_name.to_string().as_str(), table_name.to_string().as_str(), table_provider);

        Ok(1)
    }
}
