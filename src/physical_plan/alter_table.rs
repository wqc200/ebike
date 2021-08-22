use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::{Expr, LogicalPlan};
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{AlterTableOperation, ColumnDef, Ident, ObjectName, SqlOption, TableConstraint, Value};

use crate::core::global_context::GlobalContext;
use crate::core::output::CoreOutput;
use crate::core::output::FinalCount;
use crate::core::session_context::SessionContext;
use crate::datafusion_impl::datasource::rocksdb::RocksdbTable;
use crate::meta::initial::{information_schema, initial_util};
use crate::meta::meta_util;
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::physical_plan::insert::PhysicalPlanInsert;
use crate::store::engine::engine_util;
use crate::store::engine::sled::SledOperator;
use crate::util;

pub struct AlterTable {
    global_context: Arc<Mutex<GlobalContext>>,
    table_name: ObjectName,
    operation: AlterTableOperation,
}

impl AlterTable {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table_name: ObjectName,
        operation: AlterTableOperation,
    ) -> Self {
        Self {
            global_context,
            table_name,
            operation,
        }
    }

    pub fn execute(&self, datafusion_context: &mut ExecutionContext, session_context: &mut SessionContext) -> MysqlResult<u64> {
        let full_table_name = meta_util::fill_up_table_name(session_context, self.table_name.clone()).unwrap();

        let catalog_name = meta_util::cut_out_catalog_name(full_table_name.clone());
        let schema_name = meta_util::cut_out_schema_name(full_table_name.clone());
        let table_name = meta_util::cut_out_table_name(full_table_name.clone());

        match self.operation.clone() {
            AlterTableOperation::DropColumn { column_name, .. } => {
                meta_util::store_delete_column_serial_number(self.global_context.clone(), full_table_name.clone(), vec![column_name.clone()]);
                meta_util::cache_delete_column_serial_number(self.global_context.clone(), full_table_name.clone(), vec![column_name.clone()]);

                meta_util::cache_add_all_table(self.global_context.clone());
            }
            AlterTableOperation::AddColumn { column_def } => {
                meta_util::store_add_column_serial_number(self.global_context.clone(), full_table_name.clone(), vec![column_def.clone()]);
                meta_util::cache_add_column_serial_number(self.global_context.clone(), full_table_name.clone(), vec![column_def.clone()]);

                let result = initial_util::add_information_schema_columns(self.global_context.clone(), full_table_name.clone(), vec![column_def.clone()]);
                if let Err(mysql_error) = result {
                    return Err(mysql_error)
                }

                let mut table = self.global_context.lock().unwrap().meta_cache.get_table(full_table_name.clone()).unwrap();
                table.add_sqlcolumn(column_def.clone());
                self.global_context.lock().unwrap().meta_cache.add_table(full_table_name.clone(), table);
            }
            _ => {}
        }

        let result = self.global_context.lock().unwrap().meta_cache.get_table(full_table_name.clone());
        match result {
            Some(table_def) => {
                let engine = engine_util::EngineFactory::try_new_with_table_name(self.global_context.clone(), self.table_name.clone(), table_def.clone());
                let provider = match engine {
                    Ok(engine) => engine.table_provider(),
                    Err(mysql_error) => return Err(mysql_error),
                };
                datafusion_context.register_table(full_table_name.to_string().as_str(), provider);
            }
            None => {}
        }

        Ok(1)
    }
}
