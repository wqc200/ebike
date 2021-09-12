use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::{Expr, LogicalPlan};
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{AlterTableOperation, ColumnDef, Ident, ObjectName, SqlOption, TableConstraint, Value};

use crate::core::core_util::register_all_table;
use crate::core::global_context::GlobalContext;
use crate::core::output::CoreOutput;
use crate::core::output::FinalCount;
use crate::core::session_context::SessionContext;
use crate::datafusion_impl::datasource::rocksdb::RocksdbTable;
use crate::meta::def::information_schema;
use crate::meta::initial;
use crate::meta::meta_def::{SparrowColumnDef, TableDef};
use crate::meta::meta_util;
use crate::meta::meta_util::load_all_table;
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::physical_plan::insert::PhysicalPlanInsert;
use crate::store::engine::engine_util;
use crate::store::engine::engine_util::TableEngineFactory;
use crate::util;

pub struct AlterTable {
    global_context: Arc<Mutex<GlobalContext>>,
    table: TableDef,
    operation: AlterTableOperation,
}

impl AlterTable {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table: TableDef,
        operation: AlterTableOperation,
    ) -> Self {
        Self {
            global_context,
            table,
            operation,
        }
    }

    pub fn execute(&self, datafusion_context: &mut ExecutionContext, session_context: &mut SessionContext) -> MysqlResult<u64> {
        match self.operation.clone() {
            AlterTableOperation::DropColumn { column_name, .. } => {
                meta_util::cache_add_all_table(self.global_context.clone());
            }
            AlterTableOperation::AddColumn { column_def } => {
                let mut sparrow_column_list = vec![];

                let before_sparrow_column = self.table.column.get_last_sparrow_column().unwrap();
                let mut ordinal_position = before_sparrow_column.ordinal_position;
                let mut store_id = self.table.column.get_max_store_id();
                ordinal_position += 1;
                store_id += 1;
                let sparrow_column = SparrowColumnDef::new(store_id, ordinal_position, column_def);
                sparrow_column_list.push(sparrow_column.clone());

                let result = initial::add_information_schema_columns(self.global_context.clone(), self.table.option.clone(), sparrow_column_list);
                if let Err(mysql_error) = result {
                    return Err(mysql_error);
                }
            }
            _ => {}
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
