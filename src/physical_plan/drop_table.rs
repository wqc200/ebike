use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::{Mutex, Arc};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::{Result};
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::{Expr, LogicalPlan};
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{ColumnDef, Ident, ObjectName, SqlOption, TableConstraint, Value};

use crate::core::global_context::GlobalContext;
use crate::core::output::CoreOutput;
use crate::core::output::FinalCount;
use crate::core::session_context::SessionContext;
use crate::datafusion_impl::datasource::rocksdb::RocksdbTable;
use crate::meta::def::information_schema;
use crate::meta::meta_util;
use crate::mysql::error::{MysqlResult, MysqlError};
use crate::physical_plan::insert::PhysicalPlanInsert;
use crate::store::engine::engine_util;

use crate::util;

pub struct DropTable {
    global_context: Arc<Mutex<GlobalContext>>,
    input_table_name: ObjectName,
}

impl DropTable {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        input_table_name: ObjectName,
    ) -> Self {
        Self {
            global_context,
            input_table_name,
        }
    }

    pub fn execute(&self, datafusion_context: &mut ExecutionContext, session_context: &mut SessionContext) -> MysqlResult<u64> {
        let full_table_name = meta_util::fill_up_table_name(session_context, self.input_table_name.clone()).unwrap();

        let table_def = self.global_context.lock().unwrap().meta_cache.get_table(full_table_name.clone()).unwrap().clone();
        let columns = table_def.get_columns().to_vec();

        let column_names = columns.iter().map(|column|column.sql_column.name.clone()).collect::<Vec<_>>();

        meta_util::store_delete_column_serial_number(self.global_context.clone(), full_table_name.clone(), column_names.clone());
        meta_util::cache_delete_column_serial_number(self.global_context.clone(), full_table_name.clone(), column_names.clone());
        meta_util::store_delete_current_serial_number(self.global_context.clone(), full_table_name.clone());
        meta_util::cache_delete_table(self.global_context.clone(), full_table_name.clone());

        // delete rocksdb data dir

        datafusion_context.deregister_table(full_table_name.to_string().as_str());

        Ok(1)
    }
}
