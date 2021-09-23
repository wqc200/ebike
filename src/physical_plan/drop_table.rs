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
use crate::meta::def::information_schema;
use crate::meta::meta_util;
use crate::mysql::error::{MysqlResult, MysqlError};
use crate::physical_plan::insert::PhysicalPlanInsert;
use crate::store::engine::engine_util;

use crate::util;
use crate::meta::meta_def::TableDef;

pub struct PhysicalPlanDropTable {
    global_context: Arc<Mutex<GlobalContext>>,
    table: TableDef,
}

impl PhysicalPlanDropTable {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table: TableDef,
    ) -> Self {
        Self {
            global_context,
            table,
        }
    }

    pub fn execute(&self, datafusion_context: &mut ExecutionContext, session_context: &mut SessionContext) -> MysqlResult<u64> {
        let mut gc = self.global_context.lock().unwrap();

        let full_table_name = self.table.option.full_table_name.clone();
        gc.meta_data.delete_table(full_table_name.clone());

        datafusion_context.deregister_table(full_table_name.to_string().as_str());

        Ok(1)
    }
}
