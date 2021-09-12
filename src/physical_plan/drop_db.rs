use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::Result;
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
use crate::meta::initial;
use crate::meta::meta_util;
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::physical_plan::insert::PhysicalPlanInsert;
use crate::util;

pub struct DropDB {
    global_context: Arc<Mutex<GlobalContext>>,
    schema_name: ObjectName,
}

impl DropDB {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        schema_name: ObjectName,
    ) -> Self {
        Self {
            global_context,
            schema_name,
        }
    }

    pub fn execute(&self, session_context: &mut SessionContext) -> MysqlResult<u64> {
        let db_name = self.schema_name.clone();
        let full_db_name = meta_util::fill_up_schema_name(session_context, db_name).unwrap();

        initial::delete_db_form_information_schema(self.global_context.clone(), full_db_name);

        Ok(1)
    }
}
