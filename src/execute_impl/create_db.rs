use std::sync::{Arc, Mutex};

use arrow::array::StringArray;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use sqlparser::ast::{Ident, ObjectName};

use crate::core::global_context::GlobalContext;
use crate::core::output::ResultSet;
use crate::core::session_context::SessionContext;
use crate::mysql::error::MysqlResult;
use crate::meta::{meta_util, initial, meta_const};
use crate::core::core_util;

pub struct CreateDb {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl CreateDb {
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

    pub fn execute(&mut self, db_name: ObjectName) -> MysqlResult<u64> {
        let full_schema_name = meta_util::fill_up_schema_name(&mut self.session_context, db_name.clone()).unwrap();

        let db_name = meta_util::cut_out_schema_name(full_schema_name.clone());

        let result = initial::create_schema(self.global_context.clone(), full_schema_name);
        if let Err(e) = result {
            return Err(e);
        }

        core_util::register_schema(execution_context, meta_const::CATALOG_NAME, db_name.to_string().as_str());

        Ok(1)
    }
}
