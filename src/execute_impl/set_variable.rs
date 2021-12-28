use std::sync::{Mutex, Arc};

use arrow::array::{StringArray};
use arrow::datatypes::{SchemaRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sqlparser::ast::{Ident};

use crate::core::global_context::GlobalContext;
use crate::mysql::error::{MysqlResult};
use crate::core::output::ResultSet;
use crate::core::session_context::SessionContext;
use datafusion::execution::context::ExecutionContext;

pub struct SetVariable {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl SetVariable {
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

    pub fn execute(&self) -> MysqlResult<u64> {
        Ok(0)
    }
}
