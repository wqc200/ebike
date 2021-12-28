use std::sync::{Arc, Mutex};

use arrow::array::StringArray;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;

use crate::core::global_context::GlobalContext;
use crate::core::output::ResultSet;
use crate::core::session_context::SessionContext;
use crate::meta::meta_def::TableDef;
use crate::mysql::error::MysqlResult;
use crate::meta::meta_util;

pub struct ComFieldList {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl ComFieldList {
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

    pub fn execute(
        &mut self,
        table_name: ObjectName,
    ) -> MysqlResult<(ObjectName, ObjectName, TableDef)> {
        let full_table_name =
            meta_util::fill_up_table_name(&mut self.session_context, table_name.clone()).unwrap();

        let schema_name = meta_util::cut_out_schema_name(full_table_name.clone());
        let table_name = meta_util::cut_out_table_name(full_table_name.clone());

        let result = meta_util::get_table(self.global_context.clone(), full_table_name.clone());
        let table_def = match result {
            Ok(table_def) => table_def.clone(),
            Err(mysql_error) => {
                return Err(mysql_error)
            },
        };

        Ok((schema_name, table_name, table_def))
    }
}
