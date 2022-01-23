use std::sync::{Arc, Mutex};

use arrow::array::StringArray;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use sqlparser::ast::{Ident, ColumnDef, DataType as SQLDataType};
use sqlparser::ast::ObjectName;

use crate::core::global_context::GlobalContext;
use crate::core::output::{ResultSet, CoreOutput, StmtPrepare};
use crate::core::session_context::SessionContext;
use crate::meta::meta_def::{TableDef, SparrowColumnDef};
use crate::mysql::error::MysqlResult;
use crate::meta::meta_util;
use crate::mysql::metadata::Column;
use crate::util::convert::ToObjectName;

pub struct ComStmtPrepare {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl ComStmtPrepare {
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

    pub async fn execute(
        &mut self,
    ) -> MysqlResult<StmtPrepare> {
        let schema_name = "a".to_object_name();
        let table_name = "b".to_object_name();

        let column_def = ColumnDef {
            name: "name".into(),
            data_type: SQLDataType::Char(Some(100)),
            collation: None,
            options: vec![],
        };
        let sparrow_column_def = SparrowColumnDef::new(1, 1, column_def);

        let column = Column::new(schema_name.clone(), table_name.clone(), &sparrow_column_def);

        let stmt_prepare = StmtPrepare::new(1, vec![], vec![column.clone(), column.clone()]);

        Ok(stmt_prepare)
    }
}
