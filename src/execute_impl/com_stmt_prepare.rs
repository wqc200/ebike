use std::sync::{Arc, Mutex};

use arrow::array::StringArray;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use datafusion::sql::parser::Statement as DFStatement;
use sqlparser::ast::{ColumnDef, DataType as SQLDataType, Ident};
use sqlparser::ast::{ObjectName, SetExpr, Statement as SQLStatement, Value, Expr as SQLExpr};

use crate::core::core_def::StmtCacheDef;
use crate::core::global_context::GlobalContext;
use crate::core::output::{CoreOutput, ResultSet, StmtPrepare};
use crate::core::session_context::SessionContext;
use crate::core::stmt_context::StmtContext;
use crate::meta::meta_def::{SparrowColumnDef, TableDef};
use crate::meta::meta_util;
use crate::mysql::error::MysqlResult;
use crate::mysql::metadata::Column;
use crate::util::convert::ToObjectName;

pub struct ComStmtPrepare {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
    stmt_context: StmtContext,
}

impl ComStmtPrepare {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        session_context: SessionContext,
        execution_context: ExecutionContext,
        stmt_context: StmtContext,
    ) -> Self {
        Self {
            global_context,
            session_context,
            execution_context,
            stmt_context,
        }
    }

    pub async fn execute(&mut self, stmt_context: &mut StmtContext, df_statements: Vec<DFStatement>) -> MysqlResult<StmtPrepare> {
        let stmt_cache = StmtCacheDef::new(df_statements.clone());
        stmt_context.add_stmt(stmt_cache);
        let stmt_id = stmt_context.stmt_id;

        let schema_name = "schema".to_object_name();
        let table_name = "table".to_object_name();

        let mut params = vec![];

        for sql_statement in df_statements.clone() {
            match sql_statement.clone() {
                DFStatement::Statement(statement) => match statement {
                    SQLStatement::Insert { source, .. } => match &source.body {
                        SetExpr::Values(values) => {
                            for row in values.0.iter() {
                                for v in row {
                                    match v {
                                        SQLExpr::Value(Value::OnlyString(_)) => {
                                            let column_def = ColumnDef {
                                                name: "name".into(),
                                                data_type: SQLDataType::Char(Some(100)),
                                                collation: None,
                                                options: vec![],
                                            };
                                            let sparrow_column_def =
                                                SparrowColumnDef::new(1, 1, column_def);
                                            let column = Column::new(
                                                schema_name.clone(),
                                                table_name.clone(),
                                                &sparrow_column_def,
                                            );
                                            params.push(column);
                                        }
                                        _ => {},
                                    }
                                }
                            }
                        }
                        _ => {}
                    },
                    _ => {}
                },
                _ => {}
            };
        }

        let stmt_prepare = StmtPrepare::new(stmt_id, vec![], params);

        Ok(stmt_prepare)
    }
}
