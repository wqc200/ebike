use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use sqlparser::ast::{
    AlterTableOperation, Assignment, BinaryOperator, ColumnDef, Expr as SQLExpr, Ident,
    JoinConstraint, JoinOperator, ObjectName, ObjectType, Query, Select, SelectItem, SetExpr,
    ShowStatementFilter, Statement as SQLStatement, TableFactor, Value,
};

use crate::core::core_util;
use crate::core::core_util::{check_table_exists, register_all_table};
use crate::core::global_context::GlobalContext;
use crate::core::output::ResultSet;
use crate::core::session_context::SessionContext;
use crate::execute_impl::select::SelectFrom;
use crate::meta::meta_def::{SparrowColumnDef, TableDef};
use crate::meta::meta_util;
use crate::meta::meta_util::load_all_table;
use crate::meta::{initial, meta_const, scalar_value};
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::util::convert::ToObjectName;

pub struct ShowTableStatus {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl ShowTableStatus {
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

    pub async fn execute(&mut self, db_name: ObjectName) -> MysqlResult<ResultSet> {
        // table
        let full_table_name = meta_util::convert_to_object_name(
            meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES,
        );
        let table_with_joins = core_util::build_table_with_joins(full_table_name.clone());
        // projection
        let projection = vec![SelectItem::Wildcard];
        // selection
        let selection = Some(SQLExpr::BinaryOp {
            left: Box::new(SQLExpr::Identifier(Ident::new(
                meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_SCHEMA,
            ))),
            op: BinaryOperator::Eq,
            right: Box::new(SQLExpr::Value(Value::SingleQuotedString(
                db_name.to_string(),
            ))),
        });

        // select
        let select = Select {
            distinct: false,
            top: None,
            projection,
            from: vec![table_with_joins],
            lateral_views: vec![],
            selection,
            group_by: vec![],
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
        };

        let query = Box::new(Query {
            with: None,
            body: SetExpr::Select(Box::new(select)),
            order_by: vec![order_by],
            limit: None,
            offset: None,
            fetch: None,
        });
        let mut select_from = SelectFrom::new(
            self.global_context.clone(),
            self.session_context.clone(),
            self.execution_context.clone(),
        );
        select_from.execute(&query).await
    }
}
