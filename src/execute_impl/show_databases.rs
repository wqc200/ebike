use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use sqlparser::ast::{AlterTableOperation, ObjectName, Query, SetExpr, Statement, Ident, SelectItem, OrderByExpr, Select};

use crate::core::core_util;
use crate::core::core_util::{check_table_exists, register_all_table};
use crate::core::global_context::GlobalContext;
use crate::core::output::ResultSet;
use crate::core::session_context::SessionContext;
use crate::execute_impl::select_from::SelectFrom;
use crate::meta::meta_def::{SparrowColumnDef, TableDef};
use crate::meta::meta_util;
use crate::meta::meta_util::load_all_table;
use crate::meta::{initial, meta_const, scalar_value};
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::physical_plan;
use crate::util::convert::ToObjectName;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::collections::HashMap;

pub struct ShowDatabases {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl ShowDatabases {
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

    pub async fn execute(&mut self) -> MysqlResult<ResultSet> {
        let table_name = meta_util::convert_to_object_name(
            meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA,
        );
        let full_table_name = meta_util::fill_up_table_name(
            &mut self.session_context,
            table_name.clone(),
        ).unwrap();

        let projection_column = SQLExpr::Identifier(Ident {
            value:
            meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_SCHEMA_NAME
                .to_string(),
            quote_style: None,
        });
        let projection_column_alias = Ident::new("Database");
        // from
        let table_with_joins =
            core_util::build_table_with_joins(full_table_name.clone());
        // projection
        let projection = vec![SelectItem::ExprWithAlias {
            expr: projection_column,
            alias: projection_column_alias.clone(),
        }];
        // order by
        let order_by_column = SQLExpr::Identifier(projection_column_alias);
        let order_by = OrderByExpr {
            expr: order_by_column,
            asc: None,
            nulls_first: None,
        };

        // select
        let select = Select {
            distinct: false,
            top: None,
            projection,
            from: vec![table_with_joins],
            lateral_views: vec![],
            selection: None,
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
