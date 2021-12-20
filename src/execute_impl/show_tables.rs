use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use sqlparser::ast::{AlterTableOperation, ObjectName, Query, SetExpr, Statement, Ident, SelectItem, OrderByExpr, Select, BinaryOperator, Value};

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
use crate::physical_plan;
use crate::util::convert::ToObjectName;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::collections::HashMap;

pub struct ShowTables {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl ShowTables {
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

    pub async fn execute(&mut self, full: bool, db_name: Option<ObjectName>) -> MysqlResult<ResultSet> {
        let db_name = match db_name {
            None => {
                let captured_name =
                    core_util::captured_name(self.session_context.current_schema.clone());
                let schema_name = match captured_name {
                    Some(schema_name) => schema_name.clone(),
                    None => {
                        return Err(MysqlError::new_server_error(
                            1046,
                            "3D000",
                            "No database selected",
                        ));
                    }
                };
                schema_name
            }
            Some(db_name) => {
                let full_schema_name = meta_util::fill_up_schema_name(
                    &mut self.session_context,
                    db_name.clone(),
                )
                    .unwrap();

                let db_map =
                    meta_util::read_all_schema(self.global_context.clone()).unwrap();
                if !db_map.contains_key(&full_schema_name) {
                    return Err(MysqlError::new_server_error(
                        1049,
                        "42000",
                        format!("Unknown database '{}'", db_name.to_string()).as_str(),
                    ));
                }

                db_name.to_string()
            }
        };

        let column_expr = SQLExpr::Identifier(Ident {
            value: meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_NAME
                .to_string(),
            quote_style: None,
        });
        let column_alias = Ident::new(format!("Tables_in_{}", db_name.as_str()));
        // table
        let full_table_name = meta_util::convert_to_object_name(
            meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES,
        );
        let table_with_joins = core_util::build_table_with_joins(full_table_name.clone());
        // projection
        let mut projection = vec![SelectItem::ExprWithAlias {
            expr: column_expr,
            alias: column_alias.clone(),
        }];
        if full {
            let column_expr = SQLExpr::Identifier(Ident {
                value: meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_TYPE
                    .to_string(),
                quote_style: None,
            });
            let column_alias = Ident::new("Table_type");
            projection.push(SelectItem::ExprWithAlias {
                expr: column_expr,
                alias: column_alias.clone(),
            });
        }
        // selection
        let selection = SQLExpr::BinaryOp {
            left: Box::new(SQLExpr::Identifier(Ident::new(
                meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_SCHEMA,
            ))),
            op: BinaryOperator::Eq,
            right: Box::new(SQLExpr::Value(Value::SingleQuotedString(
                db_name.to_string(),
            ))),
        };
        // order by
        let order_by_column = SQLExpr::Identifier(column_alias);
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
