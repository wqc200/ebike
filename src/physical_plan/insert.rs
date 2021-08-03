use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::execution::context::{ExecutionContext, ExecutionContextState};
use datafusion::logical_plan::{LogicalPlan, Expr, ToDFSchema};
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use sqlparser::ast::{Assignment, ColumnDef, ObjectName, SqlOption, TableConstraint};

use crate::core::global_context::GlobalContext;
use crate::core::output::CoreOutput;
use crate::core::output::FinalCount;
use crate::core::core_util as CoreUtil;
use crate::meta::meta_util;
use crate::mysql::error::{MysqlResult, MysqlError};
use crate::util;
use crate::store::engine::sled::SledOperator;
use crate::test;

use crate::store::engine::engine_util;
use crate::core::session_context::SessionContext;
use arrow::record_batch::RecordBatch;
use std::borrow::Borrow;
use datafusion::physical_plan::{PhysicalExpr, ColumnarValue};
use datafusion::error::DataFusionError;
use crate::meta::def::TableDef;

pub struct Insert {
    core_context: Arc<Mutex<GlobalContext>>,
    table_name: ObjectName,
    table_schema: TableDef,
    column_name: Vec<String>,
    column_value: Vec<Vec<Expr>>,
}

impl Insert {
    pub fn new(core_context: Arc<Mutex<GlobalContext>>, table_name: ObjectName, table_schema: TableDef, column_name: Vec<String>, column_value: Vec<Vec<Expr>>) -> Self {
        Self {
            core_context,
            table_name,
            table_schema,
            column_name,
            column_value,
        }
    }

    pub fn execute(&self, datafusion_context: &mut ExecutionContext, session_context: &mut SessionContext) -> MysqlResult<u64> {
        let full_table_name = meta_util::fill_up_table_name(session_context, self.table_name.clone()).unwrap();

        let schema = Schema::empty();
        let batch = RecordBatch::new_empty(Arc::new(schema.clone()));

        let dfschema = schema.clone().to_dfschema().unwrap();

        let mut state = datafusion_context.state.lock().unwrap();
        let planner = DefaultPhysicalPlanner::default();

        let mut new_rows = vec![];
        for (row_index, row) in self.column_value.iter().enumerate() {
            let mut new_row = vec![];
            for (column_index, value) in row.iter().enumerate() {
                let result = planner.create_physical_expr(value, &dfschema, &schema, &state);
                let physical_expr = match result {
                    Ok(v) => v,
                    Err(e) => {
                        return Err(MysqlError::from(e));
                    }
                };
                let result = physical_expr.evaluate(&batch);
                let columnar_value = match result {
                    Ok(v) => v,
                    Err(e) => {
                        return Err(MysqlError::from(e));
                    }
                };
                match columnar_value {
                    ColumnarValue::Scalar(v) => {
                        new_row.push(v)
                    }
                    _ => {
                        return Err(MysqlError::new_global_error(1105, format!(
                            "Value is not a scalar value. An error occurred while evaluate the value, row_index: {:?}, column_index: {:?}",
                            row_index,
                            column_index,
                        ).as_str()));
                    }
                }
            }

            new_rows.push(new_row);
        }

        let engine = engine_util::EngineFactory::try_new(self.core_context.clone(), full_table_name.clone(), self.table_schema.clone());
        match engine {
            Ok(engine) => {
                engine.insert(self.column_name.clone(), new_rows)
            }
            Err(mysql_error) => Err(mysql_error),
        }
    }
}
