use std::borrow::Borrow;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bstr::{ByteSlice, ByteVec};
use datafusion::error::DataFusionError;
use datafusion::execution::context::{ExecutionContext, ExecutionContextState};
use datafusion::logical_plan::{Expr, LogicalPlan, ToDFSchema};
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{Assignment, ColumnDef, ObjectName, SqlOption, TableConstraint, Ident};
use uuid::Uuid;
use sqlparser::ast::{
    BinaryOperator, Expr as SQLExpr, Join, JoinConstraint, JoinOperator,
    Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins, UnaryOperator, Value,
};

use crate::core::{core_util as CoreUtil, core_util};
use crate::core::global_context::GlobalContext;
use crate::core::output::CoreOutput;
use crate::core::output::FinalCount;
use crate::core::session_context::SessionContext;
use crate::meta::def::TableDef;
use crate::meta::meta_util;
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::store::engine::engine_util;

use crate::test;
use crate::util;
use crate::util::convert::ToIdent;
use crate::core::logical_plan::CoreLogicalPlan;
use datafusion::sql::planner::{SqlToRel, ContextProvider};

pub struct Insert {
    global_context: Arc<Mutex<GlobalContext>>,
    table_name: ObjectName,
    columns: Vec<Ident>,
    source: Box<Query>,
}

impl Insert {
    pub fn new(global_context: Arc<Mutex<GlobalContext>>, table_name: ObjectName, columns: Vec<Ident>, source: Box<Query>) -> Self {
        Self {
            global_context,
            table_name,
            columns,
            source,
        }
    }

    pub fn execute<S: ContextProvider>(&self, datafusion_context: &mut ExecutionContext, session_context: &mut SessionContext, query_planner: &SqlToRel<S>) -> MysqlResult<CoreLogicalPlan> {
        let full_table_name = meta_util::fill_up_table_name(session_context, self.table_name.clone()).unwrap();

        let table_map = self.global_context.lock().unwrap().meta_cache.get_table_map();
        if !table_map.contains_key(&full_table_name) {
            let message = format!("Table '{}' doesn't exist", self.table_name.to_string());
            log::error!("{}", message);
            return Err(MysqlError::new_server_error(
                1146,
                "42S02",
                message.as_str(),
            ));
        }

        let table_def = self.global_context.lock().unwrap().meta_cache.get_table(full_table_name.clone()).unwrap().clone();

        let mut column_values_list = vec![];
        match &self.source.body {
            SetExpr::Values(values) => {
                for row_value_ast in &values.0 {
                    let mut row_value: Vec<Expr> = vec![];
                    for column_value_ast in row_value_ast {
                        let result = query_planner.sql_expr_to_logical_expr(&column_value_ast, &table_def.to_dfschema().unwrap());
                        let expr = match result {
                            Ok(v) => v,
                            Err(e) => {
                                let message = e.to_string();
                                log::error!("{}", message);
                                return Err(MysqlError::new_server_error(
                                    1305,
                                    "42000",
                                    message.as_str(),
                                ));
                            }
                        };
                        row_value.push(expr)
                    }
                    column_values_list.push(row_value);
                }
            }
            _ => {}
        }

        let mut column_name_list = vec![];
        if self.columns.len() < 1 {
            for column_def in table_def.get_columns() {
                column_name_list.push(column_def.sql_column.name.to_string())
            };
        } else {
            for column in self.columns {
                column_name_list.push(column.to_string())
            };
        }

        let schema = Schema::empty();
        let batch = RecordBatch::new_empty(Arc::new(schema.clone()));

        let dfschema = schema.clone().to_dfschema().unwrap();

        let mut state = datafusion_context.state.lock().unwrap();
        let planner = DefaultPhysicalPlanner::default();

        let mut column_value_map_list = vec![];
        for (row_index, column_values) in column_values_list.iter().enumerate() {
            let mut column_value_map = HashMap::new();
            for (column_index, value) in column_values.iter().enumerate() {
                let result = column_name_list.get(column_index);
                let column_name = match result {
                    None => {
                        return Err(MysqlError::new_global_error(1105, format!(
                            "Column name not found, row_index: {:?}, column_index: {:?}",
                            row_index,
                            column_index,
                        ).as_str()));
                    }
                    Some(column_name) => column_name.to_ident(),
                };
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
                        column_value_map.insert(column_name, v);
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

            column_value_map_list.push(column_value_map);
        }

        let result = engine_util::TableEngineFactory::try_new_with_table(self.global_context.clone(), full_table_name.clone(), table_def.clone());
        let engine = match result {
            Ok(engine) => engine,
            Err(mysql_error) => return Err(mysql_error),
        };

        let table_name = meta_util::cut_out_table_name(full_table_name.clone()).to_string();

        let all_table_index = meta_util::get_all_table_index(self.global_context.clone(), full_table_name.clone()).unwrap();

        let serial_number_map = self.global_context.lock().unwrap().meta_cache.get_serial_number_map(full_table_name.clone()).unwrap();

        let mut index_keys_list = vec![];
        for row_index in 0..column_value_map_list.len() {
            let column_value_map = column_value_map_list[row_index].clone();

            let mut index_keys = vec![];
            for (index_name, level, index_column_names) in all_table_index.clone() {
                let serial_number_value_vec = engine_util::build_column_serial_number_value(index_column_names.clone(), serial_number_map.clone(), column_value_map.clone()).unwrap();
                let result = util::dbkey::create_index(self.full_table_name.clone(), index_name.as_str(), serial_number_value_vec);
                let index_key = match result {
                    Ok(index_key) => index_key,
                    Err(mysql_error) => return Err(mysql_error)
                };
                index_keys.push((index_name.clone(), level, index_key.clone()));
            }

            index_keys_list.push(index_keys);
        }

        for index_keys in index_keys_list.clone() {
            for (index_name, level, index_key) in index_keys {
                if level == 1 || level == 2 {
                    match engine.get_key(index_key.clone()).unwrap() {
                        None => {}
                        Some(_) => {
                            if self.add_entry_type == engine_util::ADD_ENTRY_TYPE::INSERT {
                                return Err(MysqlError::new_server_error(
                                    1062,
                                    "23000",
                                    format!(
                                        "Duplicate entry '{:?}' for key '{:?}.{:?}'",
                                        index_key,
                                        table_name,
                                        index_name,
                                    ).as_str(),
                                ));
                            }
                        }
                    }
                }
            }
        }

        Ok(CoreLogicalPlan::Insert { full_table_name, table_def, column_name_list, index_keys_list, column_value_map_list })
    }
}
