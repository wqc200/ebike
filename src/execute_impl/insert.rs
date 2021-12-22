use bytes::{Buf, Bytes};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::{Expr, ToDFSchema};
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use sqlparser::ast::{Ident, ObjectName};
use sqlparser::ast::{Query, SetExpr};
use uuid::Uuid;

use crate::core::core_util;
use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::meta::meta_def::{IndexDef, TableDef};
use crate::meta::meta_util;
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::store::engine::engine_util::StoreEngineFactory;
use crate::util::convert::ToIdent;
use crate::util::dbkey::{create_table_index_key, create_column_key, create_column_rowid_key};

pub struct Insert {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl Insert {
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
        origin_table_name: ObjectName,
        columns: Vec<Ident>,
        overwrite: bool,
        source: Box<Query>,
    ) -> MysqlResult<u64> {
        let full_table_name =
            meta_util::fill_up_table_name(&mut self.session_context, origin_table_name.clone()).unwrap();

        let table =
            meta_util::get_table(self.global_context.clone(), full_table_name.clone()).unwrap();

        let catalog_name = table.option.catalog_name.to_string();
        let schema_name = table.option.schema_name.to_string();
        let table_name = table.option.table_name.to_string();

        let store_engine = StoreEngineFactory::try_new_with_table_name(
            self.global_context.clone(),
            full_table_name.clone(),
        )
        .unwrap();

        let mut column_values_list = vec![];
        match &source.body {
            SetExpr::Values(values) => {
                for row_value_ast in &values.0 {
                    let mut row_value: Vec<Expr> = vec![];
                    for column_value_ast in row_value_ast {
                        let datafusion_dfschema = table.to_datafusion_dfschema().unwrap();
                        let result = query_planner
                            .sql_expr_to_logical_expr(&column_value_ast, &datafusion_dfschema);
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

        let mut column_name_list: Vec<String> = vec![];
        if columns.len() < 1 {
            for column_def in table.get_columns() {
                column_name_list.push(column_def.sql_column.name.to_string())
            }
        } else {
            for column in &columns {
                column_name_list.push(column.to_string())
            }
        }

        let schema = Schema::empty();
        let batch = RecordBatch::new_empty(Arc::new(schema.clone()));

        let dfschema = schema.clone().to_dfschema().unwrap();

        let state = datafusion_context.state.lock().unwrap();
        let planner = DefaultPhysicalPlanner::default();

        let mut column_value_map_list = vec![];
        for (row_index, column_values) in column_values_list.iter().enumerate() {
            let mut column_value_map = HashMap::new();
            for (column_index, value) in column_values.iter().enumerate() {
                let result = column_name_list.get(column_index);
                let column_name = match result {
                    None => {
                        return Err(MysqlError::new_global_error(
                            1105,
                            format!(
                                "Column name not found, row_index: {:?}, column_index: {:?}",
                                row_index, column_index,
                            )
                            .as_str(),
                        ));
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

        let table_index_list =
            meta_util::get_table_index_list(self.global_context.clone(), full_table_name.clone())
                .unwrap();

        let mut index_keys_list = vec![];
        for row_number in 0..column_value_map_list.len() {
            let column_value_map = column_value_map_list[row_number].clone();

            let mut index_keys = vec![];
            for table_index in table_index_list.clone() {
                let index_key = create_table_index_key(
                    table.clone(),
                    table_index.clone(),
                    column_value_map.clone(),
                )
                .unwrap();
                let index = IndexDef::new(
                    table_index.index_name.as_str(),
                    table_index.level,
                    index_key.as_str(),
                );
                index_keys.push(index);
            }

            index_keys_list.push(index_keys);
        }

        for index_keys in index_keys_list.clone() {
            for row_index in index_keys {
                if row_index.level == 1 || row_index.level == 2 {
                    match store_engine.get_key(row_index.index_key.clone()).unwrap() {
                        None => {}
                        Some(_) => {
                            if !overwrite {
                                return Err(MysqlError::new_server_error(
                                    1062,
                                    "23000",
                                    format!(
                                        "Duplicate entry '{:?}' for key '{:?}.{:?}'",
                                        row_index.index_key,
                                        table_name.clone(),
                                        row_index.index_name,
                                    )
                                    .as_str(),
                                ));
                            }
                        }
                    }
                }
            }
        }

        let store_engine =
            StoreEngineFactory::try_new_with_table(self.global_context.clone(), table.clone())
                .unwrap();

        for row_number in 0..self.column_value_map_list.len() {
            let rowid = Uuid::new_v4()
                .to_simple()
                .encode_lower(&mut Uuid::encode_buffer())
                .to_string();
            let column_value_map = self.column_value_map_list[row_number].clone();

            let column_rowid_key = create_column_rowid_key(
                self.table.option.full_table_name.clone(),
                rowid.as_str(),
            );
            log::debug!("rowid_key: {:?}", column_rowid_key);
            let result = store_engine.put_key(column_rowid_key, rowid.as_bytes());
            if let Err(e) = result {
                return Err(e);
            }

            if self.index_keys_list.len() > 0 {
                let result = self.index_keys_list.get(row_number);
                let index_keys = match result {
                    None => {
                        return Err(MysqlError::new_global_error(
                            1105,
                            format!("Index keys not found, row_index: {:?}", row_number,).as_str(),
                        ));
                    }
                    Some(index_keys) => index_keys.clone(),
                };

                if index_keys.len() > 0 {
                    for index in index_keys {
                        let result = store_engine.put_key(index.index_key, rowid.as_bytes());
                        if let Err(e) = result {
                            return Err(e);
                        }
                    }
                }
            }

            for column_index in 0..self.column_name_list.to_vec().len() {
                let column_name = self.column_name_list[column_index].to_ident();
                let result = column_value_map.get(&column_name);
                let column_value = match result {
                    None => {
                        return Err(MysqlError::new_global_error(
                            1105,
                            format!(
                                "Column value not found, row_index: {:?}, column_index: {:?}",
                                row_number, column_index,
                            )
                            .as_str(),
                        ));
                    }
                    Some(column_value) => column_value.clone(),
                };

                let sparrow_column = self
                    .table
                    .get_table_column()
                    .get_sparrow_column(column_name)
                    .unwrap();
                let store_id = sparrow_column.store_id;

                let column_key = create_column_key(
                    self.table.option.full_table_name.clone(),
                    store_id,
                    rowid.as_str(),
                );
                log::debug!("column_key: {:?}", column_key);
                let result = core_util::convert_scalar_value(column_value.clone()).unwrap();
                log::debug!("column_value: {:?}", result);

                let mut payload: Vec<u8> = Vec::new();
                match result {
                    None => payload.push(0x00),
                    Some(v) => payload.extend_from_slice(v.as_bytes()),
                }
                let mem = Bytes::from(payload);

                let result = store_engine.put_key(column_key, mem.bytes());
                if let Err(e) = result {
                    return Err(e);
                }
            }
        }

        Ok(self.column_value_map_list.len() as u64)
    }
}
