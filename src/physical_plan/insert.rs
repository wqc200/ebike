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

use crate::core::{core_util as CoreUtil, core_util};
use crate::core::global_context::GlobalContext;
use crate::core::output::CoreOutput;
use crate::core::output::FinalCount;
use crate::core::session_context::SessionContext;
use crate::meta::def::TableDef;
use crate::meta::meta_util;
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::store::engine::engine_util;
use crate::store::engine::sled::SledOperator;
use crate::test;
use crate::util;
use crate::util::convert::ToIdent;

pub struct Insert {
    global_context: Arc<Mutex<GlobalContext>>,
    full_table_name: ObjectName,
    table_def: TableDef,
    column_name_list: Vec<String>,
    index_keys_list: Vec<Vec<(String, usize, String)>>,
    column_value_map_list: Vec<HashMap<Ident, ScalarValue>>,
}

impl Insert {
    pub fn new(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName, table_def: TableDef, column_name_list: Vec<String>, index_keys_list: Vec<Vec<(String, usize, String)>>, column_value_map_list: Vec<HashMap<Ident, ScalarValue>>) -> Self {
        Self {
            global_context,
            full_table_name,
            table_def,
            column_name_list,
            index_keys_list,
            column_value_map_list,
        }
    }

    pub fn execute(&self) -> MysqlResult<u64> {
        let result = engine_util::EngineFactory::try_new(self.global_context.clone(), self.full_table_name.clone(), self.table_def.clone());
        let engine = match result {
            Ok(engine) => engine,
            Err(mysql_error) => return Err(mysql_error),
        };

        for row_index in 0..self.column_value_map_list.len() {
            let rowid = Uuid::new_v4().to_simple().encode_lower(&mut Uuid::encode_buffer()).to_string();
            let column_value_map = self.column_value_map_list[row_index].clone();

            let rowid_key = util::dbkey::create_column_rowid_key(self.full_table_name.clone(), rowid.as_str());
            log::debug!("rowid_key: {:?}", String::from_utf8_lossy(rowid_key.to_vec().as_slice()));
            engine.put_key(rowid_key, rowid.as_bytes());

            if self.index_keys_list.len() > 0 {
                let result = self.index_keys_list.get(row_index);
                let index_keys = match result {
                    None => {
                        return Err(MysqlError::new_global_error(1105, format!(
                            "Index keys not found, row_index: {:?}",
                            row_index,
                        ).as_str()));
                    }
                    Some(index_keys) => index_keys.clone(),
                };

                if index_keys.len() > 0 {
                    for (index_name, level, index_key) in index_keys {
                        engine.put_key(index_key, rowid.as_bytes());
                    }
                }
            }

            for column_index in 0..self.column_name_list.to_vec().len() {
                let column_name = self.column_name_list[column_index].to_ident();
                let result = column_value_map.get(&column_name);
                let column_value = match result {
                    None => {
                        return Err(MysqlError::new_global_error(1105, format!(
                            "Column value not found, row_index: {:?}, column_index: {:?}",
                            row_index,
                            column_index,
                        ).as_str()));
                    }
                    Some(column_value) => column_value.clone(),
                };


                let column_orm_id = self.global_context.lock().unwrap().meta_cache.get_serial_number(self.full_table_name.clone(), column_name.clone()).unwrap();
                let column_key = util::dbkey::create_column_key(full_table_name.clone(), column_orm_id, rowid.as_str());
                log::debug!("column_key: {:?}", String::from_utf8_lossy(column_key.to_vec().as_slice()));
                let result = core_util::convert_scalar_value(column_value.clone()).unwrap();
                log::debug!("column_value: {:?}", result);
                if let Some(value) = result {
                    engine.put_key(column_key, value.as_bytes());
                }
            }
        }

        Ok(rows.len() as u64)
    }
}
