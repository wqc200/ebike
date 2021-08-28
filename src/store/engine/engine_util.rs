use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow::error::{ArrowError, Result};
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::Expr;
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{ObjectName, Ident};

use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::datafusion_impl::datasource::rocksdb::RocksdbTable;
use crate::meta::{meta_const, meta_util};
use crate::mysql::error::{MysqlError, MysqlResult};

use super::rocksdb;
use super::sled;
use crate::meta::def::TableDef;

#[derive(Clone, Debug, PartialEq)]
pub enum ADD_ENTRY_TYPE {
    INSERT,
    REPLACE,
}

pub trait StoreEngine {
    fn delete_key(&self, key: String) -> MysqlResult<()>;
    fn get_key(&self, key: String) -> MysqlResult<Option<&[u8]>>;
    fn put_key(&self, key: String, value: &[u8]) -> MysqlResult<()>;
}

pub trait TableEngine {
    fn table_provider(&self) -> Arc<dyn TableProvider>;
    fn table_iterator(&self) -> Arc<dyn Iterator<Item = Result<RecordBatch>>>;
}

pub struct EngineFactory;

impl EngineFactory {
    pub fn try_new_with_table(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) -> MysqlResult<Box<dyn TableEngine>> {
        let table_def = global_context.lock().unwrap().meta_cache.get_table(full_table_name.clone()).unwrap().clone();

        match table_def.clone().get_engine() {
            Some(engine) => {
                match engine.as_str() {
                    meta_const::OPTION_ENGINE_NAME_ROCKSDB => Ok(Box::new(rocksdb::TableEngineRocksdb::new(global_context, full_table_name, table_def))),
                    meta_const::OPTION_ENGINE_NAME_SLED => Ok(Box::new(sled::TableEngineSled::new(global_context, full_table_name, table_def))),
                    _ => {
                        Err(MysqlError::new_global_error(1105, format!(
                            "Unknown error. The table engine is not supported, table: {:?}, engine: {:?}",
                            full_table_name,
                            engine,
                        ).as_str()))
                    }
                }
            }
            None => {
                Err(MysqlError::new_global_error(1105, format!(
                    "Unknown error. The engine in table not found. table_name: {}",
                    full_table_name,
                ).as_str()))
            }
        }
    }

    pub fn try_new_with_engine(global_context: Arc<Mutex<GlobalContext>>, engine: &str) -> MysqlResult<Box<dyn StoreEngine>> {
        match engine {
            meta_const::OPTION_ENGINE_NAME_ROCKSDB => Ok(Box::new(rocksdb::StoreEngineRocksdb::new(global_context.lock().unwrap().engine.rocksdb_db.unwarp()))),
            meta_const::OPTION_ENGINE_NAME_SLED => Ok(Box::new(sled::StoreEngineSled::new(global_context.lock().unwrap().engine.sled_db.unwarp()))),
            _ => {
                Err(MysqlError::new_global_error(1105, format!(
                    "Unknown error. The table engine is not supported, engine: {:?}",
                    engine,
                ).as_str()))
            }
        }
    }
}

pub fn build_row_column_indexes(row_column_names: Vec<String>) -> HashMap<String, usize> {
    let mut column_indexes = HashMap::new();
    for column_index in 0..row_column_names.len() {
        let column_name = row_column_names[column_index].clone();
        column_indexes.insert(column_name, column_index);
    }
    column_indexes
}

pub fn find_constraint_row_indexes(constraint_keys: Vec<String>, column_names: Vec<String>) -> MysqlResult<Vec<usize>> {
    let column_indexes = build_row_column_indexes(column_names);

    let mut constraint_indexes = vec![];
    for i in 0..constraint_keys.len() {
        let column_name = constraint_keys[i].clone();
        let column_index = column_indexes.get(column_name.as_str()).unwrap();
        constraint_indexes.push(column_index.clone());
    }
    Ok(constraint_indexes)
}

pub fn build_column_serial_number_value(column_name_vec: Vec<Ident>, column_name_serial_number_map: HashMap<Ident, usize>, column_name_value: HashMap<Ident, ScalarValue>) -> MysqlResult<Vec<(usize, ScalarValue)>> {
    let mut serial_number_value_vec = vec![];
    for column_name in column_name_vec {
        let serial_number = match column_name_serial_number_map.get(&column_name) {
            None => return Err(MysqlError::new_global_error(1105, format!(
                "Unknown error. The column `{:?}` serial number not found in input_column_name_serial_number.",
                column_name,
            ).as_str())),
            Some(input) => {
                input.clone()
            }
        };

        let column_value = match column_name_value.get(&column_name) {
            None => return Err(MysqlError::new_global_error(1105, format!(
                "Unknown error. The column value `{:?}` not found in input input_column_name_value.",
                column_name,
            ).as_str())),
            Some(input) => {
                input.clone()
            }
        };

        serial_number_value_vec.push((serial_number, column_value))
    }

    Ok(serial_number_value_vec)
}

pub fn build_column_name_value(column_names: Vec<Ident>, column_values: Vec<ScalarValue>) -> MysqlResult<HashMap<Ident, ScalarValue>> {
    let mut column_value_map: HashMap<Ident, ScalarValue> = HashMap::new();
    for i in 0..column_names.len() {
        let column_name = column_names[i].clone();
        let column_value = match column_values.get(i) {
            None => return Err(MysqlError::new_global_error(1105, format!(
                "Unknown error. The column value `{:?}` not found in input column_values.",
                column_name,
            ).as_str())),
            Some(v) => v.clone(),
        };

        column_value_map.insert(column_name, column_value);
    }
    Ok(column_value_map)
}
