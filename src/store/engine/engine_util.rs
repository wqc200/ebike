use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{Array};
use arrow::record_batch::RecordBatch;
use arrow::error::{Result};
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::Expr;
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{ObjectName, Ident};

use crate::core::global_context::GlobalContext;
use crate::meta::{meta_const, meta_util};
use crate::mysql::error::{MysqlError, MysqlResult};

use super::sled;
use crate::meta::meta_def::TableDef;

#[derive(Clone, Debug, PartialEq)]
pub enum ADD_ENTRY_TYPE {
    INSERT,
    REPLACE,
}

pub trait StoreEngine {
    fn delete_key(&self, key: String) -> MysqlResult<()>;
    fn get_key(&self, key: String) -> MysqlResult<Option<Vec<u8>>>;
    fn put_key(&self, key: String, value: &[u8]) -> MysqlResult<()>;
}

pub trait TableEngine {
    fn table_provider(&self) -> Arc<dyn TableProvider>;
    fn table_iterator(&self, projection: Option<Vec<usize>>, filters: &[Expr]) -> Box<dyn Iterator<Item = Result<RecordBatch>>>;
}

pub struct TableEngineFactory;

impl TableEngineFactory {
    pub fn try_new_with_table_name(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) -> MysqlResult<Box<dyn TableEngine>> {
        let gc = global_context.lock().unwrap();
        let result = gc.meta_data.get_table(full_table_name.clone());
        let table = match result {
            None => {
                return Err(MysqlError::new_global_error(1105, format!(
                    "Unknown error. The table def not found. table_name: {}",
                    full_table_name,
                ).as_str()))
            },
            Some(table) => table.clone(),
        };

        TableEngineFactory::try_new_with_table(global_context.clone(), table)
    }

    pub fn try_new_with_table(global_context: Arc<Mutex<GlobalContext>>, table: TableDef) -> MysqlResult<Box<dyn TableEngine>> {
        let engine = table.clone().get_engine();
        match engine.as_str() {
            meta_const::VALUE_OF_TABLE_OPTION_ENGINE_SLED => Ok(Box::new(sled::TableEngineSled::new(global_context, table))),
            _ => {
                Err(MysqlError::new_global_error(1105, format!(
                    "Unknown error. The table engine is not supported, table: {:?}, engine: {:?}",
                    table.option.full_table_name,
                    engine,
                ).as_str()))
            }
        }
    }
}

pub struct StoreEngineFactory;

impl StoreEngineFactory {
    pub fn try_new_with_table_name(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) -> MysqlResult<Box<dyn StoreEngine>> {
        let result = meta_util::get_table(global_context.clone(), full_table_name.clone());
        let table = match result {
            Err(mysql_error) => return Err(mysql_error),
            Ok(table) => table.clone(),
        };

        let engine = table.get_engine();
        StoreEngineFactory::try_new_with_engine(global_context.clone(), engine.as_str())
    }

    pub fn try_new_with_table(global_context: Arc<Mutex<GlobalContext>>, table: TableDef) -> MysqlResult<Box<dyn StoreEngine>> {
        let engine = table.get_engine();
        StoreEngineFactory::try_new_with_engine(global_context.clone(), engine.as_str())
    }

    pub fn try_new_schema_engine(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<Box<dyn StoreEngine>> {
        let engine = global_context.lock().unwrap().clone().my_config.schema.engine;
        StoreEngineFactory::try_new_with_engine(global_context.clone(), engine.as_str())
    }

    pub fn try_new_with_engine(global_context: Arc<Mutex<GlobalContext>>, engine: &str) -> MysqlResult<Box<dyn StoreEngine>> {
        let gc = global_context.lock().unwrap();

        match engine {
            meta_const::VALUE_OF_TABLE_OPTION_ENGINE_SLED => {
                let sled_db = gc.engine.sled_db.as_ref().unwrap();
                Ok(Box::new(sled::StoreEngineSled::new(sled_db.clone())))
            }
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
