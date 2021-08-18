use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, StringArray};
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

pub trait Engine {
    fn table_provider(&self) -> Arc<dyn TableProvider>;
    fn table_iterator(&self) -> Arc<dyn Iterator>;
    fn delete_key(&self, key: String) -> MysqlResult<()>;
    fn get_key(&self, key: String) -> MysqlResult<Option<&[u8]>>;
    fn put_key(&self, key: String, value: &[u8]) -> MysqlResult<()>;
}

pub struct EngineFactory;

impl EngineFactory {
    pub fn try_new(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) -> MysqlResult<Box<dyn Engine>> {
        let table_def = global_context.lock().unwrap().meta_cache.get_table(full_table_name.clone()).unwrap().clone();

        match table_def.clone().get_engine() {
            Some(engine) => {
                match engine.as_str() {
                    meta_const::OPTION_ENGINE_NAME_ROCKSDB => Ok(Box::new(rocksdb::Rocksdb::new(global_context, full_table_name, table_def))),
                    meta_const::OPTION_ENGINE_NAME_SLED => Ok(Box::new(sled::Sled::new(global_context, full_table_name, table_def))),
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
                    "Unknown error. The engine in table {:?} not found.",
                    full_table_name,
                ).as_str()))
            }
        }
    }
}

pub fn insert(add_entry_type: ADD_ENTRY_TYPE, column_names: Vec<String>, rows: Vec<Vec<ScalarValue>>) -> MysqlResult<u64> {
    let table_name = meta_util::cut_out_table_name(self.full_table_name.clone()).to_string();

    let all_table_index = meta_util::get_all_table_index(self.global_context.clone(), self.full_table_name.clone()).unwrap();

    let serial_number_map = self.global_context.lock().unwrap().meta_cache.get_serial_number_map(self.full_table_name.clone()).unwrap();

    let mut row_index_keys = vec![];
    for row_index in 0..rows.len() {
        let row = rows[row_index].clone();

        let mut index_keys = vec![];
        for (index_name, level, column_name_vec) in all_table_index.clone() {
            let column_name_value_map = engine_util::build_column_name_value(column_name_vec.clone(), row.clone()).unwrap();
            let serial_number_value_vec = engine_util::build_column_serial_number_value(column_name_vec.clone(), serial_number_map.clone(), column_name_value_map).unwrap();
            let result = util::dbkey::create_index(self.full_table_name.clone(), index_name.as_str(), serial_number_value_vec);
            let index_key = match result {
                Ok(index_key) => index_key,
                Err(mysql_error) => return Err(mysql_error)
            };
            index_keys.push((index_name.clone(), level, index_key.clone()));
        }

        row_index_keys.push(index_keys);
    }

    for index_keys in row_index_keys.clone() {
        for (index_name, level, index_key) in index_keys {
            if level == 1 || level == 2 {
                match self.global_context.lock().unwrap().rocksdb_db.get(index_key.clone()).unwrap() {
                    None => {}
                    Some(_) => {
                        if add_entry_type == engine_util::ADD_ENTRY_TYPE::INSERT {
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

    for row_index in 0..rows.len() {
        let row = rows[row_index].clone();
        let rowid = Uuid::new_v4().to_simple().encode_lower(&mut Uuid::encode_buffer()).to_string();
        let index_keys = row_index_keys[row_index].clone();

        let rowid_key = util::dbkey::create_record_rowid(self.full_table_name.clone(), rowid.as_str());
        log::debug!("rowid_key: {:?}", String::from_utf8_lossy(rowid_key.to_vec().as_slice()));
        self.global_context.lock().unwrap().rocksdb_db.put(rowid_key.to_vec(), rowid.as_str());

        if index_keys.len() > 0 {
            for (index_name, level, index_key) in index_keys {
                self.global_context.lock().unwrap().rocksdb_db.put(index_key, rowid.as_str());
            }
        }

        for index in 0..column_names.to_vec().len() {
            let column_name = column_names[index].to_ident();
            let column_index = self.global_context.lock().unwrap().meta_cache.get_serial_number(self.full_table_name.clone(), column_name.clone()).unwrap();
            let column_key = util::dbkey::create_record_column(self.full_table_name.clone(), column_index, rowid.as_str());
            log::debug!("column_key: {:?}", String::from_utf8_lossy(column_key.to_vec().as_slice()));
            let column_value = core_util::convert_scalar_value(rows[row_index][index].clone()).unwrap();
            log::debug!("column_value: {:?}", column_value);
            if let Some(value) = column_value {
                self.global_context.lock().unwrap().rocksdb_db.put(column_key.to_vec(), value.as_bytes());
            } else {
                //self.core_context.lock().unwrap().rocksdb_db.put(column_key.to_vec(), None);
            }
        }
    }

    Ok(rows.len() as u64)
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
        let column_value = match column_values.get(i){
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
