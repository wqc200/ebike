use bstr::ByteSlice;
use std::sync::{Arc, Mutex};
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::{Expr};
use std::collections::HashMap;
use uuid::Uuid;

use arrow::array::{StringArray, Array};
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{TableConstraint, ObjectName};

use crate::core::global_context::GlobalContext;
use crate::core::core_util;
use crate::datafusion_impl::datasource::rocksdb::RocksdbTable;
use crate::meta::{meta_util, meta_const};
use crate::mysql::error::{MysqlResult, MysqlError};
use crate::store::engine::engine_util;
use crate::store::engine::engine_util::Engine;
use crate::store::rocksdb::db::Error;
use crate::store::reader::rocksdb::RocksdbReader;
use crate::util;
use crate::core::session_context::SessionContext;
use crate::util::convert::{ToObjectName, ToIdent};
use crate::meta::def::TableDef;

pub struct Rocksdb {
    global_context: Arc<Mutex<GlobalContext>>,
    full_table_name: ObjectName,
    table_def: TableDef,
}

impl Rocksdb {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        full_table_name: ObjectName,
        table_def: TableDef,
    ) -> Self {
        Self {
            global_context,
            full_table_name,
            table_def,
        }
    }
}


impl Engine for Rocksdb {
    fn table_provider(&self) -> Arc<dyn TableProvider> {
        let provider = RocksdbTable::try_new(self.global_context.clone(), self.table_def.clone(), "/tmp/rocksdb/a", self.full_table_name.clone()).unwrap();
        Arc::new(provider)
    }

    fn table_iterator(&self) -> Arc<dyn Iterator> {
        let reader = RocksdbReader::new(self.global_context.clone(), self.table_def.clone(), "/tmp/rocksdb/a", self.full_table_name.clone(), 1024, None,&[]);
        Arc::new(reader)
    }

    fn delete_key(&self, key: String) -> MysqlResult<()> {
        let result = self.global_context.lock().unwrap().engine.rocksdb_db.unwrap().delete(key).unwrap();
        Ok(())
    }

    fn get_key(&self, key: String) -> MysqlResult<Option<&[u8]>> {
        let result = self.global_context.lock().unwrap().engine.rocksdb_db.unwrap().get(key).unwrap();
        match result {
            None => Ok(None),
            Some(value) => Ok(Some(value.as_bytes())),
        }
    }

    fn put_key(&self, key: String, value: &[u8]) -> MysqlResult<()> {
        let result = self.global_context.lock().unwrap().engine.rocksdb_db.unwrap().put(key, value).unwrap();
        Ok(())
    }
}

impl Rocksdb {
    fn insert(&self, add_entry_type: engine_util::ADD_ENTRY_TYPE, column_names: Vec<String>, rows: Vec<Vec<ScalarValue>>) -> MysqlResult<u64> {
        let catalog_name = meta_util::cut_out_catalog_name(self.full_table_name.clone()).to_string();
        let schema_name = meta_util::cut_out_schema_name(self.full_table_name.clone()).to_string();
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

    fn add_rows(&self, column_names: Vec<String>, rows: Vec<Vec<Expr>>) -> MysqlResult<u64> {
        let catalog_name = meta_util::cut_out_catalog_name(self.full_table_name.clone()).to_string();
        let schema_name = meta_util::cut_out_schema_name(self.full_table_name.clone()).to_string();
        let table_name = meta_util::cut_out_table_name(self.full_table_name.clone()).to_string();

        for row_index in 0..rows.len() {
            let row = rows[row_index].clone();
            let rowid = Uuid::new_v4().to_simple().encode_lower(&mut Uuid::encode_buffer()).to_string();

            let rowid_key = util::dbkey::create_record_rowid(self.full_table_name.clone(), rowid.as_str());
            log::debug!("rowid_key: {:?}", String::from_utf8_lossy(rowid_key.to_vec().as_slice()));
            self.global_context.lock().unwrap().rocksdb_db.put(rowid_key.to_vec(), rowid.as_str());

            for index in 0..column_names.to_vec().len() {
                let column_name = column_names[index].to_ident();

                let result = self.global_context.lock().unwrap().meta_cache.get_serial_number(self.full_table_name.clone(), column_name.clone());
                let column_index = match result {
                    Ok(column_index) => column_index,
                    Err(e) => {
                        return Err(e)
                    },
                };

                let column_key = util::dbkey::create_record_column(self.full_table_name.clone(), column_index, rowid.as_str());
                log::debug!("column_key: {:?}", String::from_utf8_lossy(column_key.to_vec().as_slice()));
                let result = core_util::get_real_value(row[index].clone());
                let column_value = match result {
                    Err(e) => {
                        return Err(MysqlError::from(e));
                    }
                    Ok(column_value) => column_value
                };
                log::debug!("column_value: {:?}", column_value);
                if let Some(value) = column_value {
                    self.global_context.lock().unwrap().rocksdb_db.put(column_key.to_vec(), value.as_bytes());
                } else {
                }
            }
        }

        Ok(rows.len() as u64)
    }

    pub fn delete_rows(&self, rowid_array: &StringArray) -> MysqlResult<u64> {
        let catalog_name = meta_util::cut_out_catalog_name(self.full_table_name.clone()).to_string();
        let schema_name = meta_util::cut_out_schema_name(self.full_table_name.clone()).to_string();
        let table_name = meta_util::cut_out_table_name(self.full_table_name.clone()).to_string();

        let table_schema = self.global_context.lock().unwrap().meta_cache.get_table(self.full_table_name.clone()).unwrap();

        for row_index in 0..rowid_array.len() {
            let rowid = rowid_array.value(row_index);

            let recordPutKey = util::dbkey::create_record_rowid(self.full_table_name.clone(), rowid.as_ref());
            log::debug!("recordPutKey pk: {:?}", String::from_utf8_lossy(recordPutKey.to_vec().as_slice()));
            self.global_context.lock().unwrap().rocksdb_db.delete(recordPutKey.to_vec());

            for column_def in table_schema.to_sqlcolumns() {
                let column_name = column_def.name;
                if column_name.to_string().contains(meta_const::COLUMN_ROWID) {
                    continue;
                }

                let result = self.global_context.lock().unwrap().meta_cache.get_serial_number(self.full_table_name.clone(), column_name.clone());
                let orm_id = match result {
                    Ok(orm_id) => orm_id,
                    Err(mysql_error) => {
                        return Err(mysql_error);
                    }
                };

                let recordPutKey = util::dbkey::create_record_column(self.full_table_name.clone(), orm_id, rowid.as_ref());
                let result = self.global_context.lock().unwrap().rocksdb_db.delete(recordPutKey.to_vec());
                match result {
                    Err(error) => {
                        return Err(MysqlError::new_global_error(1105, format!(
                            "Unknown error. An error occurred while deleting the key, key: {:?}, error: {:?}",
                            recordPutKey,
                            error,
                        ).as_str()));
                    }
                    _ => {}
                }
            }
        }

        Ok(rowid_array.len() as u64)
    }
}
