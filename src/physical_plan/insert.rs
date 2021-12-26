use bytes::{Bytes, Buf};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::scalar::ScalarValue;
use sqlparser::ast::{Ident};
use uuid::Uuid;

use crate::core::{core_util};
use crate::core::global_context::GlobalContext;
use crate::meta::meta_def::{TableDef, IndexDef};
use crate::mysql::error::{MysqlError, MysqlResult};

use crate::util;
use crate::util::convert::ToIdent;
use crate::store::engine::engine_util::{StoreEngineFactory};

pub struct PhysicalPlanInsert {
    global_context: Arc<Mutex<GlobalContext>>,
}

impl PhysicalPlanInsert {
    pub fn new(global_context: Arc<Mutex<GlobalContext>>) -> Self {
        Self {
            global_context
        }
    }

    pub fn execute(&self, table: TableDef, column_name_list: Vec<String>, index_keys_list: Vec<Vec<IndexDef>>, column_value_map_list: Vec<HashMap<Ident, ScalarValue>>) -> MysqlResult<u64> {
        let store_engine = StoreEngineFactory::try_new_with_table(self.global_context.clone(), table.clone()).unwrap();

        for row_number in 0..column_value_map_list.len() {
            let rowid=  Uuid::new_v4().to_simple().encode_lower(&mut Uuid::encode_buffer()).to_string();
            let column_value_map = column_value_map_list[row_number].clone();

            let column_rowid_key = util::dbkey::create_column_rowid_key(table.option.full_table_name.clone(), rowid.as_str());
            log::debug!("rowid_key: {:?}", column_rowid_key);
            let result = store_engine.put_key(column_rowid_key, rowid.as_bytes());
            if let Err(e) = result {
                return Err(e);
            }

            if index_keys_list.len() > 0 {
                let result = index_keys_list.get(row_number);
                let index_keys = match result {
                    None => {
                        return Err(MysqlError::new_global_error(1105, format!(
                            "Index keys not found, row_index: {:?}",
                            row_number,
                        ).as_str()));
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

            for column_index in 0..column_name_list.to_vec().len() {
                let column_name = column_name_list[column_index].to_ident();
                let result = column_value_map.get(&column_name);
                let column_value = match result {
                    None => {
                        return Err(MysqlError::new_global_error(1105, format!(
                            "Column value not found, row_index: {:?}, column_index: {:?}",
                            row_number,
                            column_index,
                        ).as_str()));
                    }
                    Some(column_value) => column_value.clone(),
                };

                let sparrow_column = table.get_table_column().get_sparrow_column(column_name).unwrap();
                let store_id = sparrow_column.store_id;

                let column_key = util::dbkey::create_column_key(table.option.full_table_name.clone(), store_id, rowid.as_str());
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

        Ok(column_value_map_list.len() as u64)
    }
}
