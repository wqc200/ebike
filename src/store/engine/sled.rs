use bstr::ByteSlice;
use std::sync::{Arc, Mutex};

use sled::{Config, IVec};
use sled::{Db, Iter};

use arrow::array::{StringArray, Array};
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::Expr;

use crate::core::global_context::GlobalContext;
use crate::datafusion_impl::datasource::sled::SledTable;
use crate::meta::meta_util;
use crate::mysql::error::{MysqlResult, MysqlError};

use super::engine_util::Engine;
use crate::core::session_context::SessionContext;
use sqlparser::ast::ObjectName;
use datafusion::scalar::ScalarValue;
use crate::meta::def::TableDef;
use crate::store::reader::sled::SledReader;

pub struct Sled {
    global_context: Arc<Mutex<GlobalContext>>,
    full_table_name: ObjectName,
    table_def: TableDef,
}

impl Sled {
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

impl Engine for Sled {
    fn table_provider(&self) -> Arc<dyn TableProvider> {
        let provider = SledTable::new(self.global_context.clone(), self.table_schema.clone(), "/tmp/rocksdb/", self.full_table_name.clone(), self.table_name.clone()).unwrap();
        Arc::new(provider)
    }

    fn table_iterator(&self) -> Arc<dyn Iterator> {
        let reader = SledReader::new(self.global_context.clone(), self.table_def.clone(), self.full_table_name.clone(), 1024, None,&[]);
        Arc::new(reader)
    }

    fn delete_key(&self, key: String) -> MysqlResult<()> {
        let result = self.global_context.lock().unwrap().engine.sled_db.unwrap().remove(key).unwrap();
        Ok(())
    }

    fn get_key(&self, key: String) -> MysqlResult<Option<&[u8]>> {
        let result = self.global_context.lock().unwrap().engine.sled_db.unwrap().get(key).unwrap();
        match result {
            None => Ok(None),
            Some(value) => Ok(Some(value.as_bytes())),
        }
    }

    fn put_key(&self, key: String, value: &[u8]) -> MysqlResult<()> {
        let result = self.global_context.lock().unwrap().engine.sled_db.unwrap().insert(key, value).unwrap();
        Ok(())
    }
}
