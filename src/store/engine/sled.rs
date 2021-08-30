use bstr::ByteSlice;
use std::sync::{Arc, Mutex};

use sled::{Config, IVec};
use sled::{Db, Iter};

use arrow::array::{StringArray, Array};
use arrow::error::{ArrowError, Result};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::Expr;
use sled::Db as SledDb;

use crate::core::global_context::GlobalContext;
use crate::datafusion_impl::datasource::sled::SledTable;
use crate::meta::meta_util;
use crate::mysql::error::{MysqlResult, MysqlError};

use super::engine_util::TableEngine;
use crate::core::session_context::SessionContext;
use sqlparser::ast::ObjectName;
use datafusion::scalar::ScalarValue;
use crate::meta::def::TableDef;
use crate::store::reader::sled::SledReader;
use crate::store::engine::engine_util::StoreEngine;

pub struct TableEngineSled {
    global_context: Arc<Mutex<GlobalContext>>,
    full_table_name: ObjectName,
    table_def: TableDef,
}

impl TableEngineSled {
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

impl TableEngine for TableEngineSled {
    fn table_provider(&self) -> Arc<dyn TableProvider> {
        let provider = SledTable::new(self.global_context.clone(), self.table_def.clone(), self.full_table_name.clone());
        Arc::new(provider)
    }

    fn table_iterator(&self, projection: Option<Vec<usize>>) -> Box<dyn Iterator<Item = Result<RecordBatch>>> {
        let reader = SledReader::new(self.global_context.clone(), self.table_def.clone(), self.full_table_name.clone(), 1024, projection, &[]);
        Box::new(reader)
    }
}

pub struct StoreEngineSled {
    sled_db: SledDb,
}

impl StoreEngineSled {
    pub fn new(sled_db: SledDb) -> Self {
        Self {
            sled_db,
        }
    }
}

impl StoreEngine for StoreEngineSled {
    fn delete_key(&self, key: String) -> MysqlResult<()> {
        let result = self.sled_db.remove(key).unwrap();
        Ok(())
    }

    fn get_key(&self, key: String) -> MysqlResult<Option<&[u8]>> {
        let result = self.sled_db.get(key).unwrap();
        match result {
            None => Ok(None),
            Some(value) => Ok(Some(value.as_bytes())),
        }
    }

    fn put_key(&self, key: String, value: &[u8]) -> MysqlResult<()> {
        let result = self.sled_db.insert(key, value).unwrap();
        Ok(())
    }
}
