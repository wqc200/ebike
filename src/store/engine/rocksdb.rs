use bstr::ByteSlice;
use std::sync::{Arc, Mutex};
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::{Expr};
use std::collections::HashMap;
use uuid::Uuid;

use arrow::array::{StringArray, Array};
use arrow::error::{ArrowError, Result};
use arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{TableConstraint, ObjectName};

use crate::core::global_context::GlobalContext;
use crate::core::core_util;
use crate::datafusion_impl::datasource::rocksdb::RocksdbTable;
use crate::meta::{meta_util, meta_const};
use crate::mysql::error::{MysqlResult, MysqlError};
use crate::store::engine::engine_util;
use crate::store::engine::engine_util::{TableEngine, StoreEngine};
use crate::store::rocksdb::db::Error;
use crate::store::reader::rocksdb::RocksdbReader;
use crate::util;
use crate::core::session_context::SessionContext;
use crate::util::convert::{ToObjectName, ToIdent};
use crate::meta::def::TableDef;
use crate::store::rocksdb::db::DB as RocksdbDB;

pub struct TableEngineRocksdb {
    global_context: Arc<Mutex<GlobalContext>>,
    full_table_name: ObjectName,
    table_def: TableDef,
}

impl TableEngineRocksdb {
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


impl TableEngine for TableEngineRocksdb {
    fn table_provider(&self) -> Arc<dyn TableProvider> {
        let provider = RocksdbTable::new(self.global_context.clone(), self.table_def.clone(), self.full_table_name.clone());
        Arc::new(provider)
    }

    fn table_iterator(&self, projection: Option<Vec<usize>>) -> Box<dyn Iterator<Item=Result<RecordBatch>>> {
        let reader = RocksdbReader::new(self.global_context.clone(), self.table_def.clone(), self.full_table_name.clone(), 1024, projection, &[]);
        Box::new(reader)
    }
}

pub struct StoreEngineRocksdb {
    rocksdb_db: RocksdbDB,
}

impl StoreEngineRocksdb {
    pub fn new(rocksdb_db: RocksdbDB) -> Self {
        Self {
            rocksdb_db,
        }
    }
}

impl StoreEngine for StoreEngineRocksdb {
    fn delete_key(&self, key: String) -> MysqlResult<()> {
        let result = self.rocksdb_db.delete(key);
        Ok(())
    }

    fn get_key(&self, key: String) -> MysqlResult<Option<&[u8]>> {
        let result = self.rocksdb_db.get(key).unwrap();
        match result {
            None => Ok(None),
            Some(value) => Ok(Some(value.as_bytes())),
        }
    }

    fn put_key(&self, key: String, value: &[u8]) -> MysqlResult<()> {
        let result = self.rocksdb_db.put(key, value).unwrap();
        Ok(())
    }
}
