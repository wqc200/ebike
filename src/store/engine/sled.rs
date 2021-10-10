use std::sync::{Arc, Mutex};

use arrow::error::{Result};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::Expr;
use sled::Db as SledDb;

use crate::core::global_context::GlobalContext;
use crate::datafusion_impl::datasource::sled::SledTable;
use crate::mysql::error::{MysqlResult};

use super::engine_util::TableEngine;
use crate::meta::meta_def::TableDef;
use crate::store::reader::sled::SledReader;
use crate::store::engine::engine_util::StoreEngine;

pub struct TableEngineSled {
    global_context: Arc<Mutex<GlobalContext>>,
    table: TableDef,
}

impl TableEngineSled {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table: TableDef,
    ) -> Self {
        Self {
            global_context,
            table: table,
        }
    }
}

impl TableEngine for TableEngineSled {
    fn table_provider(&self) -> Arc<dyn TableProvider> {
        let provider = SledTable::new(self.global_context.clone(), self.table.clone());
        Arc::new(provider)
    }

    fn table_iterator(&self, projection: Option<Vec<usize>>, filters: &[Expr]) -> Box<dyn Iterator<Item = Result<RecordBatch>>> {
        let reader = SledReader::new(self.global_context.clone(), self.table.clone(), 1024, projection, filters);
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

    fn get_key(&self, key: String) -> MysqlResult<Option<Vec<u8>>> {
        let result = self.sled_db.get(key).unwrap();
        match result {
            None => Ok(None),
            Some(value) => {
                let v = value.to_vec();
                Ok(Some(v))
            },
        }
    }

    fn put_key(&self, key: String, value: &[u8]) -> MysqlResult<()> {
        let result = self.sled_db.insert(key, value).unwrap();
        Ok(())
    }
}
