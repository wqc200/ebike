use num::ToPrimitive;
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::{Result};
use sled::Db as SledDb;

// use crate::core::udf::UdfContext;
use crate::meta::cache::MetaCache;
use crate::store::rocksdb::db::DB as RocksdbDB;
use crate::test;
use crate::util;
use crate::meta::variable::Variable;
use crate::config::def::MyConfig;
use crate::store::rocksdb::option::Options;

#[derive(Clone, Debug)]
pub struct Engine {
    pub sled_db: Option<SledDb>,
    pub rocksdb_db: Option<RocksdbDB>,
}

#[derive(Clone, Debug)]
pub struct GlobalContext {
    pub my_config: MyConfig,
    pub meta_cache: MetaCache,
    pub variable: Variable,
    pub engine: Engine,
}

impl GlobalContext {
    pub fn new(my_config: MyConfig) -> Self {
        let meta_cache = MetaCache::new();
        let variable = Variable::new();

        let mut sled_db = None;
        let mut rocksdb_db = None;
        for engine in my_config.server.engines {
            if engine.eq("sled") {
                let config = sled::Config::new().temporary(false).path(my_config.engine.sled.data_path.clone());
                let db = config.open().unwrap();
                sled_db = Some(db);
            } else if engine.eq("rocksdb") {
                let mut opts = Options::default();
                let db = RocksdbDB::open(&opts, my_config.engine.rocksdb.data_path.as_str()).unwrap();
                rocksdb_db = Some(db);
            }
        }
        let engine = Engine {
            sled_db,
            rocksdb_db,
        };

        let global_context = Self {
            my_config,
            meta_cache,
            variable,
            engine,
        };
        global_context
    }
}
