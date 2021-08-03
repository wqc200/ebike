use num::ToPrimitive;
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::{Result};
use sled::Db as SledDb;

// use crate::core::udf::UdfContext;
use crate::meta::cache::MetaCache;
use crate::store::rocksdb::db::DB as rocksdbDB;
use crate::test;
use crate::util;
use crate::meta::variable::Variable;
use crate::config::def::MyConfig;

#[derive(Clone, Debug)]
pub struct Engine {
    pub sled: Option<SledDb>,
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
        if my_config.server.schema_engine.eq("sled") {
            let config = sled::Config::new().temporary(false).path(my_config.engine.sled.data_path.clone());
            let db = config.open().unwrap();
            sled_db = Some(db);
        }
        let engine = Engine {
            sled,
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
