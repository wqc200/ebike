use num::ToPrimitive;
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::{Result};
use sled::Db as SledDb;

use crate::config::util::get_config_path;
use crate::config::util::read_config;
use crate::meta::data::MetaData;
use crate::test;
use crate::util;
use crate::meta::variable::Variable;
use crate::config::def::MyConfig;

#[derive(Clone, Debug)]
pub struct Engine {
    pub sled_db: Option<SledDb>,
}

#[derive(Clone, Debug)]
pub struct GlobalContext {
    pub my_config: MyConfig,
    pub meta_data: MetaData,
    pub variable: Variable,
    pub engine: Engine,
}

impl GlobalContext {
    pub fn new() -> Self {
        let config_path = get_config_path();
        println!("The config path: {}", config_path);

        let my_config = read_config(config_path.as_str());

        let meta_cache = MetaData::new();
        let variable = Variable::new();

        let mut sled_db = None;
        for engine in &my_config.server.engines {
            if engine.eq("sled") {
                let config = sled::Config::new().temporary(false).path(my_config.engine.sled.data_path.clone());
                let db = config.open().unwrap();
                sled_db = Some(db);
            }
        }
        let engine = Engine {
            sled_db,
        };

        let global_context = Self {
            my_config,
            meta_data: meta_cache,
            variable,
            engine,
        };
        global_context
    }
}
