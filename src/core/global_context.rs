use sled::Db as SledDb;

use crate::meta::data::MetaData;
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
    pub fn new_with_config(my_config: MyConfig) -> Self {

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
