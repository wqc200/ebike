use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::LogicalPlan;
use sqlparser::ast::ObjectName;

use crate::core::core_util;
use crate::core::global_context::GlobalContext;
use crate::core::output::CoreOutput;
use crate::core::output::FinalCount;
use crate::core::session_context::SessionContext;
use crate::meta::{meta_const, meta_util};
use crate::meta::initial::initial_util;
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::store::engine::sled::SledOperator;
use crate::util;

pub struct CreateDb {
    db_name: ObjectName,
}

impl CreateDb {
    pub fn new(db_name: ObjectName) -> Self {
        Self { db_name }
    }

    pub fn execute(&self, global_context: Arc<Mutex<GlobalContext>>, execution_context: &mut ExecutionContext, session_context: &mut SessionContext) -> MysqlResult<u64> {
        let full_schema_name = meta_util::fill_up_schema_name(session_context, self.db_name.clone()).unwrap();

        let db_name = meta_util::cut_out_schema_name(full_schema_name.clone());

        initial_util::create_schema(global_context, full_schema_name);
        core_util::register_schema(execution_context, meta_const::CATALOG_NAME, db_name.to_string().as_str());

        Ok(1)
    }
}
