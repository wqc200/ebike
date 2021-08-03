use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use datafusion::error::{Result};
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::LogicalPlan;

use crate::core::core_util;
use crate::core::global_context::GlobalContext;
use crate::core::output::CoreOutput;
use crate::core::output::FinalCount;
use crate::core::session_context::SessionContext;
use crate::meta::{meta_util, meta_const};
use crate::mysql::error::{MysqlResult, MysqlError};
use crate::store::engine::sled::SledOperator;
use crate::util;
use sqlparser::ast::{ObjectName, SetVariableValue};


pub struct SetVariable {
    variable: ObjectName,
    value: Vec<SetVariableValue>,
}

impl SetVariable {
    pub fn new(variable: ObjectName, value: Vec<SetVariableValue>) -> Self {
        Self { variable, value }
    }

    pub fn execute(&self, datafusion_context: &mut ExecutionContext, global_context: Arc<Mutex<GlobalContext>>, session_context: &mut SessionContext) -> MysqlResult<u64> {
        Ok(0)
    }
}
