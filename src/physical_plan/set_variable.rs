use std::sync::{Arc, Mutex};

use datafusion::execution::context::ExecutionContext;

use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::meta::{meta_const};
use crate::mysql::error::{MysqlResult};

use sqlparser::ast::{ObjectName, SetVariableValue};

pub struct SetVariable {
    variable: ObjectName,
    value: Vec<SetVariableValue>,
}

impl SetVariable {
    pub fn new(variable: ObjectName, value: Vec<SetVariableValue>) -> Self {
        Self { variable, value }
    }

    pub fn execute(&self, _: &mut ExecutionContext, _: Arc<Mutex<GlobalContext>>, _: &mut SessionContext) -> MysqlResult<u64> {
        Ok(0)
    }
}
