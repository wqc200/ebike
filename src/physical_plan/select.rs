use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use arrow::array::{Array};
use arrow::array::{
    ArrayData,
    BinaryArray,
    Int8Array,
    Int16Array,
    Int32Array,
    Int64Array,
    UInt8Array,
    UInt16Array,
    UInt32Array,
    UInt64Array,
    Float32Array,
    Float64Array,
    StringArray,
};
use arrow::datatypes::{DataType, Field, SchemaRef, ToByteSlice};
use arrow::record_batch::RecordBatch;
use datafusion::error::{Result};
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::{Expr, LogicalPlan};
use datafusion::physical_plan::{collect, ExecutionPlan};
use sqlparser::ast::{Assignment, ObjectName, Expr as SQLExpr, Value};
use uuid::Uuid;

use crate::mysql::{command, packet, request, response, message, metadata};
use crate::core::global_context::GlobalContext;
use crate::core::output::CoreOutput;
use crate::core::output::FinalCount;
use crate::core::core_util as CoreUtil;

use crate::mysql::error::{MysqlError, MysqlResult};
use crate::store::engine::sled::SledOperator;
use crate::test;
use crate::util;

pub struct Select {
    core_context: Arc<Mutex<GlobalContext>>,
    execution_plan: Arc<ExecutionPlan>,
}

impl Select {
    pub fn new(
        core_context: Arc<Mutex<GlobalContext>>,
        execution_plan: Arc<ExecutionPlan>,
    ) -> Self {
        Self {
            core_context,
            execution_plan,
        }
    }

    pub async fn execute(&self) -> MysqlResult<(SchemaRef, Vec<RecordBatch>)> {
        let schema_ref = self.execution_plan.schema();
        let results = collect(self.execution_plan.clone()).await;
        match results {
            Ok(record_batch) => {
                Ok((schema_ref, record_batch))
            }
            Err(datafusion_error) => {
                Err(MysqlError::from(datafusion_error))
            }
        }
    }
}
