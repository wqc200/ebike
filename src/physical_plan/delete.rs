use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, StringBuilder};
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

use crate::core::global_context::GlobalContext;
use crate::core::output::CoreOutput;
use crate::core::output::FinalCount;
use crate::core::core_util as CoreUtil;
use crate::meta::meta_util;
use crate::mysql::{command, packet, request, response, message, metadata};
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::store::engine::engine_util;
use crate::store::engine::engine_util::Engine;
use crate::store::engine::sled::SledOperator;
use crate::test;
use crate::util;
use crate::store::rocksdb::db::Error;
use crate::core::session_context::SessionContext;

pub struct Delete {
    core_context: Arc<Mutex<GlobalContext>>,
    table_name: ObjectName,
    execution_plan: Arc<dyn ExecutionPlan>,
}

impl Delete {
    pub fn new(
        core_context: Arc<Mutex<GlobalContext>>,
        table_name: ObjectName,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            core_context,
            table_name,
            execution_plan,
        }
    }

    pub async fn execute(&self, session_context: &mut SessionContext) -> MysqlResult<u64> {
        let result = collect(self.execution_plan.clone()).await;
        match result {
            Ok(records) => {
                let mut total = 0;
                for record in records {
                    let result = self.delete_record(session_context, record);
                    match result {
                        Ok(count) => total += count,
                        Err(mysql_error) => return Err(mysql_error),
                    }
                }
                Ok(total)
            }
            Err(datafusion_error) => {
                Err(MysqlError::from(datafusion_error))
            }
        }
    }

    pub fn delete_record(&self, session_context: &mut SessionContext, batch: RecordBatch) -> MysqlResult<u64> {
        let full_table_name = meta_util::fill_up_table_name(session_context, self.table_name.clone()).unwrap();

        let table_schema = self.core_context.lock().unwrap().meta_cache.get_table(full_table_name.clone()).unwrap();
        let result = engine_util::EngineFactory::try_new_with_table_name(self.core_context.clone(), self.table_name.clone(), table_schema.clone());
        let engine = match result {
            Err(mysql_error) => {
                return Err(mysql_error);
            }
            Ok(engine) => {
                engine
            }
        };

        let rowid_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let result = engine.delete(rowid_array);
        result
    }
}
