use std::sync::{Arc, Mutex};

use arrow::datatypes::{SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{collect, ExecutionPlan};

use crate::core::global_context::GlobalContext;

use crate::mysql::error::{MysqlError, MysqlResult};

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
