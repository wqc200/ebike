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
use crate::meta::{meta_util, meta_const};
use crate::mysql::{command, packet, request, response, message, metadata};
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::store::engine::engine_util;
use crate::store::engine::engine_util::{TableEngine, StoreEngineFactory};

use crate::test;
use crate::util;
use crate::store::rocksdb::db::Error;
use crate::core::session_context::SessionContext;
use crate::meta::meta_def::TableDef;

pub struct PhysicalPlanDelete {
    global_context: Arc<Mutex<GlobalContext>>,
    table: TableDef,
    execution_plan: Arc<dyn ExecutionPlan>,
}

impl PhysicalPlanDelete {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table: TableDef,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            global_context,
            table,
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
        let store_engine = StoreEngineFactory::try_new_with_table(self.global_context.clone(), self.table.clone()).unwrap();

        let rowid_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for row_index in 0..rowid_array.len() {
            let rowid = rowid_array.value(row_index);

            let record_rowid_key = util::dbkey::create_record_rowid(self.table.option.full_table_name.clone(), rowid.as_ref());
            log::debug!("record_rowid_key: {:?}", record_rowid_key);
            store_engine.delete_key(record_rowid_key);

            for sql_column in self.table.get_table_column().sql_column_list {
                let column_name = sql_column.name;
                if column_name.to_string().contains(meta_const::COLUMN_ROWID) {
                    continue;
                }

                let sparrow_column = self.table.get_table_column().get_sparrow_column(column_name).unwrap();
                let store_id = sparrow_column.store_id;

                let record_column_key = util::dbkey::create_column_key(self.table.option.full_table_name.clone(), store_id, rowid.as_ref());
                let result = store_engine.delete_key(record_column_key.clone());
                match result {
                    Err(error) => {
                        return Err(MysqlError::new_global_error(1105, format!(
                            "Unknown error. An error occurred while deleting the key, key: {:?}, error: {:?}",
                            record_column_key.clone(),
                            error,
                        ).as_str()));
                    }
                    _ => {}
                }
            }
        }

        Ok(rowid_array.len() as u64)
    }
}
