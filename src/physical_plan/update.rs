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
use crate::core::session_context::SessionContext;
use crate::meta::meta_util;
use crate::util::convert::ToObjectName;

pub struct Update {
    core_context: Arc<Mutex<GlobalContext>>,
    table_name: ObjectName,
    assignments: Vec<Assignment>,
    execution_plan: Arc<ExecutionPlan>,
}

impl Update {
    pub fn new(
        core_context: Arc<Mutex<GlobalContext>>,
        table_name: ObjectName,
        assignments: Vec<Assignment>,
        execution_plan: Arc<ExecutionPlan>,
    ) -> Self {
        Self {
            core_context,
            table_name,
            assignments,
            execution_plan,
        }
    }

    pub async fn execute(&self, session_context: &mut SessionContext) -> MysqlResult<(u64)> {
        let result = collect(self.execution_plan.clone()).await;
        match result {
            Ok(records) => {
                let mut total = 0;
                for record in records {
                    let result = self.update_record(session_context, record);
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

    pub fn update_record(&self, session_context: &mut SessionContext, batch: RecordBatch) -> MysqlResult<(u64)> {
        let full_table_name = meta_util::fill_up_table_name(session_context, self.table_name.clone()).unwrap();

        let catalog_name = meta_util::cut_out_catalog_name(full_table_name.clone());
        let schema_name = meta_util::cut_out_schema_name(full_table_name.clone());
        let table_name = meta_util::cut_out_table_name(full_table_name.clone());

        let mut assignment_column_value: Vec<metadata::ArrayCell> = Vec::new();
        for column_id in 1..batch.num_columns() {
            let schema = &batch.schema();
            let field = schema.field(column_id);
            match field.data_type() {
                DataType::Utf8 => {
                    let c = batch
                        .column(column_id)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    assignment_column_value.push(metadata::ArrayCell::StringArray(c));
                }
                DataType::Int8 => {
                    let c = batch
                        .column(column_id)
                        .as_any()
                        .downcast_ref::<Int8Array>()
                        .unwrap();
                    assignment_column_value.push(metadata::ArrayCell::Int8Array(c));
                }
                DataType::Int16 => {
                    let c = batch
                        .column(column_id)
                        .as_any()
                        .downcast_ref::<Int16Array>()
                        .unwrap();
                    assignment_column_value.push(metadata::ArrayCell::Int16Array(c));
                }
                DataType::Int32 => {
                    let c = batch
                        .column(column_id)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    assignment_column_value.push(metadata::ArrayCell::Int32Array(c));
                }
                DataType::Int64 => {
                    let c = batch
                        .column(column_id)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    assignment_column_value.push(metadata::ArrayCell::Int64Array(c));
                }
                DataType::UInt8 => {
                    let c = batch
                        .column(column_id)
                        .as_any()
                        .downcast_ref::<UInt8Array>()
                        .unwrap();
                    assignment_column_value.push(metadata::ArrayCell::UInt8Array(c));
                }
                DataType::UInt16 => {
                    let c = batch
                        .column(column_id)
                        .as_any()
                        .downcast_ref::<UInt16Array>()
                        .unwrap();
                    assignment_column_value.push(metadata::ArrayCell::UInt16Array(c));
                }
                DataType::UInt32 => {
                    let c = batch
                        .column(column_id)
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .unwrap();
                    assignment_column_value.push(metadata::ArrayCell::UInt32Array(c));
                }
                DataType::UInt64 => {
                    let c = batch
                        .column(column_id)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    assignment_column_value.push(metadata::ArrayCell::UInt64Array(c));
                }
                DataType::Float32 => {
                    let c = batch
                        .column(column_id)
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .unwrap();
                    assignment_column_value.push(metadata::ArrayCell::Float32Array(c));
                }
                DataType::Float64 => {
                    let c = batch
                        .column(column_id)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    assignment_column_value.push(metadata::ArrayCell::Float64Array(c));
                }
                _ => {}
            }
        }

        let rowid_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row_index in 0..rowid_array.len() {
            let rowid = rowid_array.value(row_index);
            for assignment_index in 0..self.assignments.len() {
                let assignment = &self.assignments[assignment_index];
                let mut column_value;
                match assignment_column_value[assignment_index] {
                    metadata::ArrayCell::StringArray(s) => {
                        column_value = s.value(row_index).to_string()
                    }
                    metadata::ArrayCell::Int8Array(s) => {
                        column_value = s.value(row_index).to_string()
                    }
                    metadata::ArrayCell::Int16Array(s) => {
                        column_value = s.value(row_index).to_string()
                    }
                    metadata::ArrayCell::Int32Array(s) => {
                        column_value = s.value(row_index).to_string()
                    }
                    metadata::ArrayCell::Int64Array(s) => {
                        column_value = s.value(row_index).to_string()
                    }
                    metadata::ArrayCell::UInt8Array(s) => {
                        column_value = s.value(row_index).to_string()
                    }
                    metadata::ArrayCell::UInt16Array(s) => {
                        column_value = s.value(row_index).to_string()
                    }
                    metadata::ArrayCell::UInt32Array(s) => {
                        column_value = s.value(row_index).to_string()
                    }
                    metadata::ArrayCell::UInt64Array(s) => {
                        column_value = s.value(row_index).to_string()
                    }
                    metadata::ArrayCell::Float32Array(s) => {
                        column_value = s.value(row_index).to_string()
                    }
                    metadata::ArrayCell::Float64Array(s) => {
                        column_value = s.value(row_index).to_string()
                    }
                }

                let column_name = &assignment.id;

                let column_index = self.core_context.lock().unwrap().meta_cache.get_serial_number(full_table_name.clone(), column_name.clone()).unwrap();
                let recordPutKey = util::dbkey::create_record_column(full_table_name.clone(), column_index, rowid.as_ref());
                let result = self.core_context.lock().unwrap().rocksdb_db.put(recordPutKey.to_vec(), column_value);
                match result {
                    Err(error) => {
                        return Err(MysqlError::new_global_error(1105, format!(
                            "Unknown error. An error occurred while updating the key, key: {:?}, error: {:?}",
                            recordPutKey,
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
