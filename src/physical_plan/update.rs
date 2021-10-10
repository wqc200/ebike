use std::sync::{Arc, Mutex};

use arrow::array::{Array};
use arrow::array::{
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
use arrow::datatypes::{DataType};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{collect, ExecutionPlan};
use sqlparser::ast::{Assignment};

use crate::mysql::{metadata};
use crate::core::global_context::GlobalContext;

use crate::mysql::error::{MysqlError, MysqlResult};

use crate::core::session_context::SessionContext;
use crate::store::engine::engine_util::StoreEngineFactory;
use crate::util::dbkey::create_column_key;
use crate::meta::meta_def::TableDef;

pub struct Update {
    global_context: Arc<Mutex<GlobalContext>>,
    table: TableDef,
    assignments: Vec<Assignment>,
    execution_plan: Arc<ExecutionPlan>,
}

impl Update {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table: TableDef,
        assignments: Vec<Assignment>,
        execution_plan: Arc<ExecutionPlan>,
    ) -> Self {
        Self {
            global_context,
            table,
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
        let store_engine = StoreEngineFactory::try_new_with_table(self.global_context.clone(), self.table.clone()).unwrap();

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

                let sparrow_column = self.table.get_table_column().get_sparrow_column(column_name.clone()).unwrap();
                let store_id = sparrow_column.store_id;

                let record_column_key = create_column_key(self.table.option.full_table_name.clone(), store_id, rowid.as_ref());
                let result = store_engine.put_key(record_column_key.clone(), column_value.as_bytes());
                match result {
                    Err(error) => {
                        return Err(MysqlError::new_global_error(1105, format!(
                            "Unknown error. An error occurred while updating the key, key: {:?}, error: {:?}",
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
