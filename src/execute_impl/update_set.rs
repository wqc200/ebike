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
use sqlparser::ast::{Assignment, ObjectName, SetExpr, Query};

use crate::mysql::{metadata};
use crate::core::global_context::GlobalContext;

use crate::mysql::error::{MysqlError, MysqlResult};

use crate::core::session_context::SessionContext;
use crate::store::engine::engine_util::StoreEngineFactory;
use crate::util::dbkey::create_column_key;
use crate::meta::meta_def::TableDef;
use datafusion::execution::context::ExecutionContext;
use crate::meta::meta_util;
use crate::core::core_util;
use crate::execute_impl::select_from::SelectFrom;

pub struct UpdateSet {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl UpdateSet {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        session_context: SessionContext,
        execution_context: ExecutionContext,
    ) -> Self {
        Self {
            global_context,
            session_context,
            execution_context,
        }
    }

    pub fn execute(
        &mut self,
        table_name: ObjectName,
        assignments: Vec<Assignment>,
        selection: Option<SQLExpr>,
    ) -> MysqlResult<u64> {
        let full_table_name =
            meta_util::fill_up_table_name(&mut self.session_context, table_name.clone()).unwrap();

        let table_map = self
            .global_context
            .lock()
            .unwrap()
            .meta_data
            .get_table_map();
        let table = match table_map.get(&full_table_name) {
            None => {
                let message = format!("Table '{}' doesn't exist", table_name.to_string());
                log::error!("{}", message);
                return Err(MysqlError::new_server_error(
                    1146,
                    "42S02",
                    message.as_str(),
                ));
            }
            Some(table) => table.clone(),
        };

        let select =
            core_util::build_update_sqlselect(table_name.clone(), assignments.clone(), selection);
        let query = Box::new(Query {
            with: None,
            body: SetExpr::Select(Box::new(select)),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        });
        let mut select_from = SelectFrom::new(
            self.global_context.clone(),
            self.session_context.clone(),
            self.execution_context.clone(),
        );
        let result = select_from.execute(&query).await;
        match result {
            Ok(result_set) => {
                let result = self.update_record_batches(result_set.record_batches);
                result
            }
            Err(mysql_error) => Err(mysql_error),
        }
    }

    fn update_record_batches(&self, record_batches: Vec<RecordBatch>) -> MysqlResult<u64> {
        let mut total = 0;
        for record_batch in record_batches {
            let result = self.update_record_batch(record_batch);
            match result {
                Ok(count) => total += count,
                Err(mysql_error) => return Err(mysql_error),
            }
        }
        Ok(total)
    }

    pub fn update_record_batch(&self, batch: RecordBatch) -> MysqlResult<u64> {
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
                let column_value;
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
