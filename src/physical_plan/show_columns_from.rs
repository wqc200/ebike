use std::collections::{HashMap};
use std::sync::{Mutex, Arc};

use arrow::array::{Array, StringArray};
use arrow::datatypes::{SchemaRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::core::global_context::GlobalContext;
use crate::core::core_util;
use crate::meta::{meta_util, scalar_value};
use crate::mysql::error::{MysqlResult};

pub struct ShowColumnsFrom {
    global_context: Arc<Mutex<GlobalContext>>,
}

impl ShowColumnsFrom {
    pub fn new(
        core_context: Arc<Mutex<GlobalContext>>,
    ) -> Self {
        Self {
            global_context: core_context,
        }
    }

    pub fn execute(&self, columns_record: Vec<RecordBatch>, statistics_record: Vec<RecordBatch>) -> MysqlResult<(SchemaRef, Vec<RecordBatch>)> {
        let mut statistics_map: HashMap<String, String> = HashMap::new();

        if statistics_record.len() > 0 {
            let record_batch = statistics_record.get(0).unwrap();
            let schema = record_batch.schema();
            let column_index_of_column_name = schema.index_of("COLUMN_NAME").unwrap();
            let column_index_of_index_name = schema.index_of("INDEX_NAME").unwrap();
            let statistics_rows = core_util::convert_record_to_scalar_value(record_batch.clone());
            for row_index in 0..record_batch.num_rows() {
                let row = statistics_rows.get(row_index).unwrap();

                /// column name
                let value = row.get(column_index_of_column_name).unwrap();
                let column_name = scalar_value::to_utf8(value.clone()).unwrap();
                /// index name
                let value = row.get(column_index_of_index_name).unwrap();
                let index_name = scalar_value::to_utf8(value.clone()).unwrap();

                statistics_map.insert(column_name, index_name);
            }
        }

        let record_batch = columns_record.get(0).unwrap();
        let schema = record_batch.schema();
        let column_index_of_column_name = schema.index_of("COLUMN_NAME").unwrap();
        let column_index_of_data_type = schema.index_of("DATA_TYPE").unwrap();
        let column_index_of_is_nullable = schema.index_of("IS_NULLABLE").unwrap();
        let columns_rows = core_util::convert_record_to_scalar_value(record_batch.clone());
        let mut column_fields = vec![];
        let mut column_types = vec![];
        let mut column_nulls = vec![];
        let mut column_keys = vec![];
        for row_index in 0..record_batch.num_rows() {
            let row = columns_rows.get(row_index).unwrap();

            /// column name
            let value = row.get(column_index_of_column_name).unwrap();
            let column_name = scalar_value::to_utf8(value.clone()).unwrap();
            column_fields.push(column_name.clone());
            /// data type
            let value = row.get(column_index_of_data_type).unwrap();
            let text_data_type = scalar_value::to_utf8(value.clone()).unwrap();
            let sql_data_type = meta_util::text_to_sql_data_type(text_data_type.as_str()).unwrap();
            column_types.push(sql_data_type.to_string());
            /// nullable
            let value = row.get(column_index_of_is_nullable).unwrap();
            let text_is_nullable = scalar_value::to_utf8(value.clone()).unwrap();
            let nullable = meta_util::text_to_null(text_is_nullable.as_str()).unwrap();
            column_nulls.push(nullable.to_string());
            /// key
            if let Some(value) = statistics_map.get(column_name.clone().as_str()) {
                column_keys.push(value.clone());
            } else {
                column_keys.push("".to_string());
            }
        }

        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("Field", DataType::Utf8, false),
            Field::new("Type", DataType::Utf8, false),
            Field::new("Null", DataType::Utf8, false),
            Field::new("Key", DataType::Utf8, false),
        ]));

        let column_fields = column_fields.iter().map(|x|x.as_str()).collect::<Vec<_>>();
        let column_types = column_types.iter().map(|x|x.as_str()).collect::<Vec<_>>();
        let column_nulls = column_nulls.iter().map(|x|x.as_str()).collect::<Vec<_>>();
        let column_keys = column_keys.iter().map(|x|x.as_str()).collect::<Vec<_>>();
        let column_table_name = StringArray::from(column_fields);
        let column_type = StringArray::from(column_types);
        let column_null = StringArray::from(column_nulls);
        let column_keys = StringArray::from(column_keys);
        let record_batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(column_table_name),
            Arc::new(column_type),
            Arc::new(column_null),
            Arc::new(column_keys),
        ]).unwrap();

        Ok((schema.clone(), vec![record_batch]))
    }
}
