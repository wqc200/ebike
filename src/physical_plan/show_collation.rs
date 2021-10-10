use std::sync::{Mutex, Arc};

use arrow::array::{StringArray};
use arrow::datatypes::{SchemaRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::core::global_context::GlobalContext;
use crate::mysql::error::{MysqlResult};

pub struct ShowCollation {
    global_context: Arc<Mutex<GlobalContext>>,
}

impl ShowCollation {
    pub fn new(
        core_context: Arc<Mutex<GlobalContext>>,
    ) -> Self {
        Self {
            global_context: core_context,
        }
    }

    pub fn execute(&self) -> MysqlResult<(SchemaRef, Vec<RecordBatch>)> {
        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("Collation", DataType::Utf8, false),
            Field::new("Charset", DataType::Utf8, false),
            Field::new("Id", DataType::Utf8, false),
            Field::new("Default", DataType::Utf8, false),
            Field::new("Compiled", DataType::Utf8, false),
            Field::new("Sortlen", DataType::Utf8, false),
            Field::new("Pad_attribute", DataType::Utf8, false),
        ]));

        let column_values_of_collation = StringArray::from(vec![
            "utf8mb4_general_ci", "utf8_general_ci",
        ]);
        let column_values_of_charset = StringArray::from(vec![
            "utf8mb4", "utf8",
        ]);
        let column_values_of_id = StringArray::from(vec![
            "45",
            "33",
        ]);
        let column_values_of_default = StringArray::from(vec![
            "", "Yes",
        ]);
        let column_values_of_compiled = StringArray::from(vec![
            "Yes", "Yes",
        ]);
        let column_values_of_sortlen = StringArray::from(vec![
            "1", "1",
        ]);
        let column_values_of_pad_attribute = StringArray::from(vec![
            "PAD SPACE", "PAD SPACE",
        ]);
        let record_batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(column_values_of_collation),
            Arc::new(column_values_of_charset),
            Arc::new(column_values_of_id),
            Arc::new(column_values_of_default),
            Arc::new(column_values_of_compiled),
            Arc::new(column_values_of_sortlen),
            Arc::new(column_values_of_pad_attribute),
        ]).unwrap();

        Ok((schema.clone(), vec![record_batch]))
    }
}
