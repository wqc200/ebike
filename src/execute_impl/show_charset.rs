use std::sync::{Mutex, Arc};

use arrow::array::StringArray;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use sqlparser::ast::Ident;

use crate::core::global_context::GlobalContext;
use crate::core::output::ResultSet;
use crate::core::session_context::SessionContext;
use crate::mysql::error::MysqlResult;

pub struct ShowCharset {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl ShowCharset {
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

    pub fn execute(&self) -> MysqlResult<ResultSet> {
        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("Charset", DataType::Utf8, false),
            Field::new("Description", DataType::Utf8, false),
            Field::new("Default collation", DataType::Utf8, false),
            Field::new("Maxlen", DataType::Utf8, false),
        ]));

        let column_values_of_charset = StringArray::from(vec![
            "utf8", "utf8mb4",
        ]);
        let column_values_of_description = StringArray::from(vec![
            "UTF-8 Unicode", "UTF-8 Unicode",
        ]);
        let column_values_of_default_collation = StringArray::from(vec![
            "utf8_general_ci", "utf8mb4_0900_ai_ci",
        ]);
        let column_values_of_maxlen = StringArray::from(vec![
            "3", "4",
        ]);
        let record_batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(column_values_of_charset),
            Arc::new(column_values_of_description),
            Arc::new(column_values_of_default_collation),
            Arc::new(column_values_of_maxlen),
        ]).unwrap();

        Ok(ResultSet::new(schema, vec![record_batch]))
    }
}
