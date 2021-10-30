use std::sync::{Mutex, Arc};

use arrow::array::{StringArray};
use arrow::datatypes::{SchemaRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::core::global_context::GlobalContext;
use crate::mysql::error::{MysqlResult};
use crate::core::output::ResultSet;

pub struct ShowEngines {
    global_context: Arc<Mutex<GlobalContext>>,
}

impl ShowEngines {
    pub fn new(
        core_context: Arc<Mutex<GlobalContext>>,
    ) -> Self {
        Self {
            global_context: core_context,
        }
    }

    pub fn execute(&self) -> MysqlResult<ResultSet> {
        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("Engine", DataType::Utf8, false),
            Field::new("Support", DataType::Utf8, false),
            Field::new("Comment", DataType::Utf8, false),
            Field::new("Transactions", DataType::Utf8, false),
            Field::new("XA", DataType::Utf8, false),
            Field::new("Savepoints", DataType::Utf8, false),
        ]));

        let column_values_of_engine = StringArray::from(vec![
            "MEMORY", "CSV", "Rocksdb",
        ]);
        let column_values_of_support = StringArray::from(vec![
            "YES", "YES", "DEFAULT",
        ]);
        let column_values_of_comment = StringArray::from(vec![
            "Hash based, stored in memory, useful for temporary tables",
            "CSV storage engine",
            "Supports transactions, row-level locking, and foreign keys",
        ]);
        let column_values_of_transactions = StringArray::from(vec![
            "NO", "NO", "NO",
        ]);
        let column_values_of_xa = StringArray::from(vec![
            "NO", "NO", "NO",
        ]);
        let column_values_of_savepoints = StringArray::from(vec![
            "NO", "NO", "NO",
        ]);
        let record_batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(column_values_of_engine),
            Arc::new(column_values_of_support),
            Arc::new(column_values_of_comment),
            Arc::new(column_values_of_transactions),
            Arc::new(column_values_of_xa),
            Arc::new(column_values_of_savepoints),
        ]).unwrap();

        Ok(ResultSet::new(schema, vec![record_batch]))
    }
}
