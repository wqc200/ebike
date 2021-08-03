use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::{Mutex, Arc};

use arrow::array::{as_primitive_array, as_string_array};
use arrow::array::{Array, ArrayData, BinaryArray, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array, Float32Array, Float64Array, StringArray};
use arrow::datatypes::{SchemaRef};
use arrow::datatypes::{DataType, Field, Schema, ToByteSlice};
use arrow::record_batch::RecordBatch;
use datafusion::error::{Result};
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::{Expr, LogicalPlan};
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{ColumnDef, ColumnOption, HiveDistributionStyle, Ident, ObjectName, SqlOption, Statement, TableConstraint, Value};

use crate::core::global_context::GlobalContext;
use crate::core::output::CoreOutput;
use crate::core::output::FinalCount;
use crate::core::core_util;
use crate::core::session_context::SessionContext;
use crate::datafusion_impl::datasource::rocksdb::RocksdbTable;
use crate::meta::initial::information_schema;
use crate::meta::{meta_util, scalar_value};
use crate::mysql::error::{MysqlResult, MysqlError};
use crate::mysql::metadata;
use crate::physical_plan::insert::Insert;
use crate::store::engine::sled::SledOperator;
use crate::util;
use crate::mysql::metadata::ArrayCell;
use arrow::compute::not;

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

    pub fn execute(&self) -> MysqlResult<(SchemaRef, Vec<RecordBatch>)> {
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

        Ok((schema.clone(), vec![record_batch]))
    }
}
