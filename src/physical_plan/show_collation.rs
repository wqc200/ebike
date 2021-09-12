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
use crate::meta::def::information_schema;
use crate::meta::{meta_util, scalar_value};
use crate::mysql::error::{MysqlResult, MysqlError};
use crate::mysql::metadata;
use crate::physical_plan::insert::PhysicalPlanInsert;

use crate::util;
use crate::mysql::metadata::ArrayCell;
use arrow::compute::not;

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
