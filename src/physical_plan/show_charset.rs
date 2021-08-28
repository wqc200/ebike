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
use crate::physical_plan::insert::PhysicalPlanInsert;

use crate::util;
use crate::mysql::metadata::ArrayCell;
use arrow::compute::not;

pub struct ShowCharset {
    global_context: Arc<Mutex<GlobalContext>>,
}

impl ShowCharset {
    pub fn new(
        core_context: Arc<Mutex<GlobalContext>>,
    ) -> Self {
        Self {
            global_context: core_context,
        }
    }

    pub fn execute(&self) -> MysqlResult<(SchemaRef, Vec<RecordBatch>)> {
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

        Ok((schema.clone(), vec![record_batch]))
    }
}
