use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::{Mutex, Arc};

use arrow::array::{as_primitive_array, as_string_array, ArrayRef};
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
use crate::core::core_util;

pub struct ShowPrivileges {
    global_context: Arc<Mutex<GlobalContext>>,
}

impl ShowPrivileges {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
    ) -> Self {
        Self {
            global_context,
        }
    }

    pub fn execute(&self) -> MysqlResult<(SchemaRef, Vec<RecordBatch>)> {
        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("Privilege", DataType::Utf8, false),
            Field::new("Context", DataType::Utf8, false),
            Field::new("Comment", DataType::Utf8, false),
        ]));

        let column_values0 = StringArray::from(vec![
            "Alter", "Alter routine", "Create",
            "TABLE_ENCRYPTION_ADMIN",
        ]);
        let column_values1 = StringArray::from(vec![
            "Tables", "Functions,Procedures", "Databases,Tables,Indexes",
            "Server Admin",
        ]);
        let column_values2 = StringArray::from(vec![
            "To alter the table", "To alter or drop stored functions/procedures", "To create new databases and tables",
            "",
        ]);
        let record_batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(column_values0),
            Arc::new(column_values1),
            Arc::new(column_values2),
        ]).unwrap();

        Ok((schema.clone(), vec![record_batch]))
    }
}
