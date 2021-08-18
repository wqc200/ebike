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
use crate::store::engine::sled::SledOperator;
use crate::util;
use crate::mysql::metadata::ArrayCell;
use arrow::compute::not;
use crate::meta::def::TableDef;

pub struct ComFieldList {
    global_context: Arc<Mutex<GlobalContext>>,
    table_name: ObjectName,
}

impl ComFieldList {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table_name: ObjectName,
    ) -> Self {
        Self {
            global_context,
            table_name,
        }
    }

    pub fn execute(&self, session_context: &mut SessionContext) -> MysqlResult<(ObjectName, ObjectName, TableDef)> {
        let full_table_name = meta_util::fill_up_table_name(session_context, self.table_name.clone()).unwrap();

        let schema_name = meta_util::cut_out_schema_name(full_table_name.clone());
        let table_name = meta_util::cut_out_table_name(full_table_name.clone());

        let table_def = self.global_context.lock().unwrap().meta_cache.get_table(full_table_name.clone()).unwrap().clone();
        Ok((schema_name, table_name, table_def))
    }
}
