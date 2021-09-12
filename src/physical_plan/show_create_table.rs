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

pub struct ShowCreateTable {
    global_context: Arc<Mutex<GlobalContext>>,
    table_name: String,
}

impl ShowCreateTable {
    pub fn new(
        core_context: Arc<Mutex<GlobalContext>>,
        table_name: &str,
    ) -> Self {
        Self {
            global_context: core_context,
            table_name: table_name.to_string(),
        }
    }

    pub fn execute(&self, columns_record: Vec<RecordBatch>, statistics_record: Vec<RecordBatch>, tables_record: Vec<RecordBatch>) -> MysqlResult<(SchemaRef, Vec<RecordBatch>)> {
        let table_name = self.table_name.clone();

        let record_batch = columns_record.get(0).unwrap();
        let schema = record_batch.schema();
        let column_index_of_column_name = schema.index_of("COLUMN_NAME").unwrap();
        let column_index_of_data_type = schema.index_of("DATA_TYPE").unwrap();
        let column_index_of_is_nullable = schema.index_of("IS_NULLABLE").unwrap();
        let columns_rows = core_util::convert_record_to_scalar_value(record_batch.clone());
        let mut columns = vec![];
        for row_index in 0..record_batch.num_rows() {
            let row = columns_rows.get(row_index).unwrap();

            /// column name
            let value = row.get(column_index_of_column_name).unwrap();
            let column_name = scalar_value::to_utf8(value.clone()).unwrap();
            /// data type
            let value = row.get(column_index_of_data_type).unwrap();
            let text_data_type = scalar_value::to_utf8(value.clone()).unwrap();
            let sql_data_type = meta_util::text_to_sql_data_type(text_data_type.as_str()).unwrap();
            /// nullable
            let value = row.get(column_index_of_is_nullable).unwrap();
            let text_is_nullable = scalar_value::to_utf8(value.clone()).unwrap();
            let nullable = meta_util::text_to_null(text_is_nullable.as_str()).unwrap();
            /// create sql column
            let sql_column = meta_util::create_sql_column(column_name.as_str(), sql_data_type, nullable);

            columns.push(sql_column);
        }

        let object_name = meta_util::convert_to_object_name(table_name.as_str());
        let create_table = Statement::CreateTable {
            or_replace: false,
            temporary: false,
            external: false,
            if_not_exists: false,
            name: object_name,
            columns,
            constraints: vec![],
            hive_distribution: HiveDistributionStyle::NONE,
            hive_formats: None,
            table_properties: vec![],
            with_options: vec![],
            file_format: None,
            location: None,
            query: None,
            without_rowid: false,
            like: None,
            table_options: vec![],
        };

        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("Table", DataType::Utf8, false),
            Field::new("Create Table", DataType::Utf8, false),
        ]));
        let column_table_name = StringArray::from(vec![table_name.as_str()]);
        let column_create_table = StringArray::from(vec![create_table.to_string().as_str()]);
        let record_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(column_table_name), Arc::new(column_create_table)]).unwrap();

        Ok((schema.clone(), vec![record_batch]))
    }
}
