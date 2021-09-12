use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::io::prelude::*;
use std::process::*;
use std::sync::{Arc, Mutex};

use arrow::array;
use arrow::array::{
    ArrayData,
    BinaryArray,
    Float32Array,
    Float64Array,
    Int16Array,
    Int32Array,
    Int64Array,
    Int8Array,
    StringArray,
    UInt16Array,
    UInt32Array,
    UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::datatypes::ToByteSlice;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::csv::{CsvFile, CsvReadOptions};
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_plan::{col, DFField, DFSchema, DFSchemaRef, Expr};
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{Assignment, ColumnDef as SQLColumnDef, ColumnOption, ColumnOptionDef, DataType as SQLDataType, Ident, ObjectName, SqlOption, TableConstraint, Value};
use tempdir::TempDir;
use uuid::Uuid;

use crate::core::global_context::GlobalContext;
use crate::meta::{def, meta_const, meta_util};
use crate::meta::initial::initial_util::create_table;
use crate::mysql::error::MysqlResult;
use crate::physical_plan::create_table::CreateTable;
use crate::physical_plan::insert::PhysicalPlanInsert;
use crate::store::engine::engine_util;
use crate::store::reader::rocksdb::RocksdbReader;
use crate::store::rocksdb::db::DB;
use crate::store::rocksdb::option::Options;
use crate::util::convert::{ToIdent, ToObjectName};

pub fn global_variables(global_context: Arc<Mutex<GlobalContext>>) -> def::TableDef {
    let sql_column_list = vec![
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_NAME, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_VALUE, SQLDataType::Varchar(Some(512)), ColumnOption::Null),
    ];

    let mut columns = vec![];
    columns.push(Ident::new(meta_const::COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_NAME));
    let table_constraint = TableConstraint::Unique {
        name: Some(Ident { value: meta_const::NAME_OF_PRIMARY.to_string(), quote_style: None }),
        columns,
        is_primary: true,
    };
    let constraints = vec![table_constraint];

    create_table(
        global_context.clone(),
        meta_const::SCHEMA_NAME_OF_DEF_PERFORMANCE_SCHEMA,
        meta_const::TABLE_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES,
        sql_column_list.clone(),
        constraints.clone(),
    )
}
