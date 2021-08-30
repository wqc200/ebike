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
use crate::mysql::error::MysqlResult;
use crate::physical_plan::create_table::CreateTable;
use crate::store::engine::engine_util;
use crate::store::reader::rocksdb::RocksdbReader;
use crate::store::rocksdb::db::DB;
use crate::store::rocksdb::option::Options;
use crate::util::convert::{ToObjectName, ToIdent};
use crate::physical_plan::insert::PhysicalPlanInsert;

pub fn global_variables() -> def::TableDef {
    let mut with_option = vec![];
    let sql_option = SqlOption { name: Ident { value: meta_const::OPTION_TABLE_TYPE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_TABLE_TYPE_SYSTEM_VIEW.to_string()) };
    with_option.push(sql_option);
    let sql_option = SqlOption { name: Ident { value: meta_const::OPTION_ENGINE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_ENGINE_NAME_ROCKSDB.to_string()) };
    with_option.push(sql_option);

    let sql_columns = vec![
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_NAME, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_VALUE, SQLDataType::Varchar(Some(512)), ColumnOption::Null),
    ];

    let mut columns = vec![];
    columns.push(Ident::new(meta_const::COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_NAME));
    let table_constraint = TableConstraint::Unique {
        name: Some(Ident { value: meta_const::PRIMARY_NAME.to_string(), quote_style: None }),
        columns,
        is_primary: true,
    };
    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES.to_object_name();
    let constraints = vec![table_constraint];
    let table_def = def::TableDef::new_with_sqlcolumn(full_table_name, sql_columns, constraints, with_option);
    table_def
}

pub fn global_variables_data(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<u64> {
    let mut column_name_list = vec![];
    for column_def in global_variables().get_columns() {
        column_name_list.push(column_def.sql_column.name.to_string());
    }

    let mut column_value_map_list: Vec<HashMap<Ident, ScalarValue>> = vec![];
    let mut column_value_map = HashMap::new();
    column_value_map.insert("variable_name".to_ident(), ScalarValue::Utf8(Some("auto_increment_increment".to_string())));
    column_value_map.insert("variable_value".to_ident(), ScalarValue::Utf8(Some("0".to_string())));
    column_value_map_list.push(column_value_map);
    let mut column_value_map = HashMap::new();
    column_value_map.insert("variable_name".to_ident(), ScalarValue::Utf8(Some("lower_case_table_names".to_string())));
    column_value_map.insert("variable_value".to_ident(), ScalarValue::Utf8(Some("1".to_string())));
    column_value_map_list.push(column_value_map);
    let mut column_value_map = HashMap::new();
    column_value_map.insert("variable_name".to_ident(), ScalarValue::Utf8(Some("transaction_isolation".to_string())));
    column_value_map.insert("variable_value".to_ident(), ScalarValue::Utf8(None));
    column_value_map_list.push(column_value_map);
    let mut column_value_map = HashMap::new();
    column_value_map.insert("variable_name".to_ident(), ScalarValue::Utf8(Some("transaction_read_only".to_string())));
    column_value_map.insert("variable_value".to_ident(), ScalarValue::Utf8(Some("0".to_string())));
    column_value_map_list.push(column_value_map);

    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES.to_object_name();
    let table_def = global_variables();

    let insert = PhysicalPlanInsert::new(
        global_context.clone(),
        full_table_name,
        table_def,
        column_name_list.clone(),
        vec![],
        column_value_map_list.clone(),
    );
    let total = insert.execute().unwrap();

    Ok(total)
}
