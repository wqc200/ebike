use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::io::prelude::*;
use std::process::*;
use std::sync::Arc;

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
use sqlparser::ast::{Assignment, ColumnDef as SQLColumnDef, ColumnOption, ColumnOptionDef, DataType as SQLDataType, Ident, ObjectName, SqlOption, TableConstraint, Value};
use tempdir::TempDir;
use uuid::Uuid;

use crate::core::global_context::GlobalContext;
use crate::meta::{def, meta_const, meta_util};
use crate::physical_plan::create_table::CreateTable;
use crate::store::reader::rocksdb::RocksdbReader;
use crate::store::rocksdb::db::DB;
use crate::store::rocksdb::option::Options;
use crate::util::convert::ToObjectName;

pub fn table_columns() -> def::TableDef {
    let mut with_option = vec![];
    let sql_option = SqlOption { name: Ident { value: meta_const::TABLE_OPTION_OF_TABLE_TYPE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_TABLE_TYPE_SYSTEM_VIEW.to_string()) };
    with_option.push(sql_option);
    let sql_option = SqlOption { name: Ident { value: meta_const::TABLE_OPTION_OF_ENGINE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_ENGINE_NAME_ROCKSDB.to_string()) };
    with_option.push(sql_option);

    let mut idents = vec![];
    idents.push(Ident::new(""));
    idents.push(Ident::new("PRI"));
    idents.push(Ident::new("UNI"));
    idents.push(Ident::new("MUL"));
    let object_name = ObjectName(idents);

    let sql_columns = vec![
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_CATALOG, SQLDataType::Varchar(Some(64)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_SCHEMA, SQLDataType::Varchar(Some(64)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_NAME, SQLDataType::Varchar(Some(64)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_COLUMN_NAME, SQLDataType::Varchar(Some(64)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_STORE_ID, SQLDataType::Int, ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_ORDINAL_POSITION, SQLDataType::Int, ColumnOption::NotNull),
        meta_util::create_sql_column("COLUMN_DEFAULT", SQLDataType::Varchar(Some(512)), ColumnOption::Null),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_IS_NULLABLE, SQLDataType::Varchar(Some(3)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_DATA_TYPE, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("CHARACTER_MAXIMUM_LENGTH", SQLDataType::Int, ColumnOption::Null),
        meta_util::create_sql_column("CHARACTER_OCTET_LENGTH", SQLDataType::Int, ColumnOption::Null),
        meta_util::create_sql_column("NUMERIC_PRECISION", SQLDataType::Int, ColumnOption::Null),
        meta_util::create_sql_column("NUMERIC_SCALE", SQLDataType::Int, ColumnOption::Null),
        meta_util::create_sql_column("DATETIME_PRECISION", SQLDataType::Int, ColumnOption::Null),
        meta_util::create_sql_column("CHARACTER_SET_NAME", SQLDataType::Varchar(Some(512)), ColumnOption::Null),
        meta_util::create_sql_column("COLLATION_NAME", SQLDataType::Varchar(Some(512)), ColumnOption::Null),
        meta_util::create_sql_column("COLUMN_TYPE", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("COLUMN_KEY", SQLDataType::Custom(object_name), ColumnOption::NotNull),
        meta_util::create_sql_column("EXTRA", SQLDataType::Varchar(Some(512)), ColumnOption::Null),
        meta_util::create_sql_column("PRIVILEGES", SQLDataType::Varchar(Some(512)), ColumnOption::Null),
        meta_util::create_sql_column("COLUMN_COMMENT", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("GENERATION_EXPRESSION", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("SRS_ID", SQLDataType::Int, ColumnOption::Null),
    ];

    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS.to_object_name();
    let constraints = vec![];
    let table_def = def::TableDef::new_with_sqlcolumn(full_table_name, sql_columns, constraints, with_option);
    table_def
}

pub fn table_tables() -> def::TableDef {
    let mut with_option = vec![];
    let sql_option = SqlOption { name: Ident { value: meta_const::TABLE_OPTION_OF_TABLE_TYPE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_TABLE_TYPE_SYSTEM_VIEW.to_string()) };
    with_option.push(sql_option);
    let sql_option = SqlOption { name: Ident { value: meta_const::TABLE_OPTION_OF_ENGINE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_ENGINE_NAME_ROCKSDB.to_string()) };
    with_option.push(sql_option);

    let columns = vec![
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_CATALOG, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_INFORMATION_SCHEMA_TABLE_SCHEMA, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_INFORMATION_SCHEMA_TABLE_NAME, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_TYPE, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_ENGINE, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_VERSION, SQLDataType::Int, ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_DATA_LENGTH, SQLDataType::Int, ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_INDEX_LENGTH, SQLDataType::Int, ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_AUTO_INCREMENT, SQLDataType::Int, ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_COLUMN_MAX_STORE_ID, SQLDataType::Int, ColumnOption::NotNull),
    ];

    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES.to_object_name();
    let constraints = vec![];
    let table_def = def::TableDef::new_with_sqlcolumn(full_table_name, columns, constraints, with_option);
    table_def
}

pub fn table_schemata() -> def::TableDef {
    let mut with_option = vec![];
    let sql_option = SqlOption { name: Ident { value: meta_const::TABLE_OPTION_OF_TABLE_TYPE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_TABLE_TYPE_SYSTEM_VIEW.to_string()) };
    with_option.push(sql_option);
    let sql_option = SqlOption { name: Ident { value: meta_const::TABLE_OPTION_OF_ENGINE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_ENGINE_NAME_ROCKSDB.to_string()) };
    with_option.push(sql_option);

    let columns = vec![
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_CATALOG_NAME, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_SCHEMA_NAME, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_DEFAULT_CHARACTER_SET_NAME, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_DEFAULT_COLLATION_NAME, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("SQL_PATH", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("DEFAULT_ENCRYPTION", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
    ];

    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA.to_object_name();
    let constraints = vec![];
    let table_def = def::TableDef::new_with_sqlcolumn(full_table_name, columns, constraints, with_option);
    table_def
}

pub fn table_statistics() -> def::TableDef {
    let mut with_option = vec![];
    let sql_option = SqlOption { name: Ident { value: meta_const::TABLE_OPTION_OF_TABLE_TYPE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_TABLE_TYPE_SYSTEM_VIEW.to_string()) };
    with_option.push(sql_option);
    let sql_option = SqlOption { name: Ident { value: meta_const::TABLE_OPTION_OF_ENGINE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_ENGINE_NAME_ROCKSDB.to_string()) };
    with_option.push(sql_option);

    let columns = vec![
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_CATALOG, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_SCHEMA, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_NAME, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_NON_UNIQUE, SQLDataType::Int, ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_INDEX_NAME, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_SEQ_IN_INDEX, SQLDataType::Int, ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_COLUMN_NAME, SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
    ];

    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS.to_object_name();
    let constraints = vec![];
    let table_def = def::TableDef::new_with_sqlcolumn(full_table_name, columns, constraints, with_option);
    table_def
}

pub fn key_column_usage() -> def::TableDef {
    let mut with_option = vec![];
    let sql_option = SqlOption { name: Ident { value: meta_const::TABLE_OPTION_OF_TABLE_TYPE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_TABLE_TYPE_SYSTEM_VIEW.to_string()) };
    with_option.push(sql_option);
    let sql_option = SqlOption { name: Ident { value: meta_const::TABLE_OPTION_OF_ENGINE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_ENGINE_NAME_ROCKSDB.to_string()) };
    with_option.push(sql_option);

    let columns = vec![
        meta_util::create_sql_column("constraint_catalog", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("constraint_schema", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("constraint_name", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("table_catalog", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("table_schema", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("table_name", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("column_name", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("ordinal_position", SQLDataType::Int, ColumnOption::NotNull),
        meta_util::create_sql_column("position_in_unique_constraint", SQLDataType::Int, ColumnOption::Null),
        meta_util::create_sql_column("referenced_table_schema", SQLDataType::Varchar(Some(512)), ColumnOption::Null),
        meta_util::create_sql_column("referenced_table_name", SQLDataType::Varchar(Some(512)), ColumnOption::Null),
        meta_util::create_sql_column("referenced_column_name", SQLDataType::Varchar(Some(512)), ColumnOption::Null),
    ];

    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_KEY_COLUMN_USAGE.to_object_name();
    let constraints = vec![];
    let table_def = def::TableDef::new_with_sqlcolumn(full_table_name, columns, constraints, with_option);
    table_def
}

pub fn table_constraints() -> def::TableDef {
    let mut with_option = vec![];
    let sql_option = SqlOption { name: Ident { value: meta_const::TABLE_OPTION_OF_TABLE_TYPE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_TABLE_TYPE_SYSTEM_VIEW.to_string()) };
    with_option.push(sql_option);
    let sql_option = SqlOption { name: Ident { value: meta_const::TABLE_OPTION_OF_ENGINE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_ENGINE_NAME_ROCKSDB.to_string()) };
    with_option.push(sql_option);

    let columns = vec![
        meta_util::create_sql_column("constraint_catalog", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("constraint_schema", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("constraint_name", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("table_schema", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("table_name", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("constraint_type", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("enforced", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
    ];

    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLE_CONSTRAINTS.to_object_name();
    let constraints = vec![];
    let table_def = def::TableDef::new_with_sqlcolumn(full_table_name, columns, constraints, with_option);
    table_def
}
