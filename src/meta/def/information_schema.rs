use std::sync::{Arc, Mutex};

use sqlparser::ast::{ColumnOption, DataType as SQLDataType, Ident, ObjectName};

use crate::core::global_context::GlobalContext;
use crate::meta::{meta_const, meta_util};
use crate::meta::meta_def::{TableDef};
use crate::meta::initial::create_table;
use crate::meta::meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_CHARACTER_OCTET_LENGTH;

pub fn columns(global_context: Arc<Mutex<GlobalContext>>) -> TableDef {
    let sql_column_list = vec![
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_CATALOG, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_SCHEMA, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_NAME, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_COLUMN_NAME, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_STORE_ID, SQLDataType::Int(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_ORDINAL_POSITION, SQLDataType::Int(None), ColumnOption::NotNull),
        meta_util::create_sql_column("COLUMN_DEFAULT", SQLDataType::Char(None), ColumnOption::Null),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_IS_NULLABLE, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_DATA_TYPE, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_CHARACTER_MAXIMUM_LENGTH, SQLDataType::Int(None), ColumnOption::Null),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_CHARACTER_OCTET_LENGTH, SQLDataType::Int(None), ColumnOption::Null),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_NUMERIC_PRECISION, SQLDataType::Int(None), ColumnOption::Null),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_NUMERIC_SCALE, SQLDataType::Int(None), ColumnOption::Null),
        meta_util::create_sql_column("DATETIME_PRECISION", SQLDataType::Int(None), ColumnOption::Null),
        meta_util::create_sql_column("CHARACTER_SET_NAME", SQLDataType::Char(None), ColumnOption::Null),
        meta_util::create_sql_column("COLLATION_NAME", SQLDataType::Char(None), ColumnOption::Null),
        meta_util::create_sql_column("COLUMN_TYPE", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("COLUMN_KEY", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("EXTRA", SQLDataType::Char(None), ColumnOption::Null),
        meta_util::create_sql_column("PRIVILEGES", SQLDataType::Char(None), ColumnOption::Null),
        meta_util::create_sql_column("COLUMN_COMMENT", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("GENERATION_EXPRESSION", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("SRS_ID", SQLDataType::Int(None), ColumnOption::Null),
    ];
    let constraints = vec![];

    create_table(
        global_context.clone(),
        meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
        meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS,
        sql_column_list.clone(),
        constraints.clone(),
    )
}

pub fn tables(global_context: Arc<Mutex<GlobalContext>>) -> TableDef {
    let sql_column_list = vec![
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_CATALOG, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_INFORMATION_SCHEMA_TABLE_SCHEMA, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_INFORMATION_SCHEMA_TABLE_NAME, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_TYPE, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_ENGINE, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_VERSION, SQLDataType::Int(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_DATA_LENGTH, SQLDataType::Int(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_INDEX_LENGTH, SQLDataType::Int(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_AUTO_INCREMENT, SQLDataType::Int(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_COLUMN_MAX_STORE_ID, SQLDataType::Int(None), ColumnOption::NotNull),
    ];
    let constraints = vec![];

    create_table(
        global_context.clone(),
        meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
        meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES,
        sql_column_list.clone(),
        constraints.clone(),
    )
}

pub fn schemata(global_context: Arc<Mutex<GlobalContext>>) -> TableDef {
    let sql_column_list = vec![
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_CATALOG_NAME, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_SCHEMA_NAME, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_DEFAULT_CHARACTER_SET_NAME, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_DEFAULT_COLLATION_NAME, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("SQL_PATH", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("DEFAULT_ENCRYPTION", SQLDataType::Char(None), ColumnOption::NotNull),
    ];
    let constraints = vec![];

    create_table(
        global_context.clone(),
        meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
        meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA,
        sql_column_list.clone(),
        constraints.clone(),
    )
}

pub fn statistics(global_context: Arc<Mutex<GlobalContext>>) -> TableDef {
    let sql_column_list = vec![
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_CATALOG, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_SCHEMA, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_NAME, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_NON_UNIQUE, SQLDataType::Int(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_INDEX_NAME, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_SEQ_IN_INDEX, SQLDataType::Int(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_COLUMN_NAME, SQLDataType::Char(None), ColumnOption::NotNull),
    ];
    let constraints = vec![];

    create_table(
        global_context.clone(),
        meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
        meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS,
        sql_column_list.clone(),
        constraints.clone(),
    )
}

pub fn key_column_usage(global_context: Arc<Mutex<GlobalContext>>) -> TableDef {
    let sql_column_list = vec![
        meta_util::create_sql_column("constraint_catalog", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("constraint_schema", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("constraint_name", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("table_catalog", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("table_schema", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("table_name", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("column_name", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("ordinal_position", SQLDataType::Int(None), ColumnOption::NotNull),
        meta_util::create_sql_column("position_in_unique_constraint", SQLDataType::Int(None), ColumnOption::Null),
        meta_util::create_sql_column("referenced_table_schema", SQLDataType::Char(None), ColumnOption::Null),
        meta_util::create_sql_column("referenced_table_name", SQLDataType::Char(None), ColumnOption::Null),
        meta_util::create_sql_column("referenced_column_name", SQLDataType::Char(None), ColumnOption::Null),
    ];
    let constraints = vec![];

    create_table(
        global_context.clone(),
        meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
        meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_KEY_COLUMN_USAGE,
        sql_column_list.clone(),
        constraints.clone(),
    )
}

pub fn table_constraints(global_context: Arc<Mutex<GlobalContext>>) -> TableDef {
    let sql_column_list = vec![
        meta_util::create_sql_column("constraint_catalog", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("constraint_schema", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("constraint_name", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("table_schema", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("table_name", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("constraint_type", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("enforced", SQLDataType::Char(None), ColumnOption::NotNull),
    ];
    let constraints = vec![];

    create_table(
        global_context.clone(),
        meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
        meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLE_CONSTRAINTS,
        sql_column_list.clone(),
        constraints.clone(),
    )
}
