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
use crate::store::reader::rocksdb::Reader;
use crate::store::rocksdb::db::DB;
use crate::store::rocksdb::option::Options;
use crate::util::convert::ToObjectName;

pub fn users() -> def::TableDef {
    let mut with_option = vec![];
    let sql_option = SqlOption { name: Ident { value: meta_const::OPTION_TABLE_TYPE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_TABLE_TYPE_SYSTEM_VIEW.to_string()) };
    with_option.push(sql_option);
    let sql_option = SqlOption { name: Ident { value: meta_const::OPTION_ENGINE.to_string(), quote_style: None }, value: Value::SingleQuotedString(meta_const::OPTION_ENGINE_NAME_ROCKSDB.to_string()) };
    with_option.push(sql_option);

    let sql_columns = vec![
        meta_util::create_sql_column("Host", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("user", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Select_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Insert_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Update_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Delete_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Create_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Drop_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Reload_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Shutdown_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Process_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("File_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Grant_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("References_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Index_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Alter_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Show_db_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Super_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Create_tmp_table_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Lock_tables_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Execute_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Repl_slave_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Repl_client_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Create_view_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Show_view_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Create_routine_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Alter_routine_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Create_user_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Event_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Trigger_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Create_tablespace_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("ssl_type", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("ssl_cipher", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("x509_issuer", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("x509_subject", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("max_questions", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("max_updates", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("max_connections", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("max_user_connections", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("plugin", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("authentication_string", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("password_expired", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("password_last_changed", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("password_lifetime", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("account_locked", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Create_role_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Drop_role_priv", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Password_reuse_history", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Password_reuse_time", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("Password_require_current", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
        meta_util::create_sql_column("User_attributes", SQLDataType::Varchar(Some(512)), ColumnOption::NotNull),
    ];

    let mut columns = vec![];
    columns.push(Ident::new("Host"));
    columns.push(Ident::new("user"));
    let table_constraint = TableConstraint::Unique {
        name: Some(Ident { value: meta_const::PRIMARY_NAME.to_string(), quote_style: None }),
        columns,
        is_primary: true,
    };
    let constraints = vec![table_constraint];

    let table_schema = def::TableDef::new_with_sqlcolumn(meta_const::FULL_TABLE_NAME_OF_DEF_MYSQL_USERS, sql_columns, constraints, with_option);
    table_schema
}

pub fn users_data(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<u64> {
    let mut column_name = vec![];
    for column_def in users().get_columns() {
        column_name.push(column_def.sql_column.name.to_string());
    }

    let mut column_value: Vec<Vec<Expr>> = vec![];
    let row = vec![
        // Host
        Expr::Literal(ScalarValue::Utf8(Some("%".to_string()))),
        // User
        Expr::Literal(ScalarValue::Utf8(Some("root".to_string()))),
        // Select_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Insert_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Update_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Delete_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Create_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Drop_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Reload_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Shutdown_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Process_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // File_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Grant_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // References_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Index_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Alter_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Show_db_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Super_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Create_tmp_table_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Lock_tables_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Execute_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Repl_slave_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Repl_client_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Create_view_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Show_view_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Create_routine_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Alter_routine_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Create_user_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Event_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Trigger_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Create_tablespace_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // ssl_type
        Expr::Literal(ScalarValue::Utf8(None)),
        // ssl_cipher
        Expr::Literal(ScalarValue::Utf8(None)),
        // x509_issuer
        Expr::Literal(ScalarValue::Utf8(None)),
        // x509_subject
        Expr::Literal(ScalarValue::Utf8(None)),
        // max_questions
        Expr::Literal(ScalarValue::UInt64(Some(0))),
        // max_updates
        Expr::Literal(ScalarValue::UInt64(Some(0))),
        // max_connections
        Expr::Literal(ScalarValue::UInt64(Some(0))),
        // max_user_connections
        Expr::Literal(ScalarValue::UInt64(Some(0))),
        // plugin
        Expr::Literal(ScalarValue::Utf8(Some("mysql_native_password".to_string()))),
        // authentication_string
        Expr::Literal(ScalarValue::Utf8(Some("*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9".to_string()))),
        // password_expired
        Expr::Literal(ScalarValue::Utf8(Some("N".to_string()))),
        // password_last_changed
        Expr::Literal(ScalarValue::Utf8(None)),
        // password_lifetime
        Expr::Literal(ScalarValue::Utf8(None)),
        // account_locked
        Expr::Literal(ScalarValue::Utf8(Some("N".to_string()))),
        // Create_role_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Drop_role_priv
        Expr::Literal(ScalarValue::Utf8(Some("Y".to_string()))),
        // Password_reuse_history
        Expr::Literal(ScalarValue::Utf8(None)),
        // Password_reuse_time
        Expr::Literal(ScalarValue::Utf8(None)),
        // Password_require_current
        Expr::Literal(ScalarValue::Utf8(None)),
        // User_attributes
        Expr::Literal(ScalarValue::Utf8(None)),
    ];
    column_value.push(row);

    let table_schema = users();

    let engine = engine_util::EngineFactory::try_new(global_context.clone(), meta_const::FULL_TABLE_NAME_OF_DEF_MYSQL_USERS.to_object_name(), table_schema.clone());
    match engine {
        Ok(engine) => return engine.add_rows(column_name.clone(), column_value.clone()),
        Err(mysql_error) => return Err(mysql_error),
    }
}
