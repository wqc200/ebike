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
use crate::physical_plan::insert::PhysicalPlanInsert;
use crate::store::engine::engine_util;
use crate::store::reader::rocksdb::RocksdbReader;
use crate::store::rocksdb::db::DB;
use crate::store::rocksdb::option::Options;
use crate::util::convert::{ToObjectName, ToIdent};
use crate::mysql::metadata::MysqlType::MYSQL_TYPE_BIT;

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
    let mut column_name_list = vec![];
    for column_def in users().get_columns() {
        column_name_list.push(column_def.sql_column.name.to_string());
    }

    let mut column_value_map_list: Vec<HashMap<Ident, ScalarValue>> = vec![];
    let mut column_value_map = HashMap::new();
    // Host
    column_value_map.insert("Host".to_ident(), ScalarValue::Utf8(Some("%".to_string())));
    // User
    column_value_map.insert("User".to_ident(), ScalarValue::Utf8(Some("root".to_string())));
    // Select_priv
    column_value_map.insert("Select_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Insert_priv
    column_value_map.insert("Insert_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Update_priv
    column_value_map.insert("Update_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Delete_priv
    column_value_map.insert("Delete_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Create_priv
    column_value_map.insert("Create_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Drop_priv
    column_value_map.insert("Drop_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Reload_priv
    column_value_map.insert("Reload_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Shutdown_priv
    column_value_map.insert("Shutdown_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Process_priv
    column_value_map.insert("Process_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // File_priv
    column_value_map.insert("File_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Grant_priv
    column_value_map.insert("Grant_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // References_priv
    column_value_map.insert("References_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Index_priv
    column_value_map.insert("Index_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Alter_priv
    column_value_map.insert("Alter_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Show_db_priv
    column_value_map.insert("Show_db_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Super_priv
    column_value_map.insert("Super_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Create_tmp_table_priv
    column_value_map.insert("Create_tmp_table_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Lock_tables_priv
    column_value_map.insert("Lock_tables_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Execute_priv
    column_value_map.insert("Execute_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Repl_slave_priv
    column_value_map.insert("Repl_slave_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Repl_client_priv
    column_value_map.insert("Repl_client_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Create_view_priv
    column_value_map.insert("Create_view_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Show_view_priv
    column_value_map.insert("Show_view_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Create_routine_priv
    column_value_map.insert("Create_routine_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Alter_routine_priv
    column_value_map.insert("Alter_routine_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Create_user_priv
    column_value_map.insert("Create_user_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Event_priv
    column_value_map.insert("Event_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Trigger_priv
    column_value_map.insert("Trigger_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Create_tablespace_priv
    column_value_map.insert("Create_tablespace_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // ssl_type
    column_value_map.insert("ssl_type".to_ident(), ScalarValue::Utf8(None));
    // ssl_cipher
    column_value_map.insert("ssl_cipher".to_ident(), ScalarValue::Utf8(None));
    // x509_issuer
    column_value_map.insert("x509_issuer".to_ident(), ScalarValue::Utf8(None));
    // x509_subject
    column_value_map.insert("x509_subject".to_ident(), ScalarValue::Utf8(None));
    // max_questions
    column_value_map.insert("max_questions".to_ident(), ScalarValue::UInt64(Some(0)));
    // max_updates
    column_value_map.insert("max_updates".to_ident(), ScalarValue::UInt64(Some(0)));
    // max_connections
    column_value_map.insert("max_connections".to_ident(), ScalarValue::UInt64(Some(0)));
    // max_user_connections
    column_value_map.insert("max_user_connections".to_ident(), ScalarValue::UInt64(Some(0)));
    // plugin
    column_value_map.insert("plugin".to_ident(), ScalarValue::Utf8(Some("mysql_native_password".to_string())));
    // authentication_string
    column_value_map.insert("authentication_string".to_ident(), ScalarValue::Utf8(Some("*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9".to_string())));
    // password_expired
    column_value_map.insert("password_expired".to_ident(), ScalarValue::Utf8(Some("N".to_string())));
    // password_last_changed
    column_value_map.insert("password_last_changed".to_ident(), ScalarValue::Utf8(None));
    // password_lifetime
    column_value_map.insert("password_lifetime".to_ident(), ScalarValue::Utf8(None));
    // account_locked
    column_value_map.insert("account_locked".to_ident(), ScalarValue::Utf8(Some("N".to_string())));
    // Create_role_priv
    column_value_map.insert("Create_role_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Drop_role_priv
    column_value_map.insert("Drop_role_priv".to_ident(), ScalarValue::Utf8(Some("Y".to_string())));
    // Password_reuse_history
    column_value_map.insert("Password_reuse_history".to_ident(), ScalarValue::Utf8(None));
    // Password_reuse_time
    column_value_map.insert("Password_reuse_time".to_ident(), ScalarValue::Utf8(None));
    // Password_require_current
    column_value_map.insert("Password_require_current".to_ident(), ScalarValue::Utf8(None));
    // User_attributes
    column_value_map.insert("User_attributes".to_ident(), ScalarValue::Utf8(None));
    column_value_map_list.push(column_value_map);

    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_MYSQL_USERS.to_object_name();
    let table_def = users();

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
