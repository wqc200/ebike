use std::sync::{Arc, Mutex};

use sqlparser::ast::{ColumnOption, DataType as SQLDataType, Ident, TableConstraint};

use crate::core::global_context::GlobalContext;
use crate::meta::{meta_def, meta_const, meta_util};
use crate::meta::initial::create_table;

pub fn users(global_context: Arc<Mutex<GlobalContext>>) -> meta_def::TableDef {
    let sql_column_list = vec![
        meta_util::create_sql_column("Host", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("User", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Select_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Insert_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Update_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Delete_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Create_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Drop_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Reload_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Shutdown_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Process_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("File_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Grant_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("References_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Index_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Alter_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Show_db_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Super_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Create_tmp_table_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Lock_tables_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Execute_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Repl_slave_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Repl_client_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Create_view_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Show_view_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Create_routine_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Alter_routine_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Create_user_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Event_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Trigger_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Create_tablespace_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("ssl_type", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("ssl_cipher", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("x509_issuer", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("x509_subject", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("max_questions", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("max_updates", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("max_connections", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("max_user_connections", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("plugin", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("authentication_string", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("password_expired", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("password_last_changed", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("password_lifetime", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("account_locked", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Create_role_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Drop_role_priv", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Password_reuse_history", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Password_reuse_time", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("Password_require_current", SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column("User_attributes", SQLDataType::Char(None), ColumnOption::NotNull),
    ];

    let mut columns = vec![];
    columns.push(Ident::new("Host"));
    columns.push(Ident::new("user"));
    let table_constraint = TableConstraint::Unique {
        name: Some(Ident { value: meta_const::NAME_OF_PRIMARY.to_string(), quote_style: None }),
        columns,
        is_primary: true,
    };
    let constraints = vec![table_constraint];

    create_table(
        global_context.clone(),
        meta_const::SCHEMA_NAME_OF_DEF_MYSQL,
        meta_const::TABLE_NAME_OF_DEF_MYSQL_USERS,
        sql_column_list.clone(),
        constraints.clone(),
    )
}
