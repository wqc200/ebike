use std::sync::{Arc, Mutex};

use sqlparser::ast::{ColumnOption, DataType as SQLDataType, Ident, TableConstraint};

use crate::core::global_context::GlobalContext;
use crate::meta::{meta_def, meta_const, meta_util};
use crate::meta::initial::create_table;

pub fn users(global_context: Arc<Mutex<GlobalContext>>) -> meta_def::TableDef {
    let sql_column_list = vec![
        meta_util::create_sql_column("Host", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("User", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Select_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Insert_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Update_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Delete_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Create_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Drop_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Reload_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Shutdown_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Process_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("File_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Grant_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("References_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Index_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Alter_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Show_db_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Super_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Create_tmp_table_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Lock_tables_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Execute_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Repl_slave_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Repl_client_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Create_view_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Show_view_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Create_routine_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Alter_routine_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Create_user_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Event_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Trigger_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Create_tablespace_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("ssl_type", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("ssl_cipher", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("x509_issuer", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("x509_subject", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("max_questions", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("max_updates", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("max_connections", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("max_user_connections", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("plugin", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("authentication_string", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("password_expired", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("password_last_changed", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("password_lifetime", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("account_locked", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Create_role_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Drop_role_priv", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Password_reuse_history", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Password_reuse_time", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("Password_require_current", SQLDataType::Text, ColumnOption::NotNull),
        meta_util::create_sql_column("User_attributes", SQLDataType::Text, ColumnOption::NotNull),
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
