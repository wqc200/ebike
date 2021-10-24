use std::sync::{Arc, Mutex};

use sqlparser::ast::{ColumnOption, DataType as SQLDataType, Ident, TableConstraint};

use crate::core::global_context::GlobalContext;
use crate::meta::{meta_def, meta_const, meta_util};
use crate::meta::initial::create_table;

pub fn global_variables(global_context: Arc<Mutex<GlobalContext>>) -> meta_def::TableDef {
    let sql_column_list = vec![
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_NAME, SQLDataType::Char(None), ColumnOption::NotNull),
        meta_util::create_sql_column(meta_const::COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_VALUE, SQLDataType::Char(None), ColumnOption::Null),
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
