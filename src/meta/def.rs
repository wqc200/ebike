use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::error;
use datafusion::logical_plan::{DFField, DFSchema};
use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption, SqlOption, TableConstraint, Value};

use crate::meta::{meta_const, meta_util};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DbDef {
    with_option: Vec<SqlOption>,
}

impl DbDef {
    pub fn new(with_option: Vec<SqlOption>) -> Self {
        Self {
            with_option,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDef {
    pub ordinal_position: usize,
    pub sql_column: SQLColumnDef,
}

impl ColumnDef {
    pub fn new(
        ordinal_number: usize,
        sql_column_def: SQLColumnDef,
    ) -> Self {
        Self {
            ordinal_position: ordinal_number,
            sql_column: sql_column_def,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableDef {
    table_name: String,
    pub columns: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    pub with_option: Vec<SqlOption>,
}

impl TableDef {
    pub fn new_with_sqlcolumn(table_name: &str, sqlcolumns: Vec<SQLColumnDef>, constraints: Vec<TableConstraint>, with_option: Vec<SqlOption>) -> Self {
        let mut columns = vec![];
        let mut ordinal_position = 0;
        for sqlcolumn in sqlcolumns {
            ordinal_position += 1;
            let column = ColumnDef::new(ordinal_position, sqlcolumn);
            columns.push(column);
        }

        Self {
            table_name: table_name.to_string(),
            columns,
            constraints,
            with_option,
        }
    }

    pub fn new_with_column(table_name: &str, columns: Vec<ColumnDef>, constraints: Vec<TableConstraint>, with_option: Vec<SqlOption>) -> Self {
        Self {
            table_name: table_name.to_string(),
            columns,
            constraints,
            with_option,
        }
    }

    pub fn get_columns(&self) -> &Vec<ColumnDef> {
        &self.columns
    }

    pub fn to_sqlcolumns(&self) -> Vec<SQLColumnDef> {
        let mut sqlcolumns = vec![];
        for column_def in self.get_columns().to_vec() {
            sqlcolumns.push(column_def.sql_column);
        }
        sqlcolumns
    }

    pub fn get_constraints(&self) -> &Vec<TableConstraint> {
        &self.constraints
    }

    pub fn with_option(&self) -> &Vec<SqlOption> {
        &self.with_option
    }

    pub fn to_dfschema(&self) -> error::Result<DFSchema> {
        let mut dffields = vec![];
        dffields.push(DFField::new(Some(self.table_name.as_str()), meta_const::COLUMN_ROWID, DataType::Utf8, false));
        for column_def in self.get_columns().to_vec() {
            let field_name = column_def.sql_column.name.to_string();
            let data_type = meta_util::convert_sql_data_type_to_arrow_data_type(&column_def.sql_column.data_type).unwrap();
            let nullable = column_def.sql_column.options
                .iter()
                .any(|x| x.option == ColumnOption::Null);
            dffields.push(DFField::new(Some(self.table_name.as_str()), field_name.as_ref(), data_type, nullable));
        }

        DFSchema::new(dffields)
    }

    pub fn to_schema(&self) -> Schema {
        self.to_dfschema().unwrap().into()
    }

    pub fn to_schemaref(&self) -> SchemaRef {
        Arc::new(self.to_schema())
    }

    pub fn add_sqlcolumn(&mut self, column: SQLColumnDef) {
        let ordinal_position = self.columns.len() + 1;
        self.columns.push(ColumnDef::new(ordinal_position, column));
    }

    pub fn get_engine(&mut self) -> Option<String> {
        let engine = self.with_option.iter().fold(None, |key, x| {
            if x.name.to_string() == meta_const::OPTION_ENGINE {
                if let Value::SingleQuotedString(engine) = x.value.clone() {
                    Some(engine)
                } else {
                    None
                }
            } else {
                None
            }
        });
        engine
    }
}

#[derive(Debug, Clone)]
pub struct StatisticsColumn {
    pub column_name: String,
    pub seq_in_index: usize,
}
