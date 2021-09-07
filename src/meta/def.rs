use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::error;
use datafusion::logical_plan::{DFField, DFSchema};
use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption, SqlOption, TableConstraint, Value, ObjectName, Ident, ColumnDef};

use crate::meta::{meta_const, meta_util};
use crate::mysql::error::{MysqlError, MysqlResult};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparrowColumnDef {
    pub store_id: i32,
    pub ordinal_position: i32,
    pub sql_column: SQLColumnDef,
}

impl SparrowColumnDef {
    pub fn new(
        store_id: i32,
        ordinal_position: i32,
        sql_column: SQLColumnDef,
    ) -> Self {
        Self {
            store_id,
            ordinal_position,
            sql_column,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableColumnDef {
    pub sparrow_column_map: HashMap<Ident, SparrowColumnDef>,
    pub sparrow_column_list: Vec<SparrowColumnDef>,
    pub sql_column_list: Vec<SQLColumnDef>,
}

impl Default for TableColumnDef {
    fn default() -> Self {
        let sparrow_column_map = HashMap::new();
        let sparrow_column_list = vec![];
        let sql_column_list = vec![];

        Self {
            sparrow_column_map,
            sparrow_column_list,
            sql_column_list,
        }
    }
}

impl TableColumnDef {
    pub fn from_sparrow_column_list(mut sparrow_column_list: Vec<SparrowColumnDef>) -> Self {
        let mut table_column = TableColumnDef::default();

        sparrow_column_list.sort_by(|a, b| a.ordinal_position.cmp(&b.ordinal_position));

        let mut sparrow_column_map = HashMap::new();
        let mut sql_column_list = vec![];

        for sparrow_column in sparrow_column_list {
            let sql_column = sparrow_column.sql_column;
            let column_name = sql_column.name.clone();

            sparrow_column_map.insert(column_name, sparrow_column.clone());
            sql_column_list.push(sql_column.clone());
        }

        table_column.with_sparrow_column_list(sparrow_column_list);
        table_column.with_sparrow_column_map(sparrow_column_map);
        table_column.with_sql_column_list(sql_column_list);

        table_column
    }

    pub fn from_sql_column_list(sql_column_list: Vec<SQLColumnDef>) -> Self {
        let mut table_column = TableColumnDef::default();

        let mut sparrow_column_map = HashMap::new();
        let mut sparrow_column_list = vec![];

        let mut ordinal_position = 0;
        let mut store_id = 0;
        for sql_column in sql_column_list {
            let column_name = sql_column.name.clone();

            ordinal_position += 1;
            store_id += 1;

            let sparrow_column = SparrowColumnDef::new(store_id, ordinal_position, sql_column);

            sparrow_column_map.insert(column_name, sparrow_column.clone());
            sparrow_column_list.push(sparrow_column.clone());
        }

        table_column.with_sparrow_column_list(sparrow_column_list);
        table_column.with_sparrow_column_map(sparrow_column_map);
        table_column.with_sql_column_list(sql_column_list);

        table_column
    }
}

impl TableColumnDef {
    pub fn get_sparrow_column(&self, column_name: Ident) -> MysqlResult<SparrowColumnDef> {
        let result = self.sparrow_column_map.get(&column_name);
        match result {
            None => {
                Err(MysqlError::new_server_error(
                    1007,
                    "HY000",
                    format!("My column not found, column name: {:?}", column_name).as_str(),
                ))
            }
            Some(sparrow_column) => Ok(sparrow_column.clone())
        }
    }

    pub fn get_max_store_id(&self) -> i32 {
        let mut max_store_id = 0;

        for sparrow_column in self.sparrow_column_list {
            if sparrow_column.store_id > max_store_id {
                max_store_id = sparrow_column.store_id;
            }
        }

        max_store_id
    }

    pub fn get_last_sparrow_column(&self) -> SparrowColumnDef {
        self.sparrow_column_list.

        max_store_id
    }
}

impl TableColumnDef {
    pub fn with_sparrow_column_map(mut self, sparrow_column_map: HashMap<Ident, SparrowColumnDef>) -> Self {
        self.sparrow_column_map = sparrow_column_map;
        self
    }

    pub fn with_sparrow_column_list(mut self, sparrow_column_list: Vec<SparrowColumnDef>) -> Self {
        self.sparrow_column_list = sparrow_column_list;
        self
    }

    pub fn with_sql_column_list(mut self, sql_column_list: Vec<SQLColumnDef>) -> Self {
        self.sql_column_list = sql_column_list;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOptionDef {
    pub engine: String,
    pub column_max_store_id: i32,
    pub table_type: String,
}

impl Default for TableOptionDef {
    fn default() -> Self {
        Self {
            engine: "".to_string(),
            column_max_store_id: 0,
            table_type: "".to_string(),
        }
    }
}

impl TableOptionDef {
    pub fn from_table_options(table_options: Vec<SqlOption>) -> Self {
        let table_option = TableOptionDef::default();

        for sql_option in table_options {
            if sql_option.name.to_string().to_uppercase() == meta_const::TABLE_OPTION_OF_TABLE_TYPE.to_uppercase() {
                match sql_option.value {
                    Value::SingleQuotedString(table_type) => {
                        table_option.with_table_type(table_type.as_str());
                    }
                    _ => {}
                };
            } else if sql_option.name.to_string().to_uppercase() == meta_const::TABLE_OPTION_OF_ENGINE.to_uppercase() {
                match sql_option.value {
                    Value::SingleQuotedString(engine) => {
                        table_option.with_engine(engine.as_str());
                    }
                    _ => {}
                };
            }
        }

        table_option
    }
}

impl TableOptionDef {
    pub fn to_table_options(&self) -> Vec<SqlOption> {
        let table_options = vec![];

        let sql_option = SqlOption { name: Ident { value: meta_const::TABLE_OPTION_OF_TABLE_TYPE.to_string(), quote_style: None }, value: Value::SingleQuotedString(self.table_type.clone()) };
        with_option.push(sql_option);
        let sql_option = SqlOption { name: Ident { value: meta_const::TABLE_OPTION_OF_ENGINE.to_string(), quote_style: None }, value: Value::SingleQuotedString(self.engine.clone()) };
        with_option.push(sql_option);

        table_options
    }
}

impl TableOptionDef {
    pub fn with_engine(mut self, engine: &str) -> Self {
        self.engine = engine.to_string();
        self
    }

    pub fn with_column_max_store_id(mut self, column_max_store_id: i32) -> Self {
        self.column_max_store_id = column_max_store_id;
        self
    }

    pub fn with_table_type(mut self, table_type: &str) -> Self {
        self.table_type = table_type.to_string();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    pub full_table_name: ObjectName,
    pub column: TableColumnDef,
    pub constraints: Vec<TableConstraint>,
    pub table_option: TableOptionDef,
}

impl TableDef {
    pub fn new(full_table_name: ObjectName) -> Self {
        let constraints: Vec<TableConstraint> = vec![];
        let table_option = TableOptionDef::default();
        let column = TableColumnDef::new();

        Self {
            full_table_name,
            column,
            constraints,
            table_option,
        }
    }

    pub fn new_with_sqlcolumn(full_table_name: ObjectName, sql_column_list: Vec<SQLColumnDef>, constraints: Vec<TableConstraint>, with_option: Vec<SqlOption>) -> Self {
        let table_column = TableColumnDef::from_sql_column_list(sql_column_list);

        let mut table = TableDef::new(full_table_name);
        table.with_column(table_column);
        table.table_option.with_column_max_store_id(table_column.get_max_store_id());

        table
    }
}

impl TableDef {
    pub fn with_column(&mut self, column: TableColumnDef) {
        self.column = column
    }

    pub fn with_constraints(&mut self, constraints: Vec<TableConstraint>) {
        self.constraints = constraints
    }

    pub fn with_option(&mut self, table_option: TableOptionDef) {
        self.table_option = table_option
    }
}

impl TableDef {
    pub fn get_full_table_name(&self) -> ObjectName {
        self.full_table_name.clone()
    }

    pub fn get_columns(&self) -> &Vec<SparrowColumnDef> {
        &self.column.sparrow_column_list
    }

    pub fn get_table_column(&self) -> TableColumnDef {
        self.column.clone()
    }

    pub fn get_constraints(&self) -> &Vec<TableConstraint> {
        &self.constraints
    }

    pub fn to_datafusion_dfschema(&self) -> error::Result<DFSchema> {
        let mut dffields = vec![];
        dffields.push(DFField::new(Some(self.full_table_name.to_string().as_str()), meta_const::COLUMN_ROWID, DataType::Utf8, false));
        for sql_column in self.column.sql_column_list {
            let field_name = sql_column.name.to_string();
            let data_type = meta_util::convert_sql_data_type_to_arrow_data_type(&sql_column.data_type).unwrap();
            let nullable = sql_column.options
                .iter()
                .any(|x| x.option == ColumnOption::Null);
            dffields.push(DFField::new(Some(self.full_table_name.to_string().as_str()), field_name.as_ref(), data_type, nullable));
        }

        DFSchema::new(dffields)
    }

    pub fn to_schema(&self) -> Schema {
        self.to_datafusion_dfschema().unwrap().into()
    }

    pub fn to_schemaref(&self) -> SchemaRef {
        Arc::new(self.to_schema())
    }

    pub fn add_sqlcolumn(&mut self, column: SQLColumnDef) {
        let ordinal_position = self.columns.len() + 1;
        self.columns.push(SparrowColumnDef::new(ordinal_position, column));
    }

    pub fn get_engine(&mut self) -> Option<String> {
        let engine = self.sql_options.iter().fold(None, |key, x| {
            if x.name.to_string() == meta_const::TABLE_OPTION_OF_ENGINE {
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
