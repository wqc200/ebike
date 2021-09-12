use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::error;
use datafusion::logical_plan::{DFField, DFSchema};
use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption, SqlOption, TableConstraint, Value, ObjectName, Ident, ColumnDef};

use crate::meta::{meta_const, meta_util};
use crate::mysql::error::{MysqlError, MysqlResult};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDef {
    pub full_schema_name: ObjectName,
    pub schema_option: SchemaOptionDef,
}

impl SchemaDef {
    pub fn new(full_schema_name: ObjectName) -> Self {
        Self {
            full_schema_name,
            schema_option: SchemaOptionDef::default(),
        }
    }
}

impl SchemaDef {
    pub fn with_schema_option(&mut self, schema_option: SchemaOptionDef) {
        self.schema_option = schema_option
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaOptionDef {
    pub catalog_name: String,
    pub schema_name: String,
    pub default_character_set_name: String,
    pub default_collation_name: String,
}

impl Default for SchemaOptionDef {
    fn default() -> Self {
        Self {
            catalog_name: "".to_string(),
            schema_name: "".to_string(),
            default_character_set_name: "".to_string(),
            default_collation_name: "".to_string(),
        }
    }
}

impl SchemaOptionDef {
    pub fn new(catalog_name: &str, schema_name: &str) -> Self {
        Self {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            default_character_set_name: "".to_string(),
            default_collation_name: "".to_string(),
        }
    }
}

// impl SchemaOptionDef {
//     pub fn use_schema_options(mut self, schema_options: Vec<SqlOption>) -> Self {
//         for sql_option in schema_options {
//             if sql_option.name.to_string().to_uppercase() == meta_const::NAME_OF_SCHEMA_OPTION_DEFAULT_CHARACTER_SET_NAME.to_uppercase() {
//                 match sql_option.value {
//                     Value::SingleQuotedString(value) => {
//                         self.with_default_character_set_name(value.as_str());
//                     }
//                     _ => {}
//                 };
//             } else if sql_option.name.to_string().to_uppercase() == meta_const::NAME_OF_SCHEMA_OPTION_DEFAULT_COLLATION_NAME.to_uppercase() {
//                 match sql_option.value {
//                     Value::SingleQuotedString(value) => {
//                         self.with_default_collation_name(value.as_str());
//                     }
//                     _ => {}
//                 };
//             }
//         }
//
//         self
//     }
// }

impl SchemaOptionDef {
    pub fn with_default_character_set_name(&mut self, default_character_set_name: &str) {
        self.default_character_set_name = default_character_set_name.to_string()
    }

    pub fn with_default_collation_name(&mut self, default_collation_name: &str) {
        self.default_collation_name = default_collation_name.to_string()
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
    pub fn use_sparrow_column_list(&mut self, mut sparrow_column_list: Vec<SparrowColumnDef>) {
        sparrow_column_list.sort_by(|a, b| a.ordinal_position.cmp(&b.ordinal_position));

        let mut sparrow_column_map = HashMap::new();
        let mut sql_column_list = vec![];

        for sparrow_column in sparrow_column_list.clone() {
            let sql_column = sparrow_column.sql_column.clone();
            let column_name = sql_column.name.clone();

            sparrow_column_map.insert(column_name.clone(), sparrow_column.clone());
            sql_column_list.push(sql_column.clone());
        }

        self.with_sparrow_column_list(sparrow_column_list.clone());
        self.with_sparrow_column_map(sparrow_column_map.clone());
        self.with_sql_column_list(sql_column_list.clone());
    }

    pub fn use_sql_column_list(&mut self, sql_column_list: Vec<SQLColumnDef>) {
        let mut sparrow_column_map = HashMap::new();
        let mut sparrow_column_list = vec![];

        let mut ordinal_position = 0;
        let mut store_id = 0;
        for sql_column in sql_column_list.clone() {
            let column_name = sql_column.name.clone();

            ordinal_position += 1;
            store_id += 1;

            let sparrow_column = SparrowColumnDef::new(store_id, ordinal_position, sql_column);

            sparrow_column_map.insert(column_name, sparrow_column.clone());
            sparrow_column_list.push(sparrow_column.clone());
        }

        self.with_sparrow_column_list(sparrow_column_list.clone());
        self.with_sparrow_column_map(sparrow_column_map.clone());
        self.with_sql_column_list(sql_column_list.clone());
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

        for sparrow_column in self.sparrow_column_list.clone() {
            if sparrow_column.store_id > max_store_id {
                max_store_id = sparrow_column.store_id;
            }
        }

        max_store_id
    }

    pub fn get_last_sparrow_column(&self) -> Option<SparrowColumnDef> {
        let length = self.sparrow_column_list.len();
        let result = self.sparrow_column_list.get(length - 1);
        match result {
            None => None,
            Some(sparrow_column) => Some(sparrow_column.clone())
        }
    }
}

impl TableColumnDef {
    pub fn with_sparrow_column_map(&mut self, sparrow_column_map: HashMap<Ident, SparrowColumnDef>) {
        self.sparrow_column_map = sparrow_column_map
    }

    pub fn with_sparrow_column_list(&mut self, sparrow_column_list: Vec<SparrowColumnDef>) {
        self.sparrow_column_list = sparrow_column_list
    }

    pub fn with_sql_column_list(&mut self, sql_column_list: Vec<SQLColumnDef>) {
        self.sql_column_list = sql_column_list
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOptionDef {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub full_table_name: ObjectName,
    pub engine: String,
    pub column_max_store_id: i32,
    pub table_type: String,
}

impl Default for TableOptionDef {
    fn default() -> Self {
        Self {
            catalog_name: "".to_string(),
            schema_name: "".to_string(),
            table_name: "".to_string(),
            full_table_name: ObjectName(vec![]),
            engine: "".to_string(),
            column_max_store_id: 0,
            table_type: "".to_string(),
        }
    }
}

impl TableOptionDef {
    pub fn new(catalog_name: &str, schema_name: &str, table_name: &str) -> Self {
        let full_table_name = meta_util::create_full_table_name(catalog_name, schema_name, table_name);

        Self {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            full_table_name,
            engine: "".to_string(),
            column_max_store_id: 0,
            table_type: "".to_string(),
        }
    }
}

impl TableOptionDef {
    pub fn to_table_options(&self) -> Vec<SqlOption> {
        let mut table_options = vec![];

        let sql_option = SqlOption { name: Ident { value: meta_const::NAME_OF_TABLE_OPTION_TABLE_TYPE.to_string(), quote_style: None }, value: Value::SingleQuotedString(self.table_type.clone()) };
        table_options.push(sql_option);
        let sql_option = SqlOption { name: Ident { value: meta_const::NAME_OF_TABLE_OPTION_ENGINE.to_string(), quote_style: None }, value: Value::SingleQuotedString(self.engine.clone()) };
        table_options.push(sql_option);

        table_options
    }

    pub fn use_table_options(&mut self, table_options: Vec<SqlOption>) {
        for sql_option in table_options {
            if sql_option.name.to_string().to_uppercase() == meta_const::NAME_OF_TABLE_OPTION_ENGINE.to_uppercase() {
                match sql_option.value {
                    Value::SingleQuotedString(value) => {
                        self.with_engine(value.as_str());
                    }
                    _ => {}
                };
            }
        }
    }
}

impl TableOptionDef {
    pub fn with_engine(&mut self, engine: &str) {
        self.engine = engine.to_string()
    }

    pub fn with_column_max_store_id(&mut self, column_max_store_id: i32) {
        self.column_max_store_id = column_max_store_id
    }

    pub fn with_table_type(&mut self, table_type: &str) {
        self.table_type = table_type.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct TableIndexDef {
    pub index_name: String,
    pub level: i32,
    pub column_name_list: Vec<Ident>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    pub column: TableColumnDef,
    pub constraints: Vec<TableConstraint>,
    pub option: TableOptionDef,
}

impl TableDef {
    pub fn new() -> Self {
        let table_constraints: Vec<TableConstraint> = vec![];
        let table_option = TableOptionDef::default();
        let table_column = TableColumnDef::default();

        Self {
            column: table_column,
            constraints: table_constraints,
            option: table_option,
        }
    }

    pub fn new_with_sqlcolumn(full_table_name: ObjectName, sql_column_list: Vec<SQLColumnDef>, constraints: Vec<TableConstraint>, with_option: Vec<SqlOption>) -> Self {
        let mut table_column = TableColumnDef::default();
        table_column.use_sql_column_list(sql_column_list);

        let mut table = TableDef::new();
        table.with_column(table_column.clone());
        table.option.with_column_max_store_id(table_column.get_max_store_id());

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
        self.option = table_option
    }
}

impl TableDef {
    pub fn get_full_table_name(&self) -> ObjectName {
        self.option.full_table_name.clone()
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
        dffields.push(DFField::new(Some(self.option.full_table_name.to_string().as_str()), meta_const::COLUMN_ROWID, DataType::Utf8, false));
        for sql_column in self.column.sql_column_list.clone() {
            let field_name = sql_column.name.to_string();
            let data_type = meta_util::convert_sql_data_type_to_arrow_data_type(&sql_column.data_type).unwrap();
            let nullable = sql_column.options
                .iter()
                .any(|x| x.option == ColumnOption::Null);
            dffields.push(DFField::new(Some(self.option.full_table_name.to_string().as_str()), field_name.as_ref(), data_type, nullable));
        }

        DFSchema::new(dffields)
    }

    pub fn to_schema(&self) -> Schema {
        self.to_datafusion_dfschema().unwrap().into()
    }

    pub fn to_schema_ref(&self) -> SchemaRef {
        Arc::new(self.to_schema())
    }

    pub fn get_engine(&self) -> String {
        let engine = self.option.engine.clone();
        engine
    }
}

#[derive(Debug, Clone)]
pub struct StatisticsColumn {
    pub column_name: String,
    pub seq_in_index: usize,
}

#[derive(Debug, Clone)]
pub struct IndexDef {
    pub index_name: String,
    pub level: i32,
    pub index_key: String,
}

impl IndexDef {
    pub fn new(index_name: &str, level: i32, index_key: &str) -> Self {
        Self {
            index_name: index_name.to_string(),
            level,
            index_key: index_key.to_string(),
        }
    }
}