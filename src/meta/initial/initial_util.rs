use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::{Arc, Mutex};

use arrow::array::{as_primitive_array, as_string_array, Int32Array, StringArray};
use datafusion::catalog::TableReference;
use datafusion::logical_plan::Expr;
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption, Ident, ObjectName, SqlOption, TableConstraint, Value};

use crate::core::global_context::GlobalContext;
use crate::meta::{initial, meta_const, meta_util};
use crate::meta::def::{ColumnDef, DbDef, StatisticsColumn, TableDef};
use crate::meta::initial::information_schema;
use crate::meta::initial::performance_schema;
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::store::engine::engine_util;
use crate::store::engine::engine_util::{TableEngine, ADD_ENTRY_TYPE};
use crate::store::reader::rocksdb::RocksdbReader;
use crate::util::convert::{ToIdent, ToObjectName};
use crate::physical_plan;
use crate::physical_plan::insert::PhysicalPlanInsert;

#[derive(Debug, Clone)]
pub struct SaveTableConstraints {
    global_context: Arc<Mutex<GlobalContext>>,
    catalog_name: String,
    schema_name: String,
    table_name: String,
    column_value_map_list: Vec<HashMap<Ident, ScalarValue>>,
}

impl SaveTableConstraints {
    pub fn new(global_context: Arc<Mutex<GlobalContext>>, catalog_name: &str, schema_name: &str, table_name: &str) -> Self {
        Self {
            global_context,
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            column_value_map_list: vec![],
        }
    }

    pub fn add_row(&mut self, constraint_name: &str, constraint_type: &str) {
        let mut column_value_map = HashMap::new();
        column_value_map.insert("constraint_catalog".to_ident(), ScalarValue::Utf8(Some(self.catalog_name.clone())));
        column_value_map.insert("constraint_schema".to_ident(), ScalarValue::Utf8(Some(self.schema_name.clone())));
        column_value_map.insert("constraint_name".to_ident(), ScalarValue::Utf8(Some(constraint_name.to_string())));
        column_value_map.insert("table_schema".to_ident(), ScalarValue::Utf8(Some(self.schema_name.clone())));
        column_value_map.insert("table_name".to_ident(), ScalarValue::Utf8(Some(self.table_name.clone())));
        column_value_map.insert("constraint_type".to_ident(), ScalarValue::Utf8(Some(constraint_type.to_string())));
        column_value_map.insert("enforced".to_ident(), ScalarValue::Utf8(Some("YES".to_string())));
        self.column_value_map_list.push(column_value_map);
    }

    pub fn save(&mut self) -> MysqlResult<u64> {
        let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLE_CONSTRAINTS.to_object_name();
        let table_def = self.global_context.lock().unwrap().meta_cache.get_table(full_table_name.clone()).unwrap().clone();

        let mut column_name_list = vec![];
        for column_def in table_def.get_columns() {
            column_name_list.push(column_def.sql_column.name.to_string());
        }

        let table_def = initial::information_schema::table_constraints();
        let insert = PhysicalPlanInsert::new(
            self.global_context.clone(),
            full_table_name,
            table_def,
            column_name_list.clone(),
            vec![],
            self.column_value_map_list.clone(),
        );
        let total = insert.execute();
        total
    }
}

#[derive(Debug, Clone)]
pub struct SaveKeyColumnUsage {
    global_context: Arc<Mutex<GlobalContext>>,
    catalog_name: String,
    schema_name: String,
    table_name: String,
    column_value_map_list: Vec<HashMap<Ident, ScalarValue>>,
}

impl SaveKeyColumnUsage {
    pub fn new(global_context: Arc<Mutex<GlobalContext>>, catalog_name: &str, schema_name: &str, table_name: &str) -> Self {
        Self {
            global_context,
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            column_value_map_list: vec![],
        }
    }

    pub fn add_row(&mut self, constraint_name: &str, seq_in_index: i32, column_name: &str) {
        let mut column_value_map = HashMap::new();
        column_value_map.insert("constraint_catalog".to_ident(), ScalarValue::Utf8(Some(self.catalog_name.clone())));
        column_value_map.insert("constraint_schema".to_ident(), ScalarValue::Utf8(Some(self.schema_name.clone())));
        column_value_map.insert("constraint_name".to_ident(), ScalarValue::Utf8(Some(constraint_name.to_string())));
        column_value_map.insert("table_catalog".to_ident(), ScalarValue::Utf8(Some(self.catalog_name.clone())));
        column_value_map.insert("table_schema".to_ident(), ScalarValue::Utf8(Some(self.schema_name.clone())));
        column_value_map.insert("table_name".to_ident(), ScalarValue::Utf8(Some(self.table_name.clone())));
        column_value_map.insert("column_name".to_ident(), ScalarValue::Utf8(Some(column_name.to_string())));
        column_value_map.insert("ordinal_position".to_ident(), ScalarValue::Int32(Some(seq_in_index)));
        // POSITION_IN_UNIQUE_CONSTRAINT, it is null if constraints is primary or unique
        column_value_map.insert("position_in_unique_constraint".to_ident(), ScalarValue::Int32(None));
        column_value_map.insert("referenced_table_schema".to_ident(), ScalarValue::Utf8(None));
        column_value_map.insert("referenced_table_name".to_ident(), ScalarValue::Utf8(None));
        column_value_map.insert("referenced_column_name".to_ident(), ScalarValue::Utf8(None));
        self.column_value_map_list.push(column_value_map);
    }

    pub fn save(&mut self) -> MysqlResult<u64> {
        let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_KEY_COLUMN_USAGE.to_object_name();
        let table_def = self.global_context.lock().unwrap().meta_cache.get_table(full_table_name.clone()).unwrap().clone();

        let mut column_name_list = vec![];
        for column_def in table_def.get_columns() {
            column_name_list.push(column_def.sql_column.name.to_string());
        }

        let table_def = initial::information_schema::key_column_usage();
        let insert = physical_plan::insert::PhysicalPlanInsert::new(
            self.global_context.clone(),
            full_table_name,
            table_def,
            column_name_list.clone(),
            vec![],
            self.column_value_map_list.clone(),
        );
        let total = insert.execute();
        total
    }
}

#[derive(Debug, Clone)]
pub struct SaveStatistics {
    global_context: Arc<Mutex<GlobalContext>>,
    catalog_name: String,
    schema_name: String,
    table_name: String,
    column_value_map_list: Vec<HashMap<Ident, ScalarValue>>,
}

impl SaveStatistics {
    pub fn new(global_context: Arc<Mutex<GlobalContext>>, catalog_name: &str, schema_name: &str, table_name: &str) -> Self {
        Self {
            global_context,
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            column_value_map_list: vec![],
        }
    }

    pub fn add_row(&mut self, index_name: &str, seq_in_index: i32, column_name: &str) {
        let mut column_value_map = HashMap::new();
        column_value_map.insert(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_CATALOG.to_ident(), ScalarValue::Utf8(Some(self.catalog_name.clone())));
        column_value_map.insert(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_SCHEMA.to_ident(), ScalarValue::Utf8(Some(self.schema_name.clone())));
        column_value_map.insert(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_NAME.to_ident(), ScalarValue::Utf8(Some(self.table_name.clone())));
        column_value_map.insert(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_NON_UNIQUE.to_ident(), ScalarValue::Int32(Some(0)));
        column_value_map.insert(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_INDEX_NAME.to_ident(), ScalarValue::Utf8(Some(index_name.to_string())));
        column_value_map.insert(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_SEQ_IN_INDEX.to_ident(), ScalarValue::Int32(Some(seq_in_index)));
        column_value_map.insert(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_COLUMN_NAME.to_ident(), ScalarValue::Utf8(Some(column_name.to_string())));
        self.column_value_map_list.push(column_value_map);
    }

    pub fn save(&mut self) -> MysqlResult<u64> {
        let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS.to_object_name();
        let table_def = self.global_context.lock().unwrap().meta_cache.get_table(full_table_name.clone()).unwrap().clone();

        let mut column_name_list = vec![];
        for column_def in initial::information_schema::table_statistics().get_columns() {
            column_name_list.push(column_def.sql_column.name.to_string());
        }

        let table_def = initial::information_schema::table_statistics();
        let insert = physical_plan::insert::PhysicalPlanInsert::new(
            self.global_context.clone(),
            full_table_name,
            table_def,
            column_name_list.clone(),
            vec![],
            self.column_value_map_list.clone(),
        );
        let total = insert.execute();
        total
    }
}

pub fn delete_db_form_information_schema(global_context: Arc<Mutex<GlobalContext>>, full_db_name: ObjectName) -> MysqlResult<u64> {
    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA.to_object_name();

    let table_def = global_context.lock().unwrap().meta_cache.get_table(full_table_name.clone()).unwrap();
    let schema_ref = table_def.to_schemaref();

    let rowid_index = schema_ref.index_of(meta_const::COLUMN_ROWID).unwrap();
    let db_name_index = schema_ref.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_SCHEMA_NAME).unwrap();
    let projection = Some(vec![rowid_index, db_name_index]);

    let mut store_engine = engine_util::StoreEngineFactory::try_new_with_table(global_context.clone(), full_table_name.clone()).unwrap();
    let mut table_iterator = engine_util::TableEngineFactory::try_new_with_table(global_context.clone(), full_table_name.clone()).unwrap().table_iterator(projection);

    let mut total = 0;
    loop {
        match table_iterator.next() {
            Some(item) => {
                match item {
                    Ok(record_batch) => {
                        let column_rowid: &StringArray = as_string_array(record_batch.column(0));
                        let column_db_name: &StringArray = as_string_array(record_batch.column(1));

                        let mut rowids: Vec<String> = vec![];
                        for row_index in 0..record_batch.num_rows() {
                            let rowid = column_rowid.value(row_index).to_string();
                            let find_db_name = column_db_name.value(row_index).to_string();

                            if find_db_name.eq(&full_db_name.to_string()) {
                                rowids.push(rowid);
                            }
                        }

                        for rowid in rowids {
                            let result = store_engine.delete_key(rowid);
                            match result {
                                Ok(count) => { total += 1 }
                                Err(mysql_error) => {
                                    return Err(mysql_error);
                                }
                            }
                        }
                    }
                    Err(arrow_error) => return Err(MysqlError::from(arrow_error)),
                }
            }
            None => {
                break;
            }
        }
    }
    Ok(total)
}

pub fn add_information_schema_tables(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName, with_options: Vec<SqlOption>) -> MysqlResult<u64> {
    let mut column_name_list = vec![];
    for column_def in initial::information_schema::table_tables().columns {
        column_name_list.push(column_def.sql_column.name.to_string());
    }

    let tables_reference = TableReference::try_from(&full_table_name).unwrap();
    let resolved_table_reference = tables_reference.resolve(meta_const::CATALOG_NAME, meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA);
    let catalog_name = resolved_table_reference.catalog.to_string();
    let schema_name = resolved_table_reference.schema.to_string();
    let table_name = resolved_table_reference.table.to_string();

    let mut table_type = "".to_string();
    for option in with_options {
        if option.name.to_string().to_uppercase() == meta_const::OPTION_TABLE_TYPE.to_uppercase() {
            table_type = match option.value {
                Value::SingleQuotedString(s) => { s.clone() }
                _ => { "".to_string() }
            };
        }
    }

    let mut column_value_map_list: Vec<HashMap<Ident, ScalarValue>> = vec![];
    let mut column_value_map = HashMap::new();
    column_value_map.insert(meta_const::COLUMN_INFORMATION_SCHEMA_TABLE_CATALOG.to_ident(), ScalarValue::Utf8(Some(catalog_name.to_string())));
    column_value_map.insert(meta_const::COLUMN_INFORMATION_SCHEMA_TABLE_SCHEMA.to_ident(), ScalarValue::Utf8(Some(schema_name.to_string())));
    column_value_map.insert(meta_const::COLUMN_INFORMATION_SCHEMA_TABLE_NAME.to_ident(), ScalarValue::Utf8(Some(table_name.to_string())));
    column_value_map.insert(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_TYPE.to_ident(), ScalarValue::Utf8(Some(table_type.to_string())));
    column_value_map.insert(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_ENGINE.to_ident(), ScalarValue::Utf8(Some("rocksdb".to_string())));
    column_value_map.insert(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_VERSION.to_ident(), ScalarValue::Int32(Some(0)));
    column_value_map.insert(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_DATA_LENGTH.to_ident(), ScalarValue::Int32(Some(0)));
    column_value_map.insert(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_INDEX_LENGTH.to_ident(), ScalarValue::Int32(Some(0)));
    column_value_map.insert(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_AUTO_INCREMENT.to_ident(), ScalarValue::Int32(Some(0)));
    column_value_map_list.push(column_value_map);

    let table_def = initial::information_schema::table_tables();
    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES.to_object_name();
    let insert = physical_plan::insert::PhysicalPlanInsert::new(
        global_context.clone(),
        full_table_name,
        table_def,
        column_name_list.clone(),
        vec![],
        column_value_map_list.clone(),
    );
    let total = insert.execute();
    total
}

pub fn add_information_schema_columns(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName, columns: Vec<SQLColumnDef>) -> MysqlResult<u64> {
    let mut column_name_list = vec![];
    for column_def in initial::information_schema::table_columns().columns {
        column_name_list.push(column_def.sql_column.name.to_string());
    }

    let tables_reference = TableReference::try_from(&full_table_name).unwrap();
    let resolved_table_reference = tables_reference.resolve(meta_const::CATALOG_NAME, meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA);
    let catalog_name = resolved_table_reference.catalog.to_string();
    let schema_name = resolved_table_reference.schema.to_string();
    let table_name = resolved_table_reference.table.to_string();

    let mut ordinal_position = meta_util::get_table_max_ordinal_position(global_context.clone(), full_table_name.clone());
    let table_has_primary_key = meta_util::table_has_primary_key(global_context.clone(), full_table_name.clone());

    let mut create_statistics = SaveStatistics::new(global_context.clone(), catalog_name.as_str(), schema_name.as_str(), table_name.as_str());

    let mut column_value_map_list: Vec<HashMap<Ident, ScalarValue>> = vec![];
    for column in columns {
        // start from 1
        ordinal_position += 1;

        let column_name = column.clone().name.to_string();
        for x in column.clone().options {
            match x.option {
                ColumnOption::Unique { is_primary } => {
                    if is_primary {
                        if table_has_primary_key {
                            return Err(MysqlError::new_server_error(1068, "42000", "Multiple primary key defined"));
                        }
                        create_statistics.add_row(meta_const::PRIMARY_NAME, 1, column_name.as_str())
                    } else {
                        create_statistics.add_row(column_name.as_str(), 1, column_name.as_str())
                    }
                }
                _ => {}
            }
        }

        let data_type = meta_util::convert_sql_data_type_to_text(&column.data_type).unwrap();

        let allow_null = column.clone().options
            .iter()
            .any(|x| x.option == ColumnOption::Null);
        let mut is_nullable = "NO";
        if allow_null {
            is_nullable = "YES"
        }

        let mut column_value_map = HashMap::new();
        /// TABLE_CATALOG
        column_value_map.insert("table_catalog".to_ident(), ScalarValue::Utf8(Some(catalog_name.clone())));
        /// TABLE_SCHEMA
        column_value_map.insert("table_schema".to_ident(), ScalarValue::Utf8(Some(schema_name.clone())));
        /// TABLE_NAME
        column_value_map.insert("table_name".to_ident(), ScalarValue::Utf8(Some(table_name.clone())));
        /// COLUMN_NAME
        column_value_map.insert("column_name".to_ident(), ScalarValue::Utf8(Some(column_name.clone())));
        /// ORDINAL_POSITION
        column_value_map.insert("ordinal_position".to_ident(), ScalarValue::Int32(Some(ordinal_position as i32)));
        /// COLUMN_DEFAULT
        column_value_map.insert("COLUMN_DEFAULT".to_ident(), ScalarValue::Utf8(None));
        /// IS_NULLABLE
        column_value_map.insert("is_nullable".to_ident(), ScalarValue::Utf8(Some(is_nullable.to_string())));
        /// DATA_TYPE
        column_value_map.insert("data_type".to_ident(), ScalarValue::Utf8(Some(data_type)));
        /// CHARACTER_MAXIMUM_LENGTH
        column_value_map.insert("CHARACTER_MAXIMUM_LENGTH".to_ident(), ScalarValue::Int32(None));
        /// CHARACTER_OCTET_LENGTH
        column_value_map.insert("CHARACTER_OCTET_LENGTH".to_ident(), ScalarValue::Int32(None));
        /// NUMERIC_PRECISION
        column_value_map.insert("NUMERIC_PRECISION".to_ident(), ScalarValue::Int32(None));
        /// NUMERIC_SCALE
        column_value_map.insert("NUMERIC_SCALE".to_ident(), ScalarValue::Int32(None));
        /// DATETIME_PRECISION
        column_value_map.insert("DATETIME_PRECISION".to_ident(), ScalarValue::Int32(None));
        /// CHARACTER_SET_NAME
        column_value_map.insert("CHARACTER_SET_NAME".to_ident(), ScalarValue::Utf8(None));
        /// COLLATION_NAME
        column_value_map.insert("COLLATION_NAME".to_ident(), ScalarValue::Utf8(None));
        /// COLUMN_TYPE
        column_value_map.insert("COLUMN_TYPE".to_ident(), ScalarValue::Utf8(None));
        /// COLUMN_KEY
        column_value_map.insert("COLUMN_KEY".to_ident(), ScalarValue::Utf8(None));
        /// EXTRA
        column_value_map.insert("EXTRA".to_ident(), ScalarValue::Utf8(None));
        /// PRIVILEGES
        column_value_map.insert("PRIVILEGES".to_ident(), ScalarValue::Utf8(None));
        /// COLUMN_COMMENT
        column_value_map.insert("COLUMN_COMMENT".to_ident(), ScalarValue::Utf8(None));
        /// GENERATION_EXPRESSION
        column_value_map.insert("GENERATION_EXPRESSION".to_ident(), ScalarValue::Utf8(None));
        /// SRS_ID
        column_value_map.insert("SRS_ID".to_ident(), ScalarValue::Int32(None));
        column_value_map_list.push(column_value_map);
    }

    let table_def = initial::information_schema::table_columns();
    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS.to_object_name();
    let insert = physical_plan::insert::PhysicalPlanInsert::new(
        global_context.clone(),
        full_table_name,
        table_def,
        column_name_list.clone(),
        vec![],
        column_value_map_list.clone(),
    );
    let total = insert.execute();
    total
}

pub fn create_schema(global_context: Arc<Mutex<GlobalContext>>, full_schema_name: ObjectName) -> MysqlResult<u64> {
    let mut column_name_list = vec![];
    for column_def in initial::information_schema::table_schemata().get_columns() {
        column_name_list.push(column_def.sql_column.name.to_string());
    }

    let catalog_name = meta_util::cut_out_catalog_name(full_schema_name.clone());
    let schema_name = meta_util::cut_out_schema_name(full_schema_name.clone());

    let mut column_value_map_list: Vec<HashMap<Ident, ScalarValue>> = vec![];
    let mut column_value_map = HashMap::new();
    column_value_map.insert("catalog_name".to_ident(), ScalarValue::Utf8(Some(catalog_name.to_string())));
    column_value_map.insert("schema_name".to_ident(), ScalarValue::Utf8(Some(schema_name.to_string())));
    column_value_map.insert("default_character_set_name".to_ident(), ScalarValue::Utf8(Some("utf8mb4".to_string())));
    column_value_map.insert("default_collation_name".to_ident(), ScalarValue::Utf8(Some("utf8mb4_0900_ai_ci".to_string())));
    column_value_map.insert("SQL_PATH".to_ident(), ScalarValue::Utf8(Some("".to_string())));
    column_value_map.insert("DEFAULT_ENCRYPTION".to_ident(), ScalarValue::Utf8(Some("NO".to_string())));
    column_value_map_list.push(column_value_map);

    let table_def = initial::information_schema::table_schemata();
    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA.to_object_name();
    let insert = physical_plan::insert::PhysicalPlanInsert::new(
        global_context.clone(),
        full_table_name,
        table_def,
        column_name_list.clone(),
        vec![],
        column_value_map_list.clone(),
    );
    let total = insert.execute();
    total
}

pub fn read_all_table(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<HashMap<ObjectName, TableDef>> {
    let schema_table_sql_options = read_information_schema_tables(global_context.clone()).unwrap();
    let schema_table_columns = read_information_schema_columns(global_context.clone()).unwrap();
    let schema_table_constraints = read_information_schema_statistics(global_context.clone()).unwrap();

    let mut all_schema: HashMap<ObjectName, TableDef> = HashMap::new();

    for (full_table_name, mut table_columns) in schema_table_columns {
        //let table_name = cut_out_table_name(full_table_name.clone());

        table_columns.sort_by(|a, b| a.ordinal_position.cmp(&b.ordinal_position));

        let mut table_constraints = vec![];
        match schema_table_constraints.get(&full_table_name.clone()) {
            None => {}
            Some(tc) => {
                table_constraints = tc.to_vec();
            }
        }

        let mut sql_options = vec![];
        match schema_table_sql_options.get(&full_table_name.clone()) {
            None => {}
            Some(tc) => {
                sql_options = tc.to_vec();
            }
        }

        let table_schema = TableDef::new_with_column(full_table_name.to_string().as_str(), table_columns.to_vec(), table_constraints.clone(), sql_options.clone());

        all_schema.insert(full_table_name.clone(), table_schema);
    }

    Ok(all_schema)
}

pub fn read_information_schema_tables(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<HashMap<ObjectName, Vec<SqlOption>>> {
    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES.to_object_name();

    let engine = engine_util::TableEngineFactory::try_new_with_table(global_context.clone(), full_table_name).unwrap();
    let mut table_iterator = engine.table_iterator(None);

    let projection_schema = information_schema::table_tables().to_schema();

    let column_index_of_db_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_SCHEMA).unwrap();
    let column_index_of_table_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_NAME).unwrap();
    let column_index_of_table_type = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_TYPE).unwrap();
    let column_index_of_engine = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_ENGINE).unwrap();

    let mut table_sql_options: HashMap<ObjectName, Vec<SqlOption>> = HashMap::new();
    loop {
        match table_iterator.next() {
            Some(item) => {
                match item {
                    Ok(record_batch) => {
                        let db_name_row: &StringArray = as_string_array(record_batch.column(column_index_of_db_name));
                        let table_name_row: &StringArray = as_string_array(record_batch.column(column_index_of_table_name));
                        let table_type_row: &StringArray = as_string_array(record_batch.column(column_index_of_table_type));
                        let engine_row: &StringArray = as_string_array(record_batch.column(column_index_of_engine));

                        for row_index in 0..record_batch.num_rows() {
                            let db_name = db_name_row.value(row_index).to_string();
                            let table_name = table_name_row.value(row_index).to_string();
                            let table_type = table_type_row.value(row_index).to_string();
                            let engine = engine_row.value(row_index).to_string();

                            let full_table_name = meta_util::create_full_table_name(meta_const::CATALOG_NAME, db_name.as_str(), table_name.as_str());

                            let mut with_option = vec![];
                            let sql_option = SqlOption { name: Ident { value: meta_const::OPTION_TABLE_TYPE.to_string(), quote_style: None }, value: Value::SingleQuotedString(table_type.clone()) };
                            with_option.push(sql_option);
                            let sql_option = SqlOption { name: Ident { value: meta_const::OPTION_ENGINE.to_string(), quote_style: None }, value: Value::SingleQuotedString(engine.clone()) };
                            with_option.push(sql_option);

                            table_sql_options.entry(full_table_name.clone()).or_insert(with_option);
                        }
                    }
                    Err(arrow_error) => return Err(MysqlError::from(arrow_error)),
                }
            }
            None => break,
        }
    }

    Ok(table_sql_options.clone())
}

pub fn read_information_schema_schemata(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<HashMap<ObjectName, DbDef>> {
    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA.to_object_name();

    let engine = engine_util::TableEngineFactory::try_new_with_table(global_context.clone(), full_table_name.clone()).unwrap();
    let mut table_iterator = engine.table_iterator(None);

    let projection_schema = information_schema::table_schemata().to_schema();

    let column_index_of_db_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_SCHEMA_NAME).unwrap();
    let column_index_of_default_character_set_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_DEFAULT_CHARACTER_SET_NAME).unwrap();
    let column_index_of_default_collation_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_DEFAULT_COLLATION_NAME).unwrap();

    let mut schema_map: HashMap<ObjectName, DbDef> = HashMap::new();
    loop {
        match table_iterator.next() {
            Some(item) => {
                match item {
                    Ok(record_batch) => {
                        let column_of_db_name: &StringArray = as_string_array(record_batch.column(column_index_of_db_name));
                        let column_of_default_character_set_name: &StringArray = as_string_array(record_batch.column(column_index_of_default_character_set_name));
                        let column_of_default_collation_name: &StringArray = as_string_array(record_batch.column(column_index_of_default_collation_name));

                        for row_index in 0..record_batch.num_rows() {
                            let db_name = column_of_db_name.value(row_index).to_string();
                            let default_character_set_name = column_of_default_character_set_name.value(row_index);
                            let default_collation_name = column_of_default_collation_name.value(row_index);

                            let mut with_option = vec![];
                            let sql_option = SqlOption { name: Ident { value: "DEFAULT_CHARACTER_SET_NAME".to_string(), quote_style: None }, value: Value::SingleQuotedString(default_character_set_name.to_string()) };
                            with_option.push(sql_option);
                            let sql_option = SqlOption { name: Ident { value: "DEFAULT_COLLATION_NAME".to_string(), quote_style: None }, value: Value::SingleQuotedString(default_collation_name.to_string()) };
                            with_option.push(sql_option);

                            let full_schema_name = meta_util::create_full_schema_name(meta_const::CATALOG_NAME, db_name.as_str());
                            let schema_def = DbDef::new(with_option);

                            schema_map.entry(full_schema_name.clone()).or_insert(schema_def);
                        }
                    }
                    Err(arrow_error) => return Err(MysqlError::from(arrow_error)),
                }
            }
            None => break,
        }
    }

    Ok(schema_map)
}

pub fn read_information_schema_statistics(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<HashMap<ObjectName, Vec<TableConstraint>>> {
    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS.to_object_name();

    let engine = engine_util::TableEngineFactory::try_new_with_table(global_context.clone(), full_table_name.clone()).unwrap();
    let mut table_iterator = engine.table_iterator(None);

    let projection_schema = information_schema::table_statistics().to_schema();

    let column_index_of_db_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_SCHEMA).unwrap();
    let column_index_of_table_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_TABLE_NAME).unwrap();
    let column_index_of_index_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_INDEX_NAME).unwrap();
    let column_index_of_seq_in_index = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_SEQ_IN_INDEX).unwrap();
    let column_index_of_column_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_COLUMN_NAME).unwrap();

    let mut schema_table_index: HashMap<ObjectName, HashMap<String, Vec<StatisticsColumn>>> = HashMap::new();
    loop {
        match table_iterator.next() {
            Some(item) => {
                match item {
                    Ok(record_batch) => {
                        let column_of_db_name: &StringArray = as_string_array(record_batch.column(column_index_of_db_name));
                        let column_of_table_name: &StringArray = as_string_array(record_batch.column(column_index_of_table_name));
                        let column_of_index_name: &StringArray = as_string_array(record_batch.column(column_index_of_index_name));
                        let column_of_seq_in_index: &Int32Array = as_primitive_array(record_batch.column(column_index_of_seq_in_index));
                        let column_of_column_name: &StringArray = as_string_array(record_batch.column(column_index_of_column_name));

                        for row_index in 0..record_batch.num_rows() {
                            let db_name = column_of_db_name.value(row_index);
                            let table_name = column_of_table_name.value(row_index);
                            let index_name = column_of_index_name.value(row_index).to_string();
                            let seq_in_index = column_of_seq_in_index.value(row_index) as usize;
                            let column_name = column_of_column_name.value(row_index).to_string();

                            let full_table_name = meta_util::create_full_table_name(meta_const::CATALOG_NAME, db_name, table_name);

                            let sc = StatisticsColumn {
                                column_name: column_name.clone(),
                                seq_in_index,
                            };

                            schema_table_index
                                .entry(full_table_name.clone()).or_insert(HashMap::new())
                                .entry(index_name.clone()).or_insert(Vec::new())
                                .push(sc);
                        }
                    }
                    Err(arrow_error) => return Err(MysqlError::from(arrow_error)),
                }
            }
            None => break,
        }
    }

    let mut schema_table_constraint: HashMap<ObjectName, Vec<TableConstraint>> = HashMap::new();

    for (schema_name, index_field) in schema_table_index.iter() {
        let mut table_constraints = vec![];
        for (index_name, column_field) in index_field.iter() {
            let mut column_field = column_field.to_vec();
            column_field.sort_by(|a, b| a.seq_in_index.cmp(&b.seq_in_index));

            let columns = column_field.iter().map(|statistics_column| Ident { value: statistics_column.column_name.clone(), quote_style: None }).collect::<Vec<_>>();
            let table_constraint = TableConstraint::Unique {
                name: Some(Ident { value: index_name.to_string(), quote_style: None }),
                columns,
                is_primary: true,
            };
            table_constraints.push(table_constraint);
        }

        schema_table_constraint
            .entry(schema_name.clone()).or_insert(table_constraints);
    }

    Ok(schema_table_constraint.clone())
}

pub fn read_information_schema_columns(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<HashMap<ObjectName, Vec<ColumnDef>>> {
    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS.to_object_name();

    let engine = engine_util::TableEngineFactory::try_new_with_table(global_context.clone(), full_table_name.clone()).unwrap();
    let mut table_iterator = engine.table_iterator(None);

    let projection_schema = information_schema::table_columns().to_schema();

    let column_index_of_db_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_SCHEMA).unwrap();
    let column_index_of_table_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_NAME).unwrap();
    let column_index_of_column_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_COLUMN_NAME).unwrap();
    let column_index_of_ordinal_position = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_ORDINAL_POSITION).unwrap();
    let column_index_of_is_nullable = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_IS_NULLABLE).unwrap();
    let column_index_of_data_type = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_DATA_TYPE).unwrap();

    let mut schema_column: HashMap<ObjectName, Vec<ColumnDef>> = HashMap::new();
    loop {
        match table_iterator.next() {
            Some(item) => {
                match item {
                    Ok(record_batch) => {
                        let column_of_db_name: &StringArray = as_string_array(record_batch.column(column_index_of_db_name));
                        let column_of_table_name: &StringArray = as_string_array(record_batch.column(column_index_of_table_name));
                        let column_of_column_name: &StringArray = as_string_array(record_batch.column(column_index_of_column_name));
                        let column_of_ordinal_position: &Int32Array = as_primitive_array(record_batch.column(column_index_of_ordinal_position));
                        let column_of_is_nullable: &StringArray = as_string_array(record_batch.column(column_index_of_is_nullable));
                        let column_of_data_type: &StringArray = as_string_array(record_batch.column(column_index_of_data_type));

                        for row_index in 0..record_batch.num_rows() {
                            let db_name = column_of_db_name.value(row_index).to_string();
                            let table_name = column_of_table_name.value(row_index).to_string();
                            let column_name = column_of_column_name.value(row_index).to_string();
                            let ordinal_position = column_of_ordinal_position.value(row_index) as usize;
                            let is_nullable = column_of_is_nullable.value(row_index).to_string();
                            let data_type = column_of_data_type.value(row_index).to_string();

                            let full_table_name = meta_util::create_full_table_name(meta_const::CATALOG_NAME, db_name.as_str(), table_name.as_str());

                            let sql_data_type = meta_util::text_to_sql_data_type(data_type.as_str()).unwrap();
                            let nullable = meta_util::text_to_null(is_nullable.as_str()).unwrap();

                            let sql_column = meta_util::create_sql_column(column_name.as_str(), sql_data_type, nullable);
                            let column = meta_util::create_column(ordinal_position, sql_column);

                            schema_column.entry(full_table_name.clone()).or_insert(Vec::new()).push(column);
                        }
                    }
                    Err(arrow_error) => return Err(MysqlError::from(arrow_error)),
                }
            }
            None => break,
        }
    }

    Ok(schema_column.clone())
}

pub fn read_performance_schema_global_variables(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<HashMap<String, String>> {
    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES.to_object_name();

    let engine = engine_util::TableEngineFactory::try_new_with_table(global_context.clone(), full_table_name.clone()).unwrap();
    let mut table_iterator = engine.table_iterator(None);

    let projection_schema = performance_schema::global_variables().to_schema();

    let column_index_of_variable_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_NAME).unwrap();
    let column_index_of_variable_value = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_VALUE).unwrap();

    let mut variable_map: HashMap<String, String> = HashMap::new();
    loop {
        match table_iterator.next() {
            Some(item) => {
                match item {
                    Ok(record_batch) => {
                        let column_of_variable_name: &StringArray = as_string_array(record_batch.column(column_index_of_variable_name));
                        let column_of_variable_value: &StringArray = as_string_array(record_batch.column(column_index_of_variable_value));

                        for row_index in 0..record_batch.num_rows() {
                            let variable_name = column_of_variable_name.value(row_index);
                            let variable_value = column_of_variable_value.value(row_index);

                            variable_map.insert(variable_name.to_string(), variable_value.to_string());
                        }
                    }
                    Err(arrow_error) => return Err(MysqlError::from(arrow_error)),
                }
            }
            None => break,
        }
    }

    Ok(variable_map.clone())
}

pub fn read_column_index(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<HashMap<ObjectName, HashMap<Ident, usize>>> {
    let full_table_name = meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS.to_object_name();

    let engine = engine_util::TableEngineFactory::try_new_with_table(global_context.clone(), full_table_name.clone()).unwrap();
    let mut iterator = engine.table_iterator(None);

    let projection_schema = information_schema::table_columns().to_schema();

    let column_index_of_db_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_SCHEMA).unwrap();
    let column_index_of_table_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_NAME).unwrap();
    let column_index_of_column_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_COLUMN_NAME).unwrap();

    let mut schema_column_index: HashMap<ObjectName, HashMap<Ident, usize>> = HashMap::new();
    loop {
        match iterator.next() {
            Some(item) => {
                match item {
                    Ok(record_batch) => {
                        let column_of_db_name: &StringArray = as_string_array(record_batch.column(column_index_of_db_name));
                        let column_of_table_name: &StringArray = as_string_array(record_batch.column(column_index_of_table_name));
                        let column_of_column_name: &StringArray = as_string_array(record_batch.column(column_index_of_column_name));

                        for row_index in 0..record_batch.num_rows() {
                            let db_name = column_of_db_name.value(row_index).to_string();
                            let table_name = column_of_table_name.value(row_index).to_string();
                            let column_name = column_of_column_name.value(row_index).to_string();

                            let full_table_name = meta_util::create_full_table_name(meta_const::CATALOG_NAME, db_name.as_str(), table_name.as_str());
                            let column_name = column_name.to_ident();

                            let column_index = meta_util::store_get_column_serial_number(global_context.clone(), full_table_name.clone(), column_name.clone());
                            match column_index {
                                Ok(column_index) => {
                                    match column_index {
                                        Some(column_index) => {
                                            schema_column_index
                                                .entry(full_table_name.clone()).or_insert(HashMap::new())
                                                .entry(column_name.clone()).or_insert(column_index);
                                        }
                                        _ => {}
                                    }
                                }
                                Err(mysql_error) => {
                                    return Err(mysql_error);
                                }
                            }
                        }
                    }
                    Err(arrow_error) => return Err(MysqlError::from(arrow_error)),
                }
            }
            None => break,
        }
    }

    Ok(schema_column_index.clone())
}
