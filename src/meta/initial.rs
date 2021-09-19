use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::{Arc, Mutex};

use arrow::array::{as_primitive_array, as_string_array, Int32Array, StringArray};
use datafusion::catalog::TableReference;
use datafusion::logical_plan::Expr;
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnDef, ColumnOption, Ident, ObjectName, SqlOption, TableConstraint, Value};

use crate::core::global_context::GlobalContext;
use crate::meta::{def, initial, meta_const, meta_util};
use crate::meta::def::{information_schema, mysql};
use crate::meta::def::performance_schema;
use crate::meta::meta_def::{SchemaDef, SchemaOptionDef, SparrowColumnDef, StatisticsColumn, TableColumnDef, TableDef, TableOptionDef};
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::physical_plan;
use crate::physical_plan::insert::PhysicalPlanInsert;
use crate::store::engine::engine_util;
use crate::store::engine::engine_util::{ADD_ENTRY_TYPE, TableEngine, TableEngineFactory};
use crate::store::reader::rocksdb::RocksdbReader;
use crate::util::convert::{ToIdent, ToObjectName};

pub fn create_table(global_context: Arc<Mutex<GlobalContext>>, schema_name: &str, table_name: &str, sql_column_list: Vec<SQLColumnDef>, constraints: Vec<TableConstraint>) -> TableDef {
    let gc = global_context.lock().unwrap();

    let mut table_column = TableColumnDef::default();
    table_column.use_sql_column_list(sql_column_list);

    let column_max_store_id = table_column.get_max_store_id();

    let mut table_option = TableOptionDef::new(
        meta_const::CATALOG_NAME,
        schema_name,
        table_name,
    );
    table_option.with_engine(gc.my_config.schema.engine.as_str());
    table_option.with_table_type(meta_const::VALUE_OF_TABLE_OPTION_TABLE_TYPE_BASE_TABLE);
    table_option.with_column_max_store_id(column_max_store_id);

    let mut table = TableDef::new();
    table.with_column(table_column.clone());
    table.with_option(table_option.clone());
    table.with_constraints(constraints.clone());

    table
}

pub fn create_schema(global_context: Arc<Mutex<GlobalContext>>, full_schema_name: ObjectName) -> MysqlResult<u64> {
    let mut column_name_list = vec![];
    for sql_column in def::information_schema::schemata(global_context.clone()).column.sql_column_list {
        column_name_list.push(sql_column.name.to_string());
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

    let table_def = def::information_schema::schemata(global_context.clone());
    let insert = physical_plan::insert::PhysicalPlanInsert::new(
        global_context.clone(),
        table_def,
        column_name_list.clone(),
        vec![],
        column_value_map_list.clone(),
    );
    let total = insert.execute();
    total
}

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
        let table_def = def::information_schema::table_constraints(self.global_context.clone());

        let mut column_name_list = vec![];
        for column_def in table_def.get_columns() {
            column_name_list.push(column_def.sql_column.name.to_string());
        }

        let insert = PhysicalPlanInsert::new(
            self.global_context.clone(),
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
        let table_def = def::information_schema::key_column_usage(self.global_context.clone());

        let mut column_name_list = vec![];
        for column_def in table_def.get_columns() {
            column_name_list.push(column_def.sql_column.name.to_string());
        }

        let insert = physical_plan::insert::PhysicalPlanInsert::new(
            self.global_context.clone(),
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
        let table_def = def::information_schema::statistics(self.global_context.clone());

        let mut column_name_list = vec![];
        for column_def in def::information_schema::statistics(self.global_context.clone()).get_columns() {
            column_name_list.push(column_def.sql_column.name.to_string());
        }

        let insert = physical_plan::insert::PhysicalPlanInsert::new(
            self.global_context.clone(),
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
    let gc = global_context.lock().unwrap();

    let table_def = gc.meta_data.get_table(full_table_name.clone()).unwrap();
    let schema_ref = table_def.to_schema_ref();

    let rowid_index = schema_ref.index_of(meta_const::COLUMN_ROWID).unwrap();
    let db_name_index = schema_ref.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_SCHEMA_NAME).unwrap();
    let projection = Some(vec![rowid_index, db_name_index]);

    let mut store_engine = engine_util::StoreEngineFactory::try_new_with_table_name(global_context.clone(), full_table_name.clone()).unwrap();
    let mut table_engine = engine_util::TableEngineFactory::try_new_with_table_name(global_context.clone(), full_table_name.clone()).unwrap();
    let mut table_iterator = table_engine.table_iterator(projection, &[]);

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

pub fn add_information_schema_tables(global_context: Arc<Mutex<GlobalContext>>, table_option: TableOptionDef) -> MysqlResult<u64> {
    let table_of_def_information_schema_tables = def::information_schema::tables(global_context.clone());

    let mut column_name_list = vec![];
    for sql_column in table_of_def_information_schema_tables.column.sql_column_list.clone() {
        column_name_list.push(sql_column.name.to_string());
    }

    let catalog_name = table_option.catalog_name;
    let schema_name = table_option.schema_name;
    let table_name = table_option.table_name;
    let table_type = table_option.table_type;
    let column_max_store_id = table_option.column_max_store_id;

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
    column_value_map.insert(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_COLUMN_MAX_STORE_ID.to_ident(), ScalarValue::Int32(Some(column_max_store_id)));
    column_value_map_list.push(column_value_map);

    let insert = physical_plan::insert::PhysicalPlanInsert::new(
        global_context.clone(),
        table_of_def_information_schema_tables,
        column_name_list.clone(),
        vec![],
        column_value_map_list.clone(),
    );
    let total = insert.execute();

    total
}

pub fn add_information_schema_columns(global_context: Arc<Mutex<GlobalContext>>, table_option: TableOptionDef, sparrow_column_list: Vec<SparrowColumnDef>) -> MysqlResult<u64> {
    let meta_table = def::information_schema::columns(global_context.clone());

    let mut column_name_list = vec![];
    for sql_column in &meta_table.column.sql_column_list {
        column_name_list.push(sql_column.name.to_string());
    }

    let catalog_name = table_option.catalog_name;
    let schema_name = table_option.schema_name;
    let table_name = table_option.table_name;

    let table_has_primary_key = meta_util::table_has_primary_key(global_context.clone(), table_option.full_table_name.clone());

    let mut create_statistics = SaveStatistics::new(global_context.clone(), catalog_name.as_str(), schema_name.as_str(), table_name.as_str());

    let mut column_value_map_list: Vec<HashMap<Ident, ScalarValue>> = vec![];
    for sparrow_column in sparrow_column_list {
        let column_name = sparrow_column.sql_column.name.to_string();
        for x in sparrow_column.sql_column.options.clone() {
            match x.option {
                ColumnOption::Unique { is_primary } => {
                    if is_primary {
                        if table_has_primary_key {
                            return Err(MysqlError::new_server_error(1068, "42000", "Multiple primary key defined"));
                        }
                        create_statistics.add_row(meta_const::NAME_OF_PRIMARY, 1, column_name.as_str())
                    } else {
                        create_statistics.add_row(column_name.as_str(), 1, column_name.as_str())
                    }
                }
                _ => {}
            }
        }

        let data_type = meta_util::convert_sql_data_type_to_text(&sparrow_column.sql_column.data_type).unwrap();

        let allow_null = sparrow_column.sql_column.options.clone()
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
        column_value_map.insert("store_id".to_ident(), ScalarValue::Int32(Some(sparrow_column.store_id)));
        /// ORDINAL_POSITION
        column_value_map.insert("ordinal_position".to_ident(), ScalarValue::Int32(Some(sparrow_column.ordinal_position)));
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

    create_statistics.save();

    let insert = physical_plan::insert::PhysicalPlanInsert::new(
        global_context.clone(),
        meta_table,
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

    for (full_table_name, mut table_option) in schema_table_sql_options {
        let mut table_column = TableColumnDef::default();
        match schema_table_columns.get(&full_table_name.clone()) {
            None => {}
            Some(tc) => {
                table_column.use_sparrow_column_list(tc.clone());
            }
        }

        let mut table_constraints = vec![];
        match schema_table_constraints.get(&full_table_name.clone()) {
            None => {}
            Some(tc) => {
                table_constraints = tc.to_vec();
            }
        }

        let mut table_def = TableDef::new();
        table_def.with_column(table_column);
        table_def.with_constraints(table_constraints);
        table_def.with_option(table_option);

        all_schema.insert(full_table_name.clone(), table_def);
    }

    Ok(all_schema)
}

pub fn read_information_schema_tables(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<HashMap<ObjectName, TableOptionDef>> {
    let table_of_def_information_schema_tables = information_schema::tables(global_context.clone());

    let engine = engine_util::TableEngineFactory::try_new_with_table(global_context.clone(), table_of_def_information_schema_tables.clone()).unwrap();
    let mut table_iterator = engine.table_iterator(None, &[]);

    let projection_schema = table_of_def_information_schema_tables.to_schema();

    let column_index_of_db_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_SCHEMA).unwrap();
    let column_index_of_table_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_NAME).unwrap();
    let column_index_of_table_type = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_TYPE).unwrap();
    let column_index_of_engine = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_ENGINE).unwrap();
    let projection_index_of_column_store_id = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_COLUMN_MAX_STORE_ID).unwrap();

    let mut table_sql_options: HashMap<ObjectName, TableOptionDef> = HashMap::new();
    loop {
        match table_iterator.next() {
            Some(item) => {
                match item {
                    Ok(record_batch) => {
                        let db_name_row: &StringArray = as_string_array(record_batch.column(column_index_of_db_name));
                        let table_name_row: &StringArray = as_string_array(record_batch.column(column_index_of_table_name));
                        let table_type_row: &StringArray = as_string_array(record_batch.column(column_index_of_table_type));
                        let engine_row: &StringArray = as_string_array(record_batch.column(column_index_of_engine));
                        let column_store_id_row: &Int32Array = as_primitive_array(record_batch.column(projection_index_of_column_store_id));

                        for row_index in 0..record_batch.num_rows() {
                            let schema_name = db_name_row.value(row_index).to_string();
                            let table_name = table_name_row.value(row_index).to_string();
                            let table_type = table_type_row.value(row_index).to_string();
                            let engine = engine_row.value(row_index).to_string();
                            let column_store_id = column_store_id_row.value(row_index);

                            let full_table_name = meta_util::create_full_table_name(meta_const::CATALOG_NAME, schema_name.as_str(), table_name.as_str());

                            let mut table_option = TableOptionDef::new(meta_const::CATALOG_NAME, schema_name.as_str(), table_name.as_str());
                            table_option.with_table_type(table_type.as_str());
                            table_option.with_column_max_store_id(column_store_id);
                            table_option.with_engine(engine.as_str());

                            table_sql_options.entry(full_table_name.clone()).or_insert(table_option);
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

pub fn read_information_schema_schemata(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<HashMap<ObjectName, SchemaDef>> {
    let table_of_information_schema_schemata = information_schema::schemata(global_context.clone());

    let engine = engine_util::TableEngineFactory::try_new_with_table(global_context.clone(), table_of_information_schema_schemata.clone()).unwrap();
    let mut table_iterator = engine.table_iterator(None, &[]);

    let projection_schema = table_of_information_schema_schemata.to_schema();

    let column_index_of_db_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_SCHEMA_NAME).unwrap();
    let column_index_of_default_character_set_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_DEFAULT_CHARACTER_SET_NAME).unwrap();
    let column_index_of_default_collation_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_DEFAULT_COLLATION_NAME).unwrap();

    let mut schema_map: HashMap<ObjectName, SchemaDef> = HashMap::new();
    loop {
        match table_iterator.next() {
            Some(item) => {
                match item {
                    Ok(record_batch) => {
                        let column_of_db_name: &StringArray = as_string_array(record_batch.column(column_index_of_db_name));
                        let column_of_default_character_set_name: &StringArray = as_string_array(record_batch.column(column_index_of_default_character_set_name));
                        let column_of_default_collation_name: &StringArray = as_string_array(record_batch.column(column_index_of_default_collation_name));

                        for row_index in 0..record_batch.num_rows() {
                            let schema_name = column_of_db_name.value(row_index).to_string();
                            let default_character_set_name = column_of_default_character_set_name.value(row_index).to_string();
                            let default_collation_name = column_of_default_collation_name.value(row_index).to_string();

                            let mut schema_option = SchemaOptionDef::new(meta_const::CATALOG_NAME, schema_name.as_str());
                            schema_option.with_default_character_set_name(default_character_set_name.as_str());
                            schema_option.with_default_collation_name(default_collation_name.as_str());

                            let full_schema_name = meta_util::create_full_schema_name(meta_const::CATALOG_NAME, schema_name.as_str());
                            let mut schema = SchemaDef::new(full_schema_name.clone());
                            schema.with_schema_option(schema_option);

                            schema_map.entry(full_schema_name.clone()).or_insert(schema);
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
    let table_def = information_schema::statistics(global_context.clone());

    let engine = engine_util::TableEngineFactory::try_new_with_table(global_context.clone(), table_def.clone()).unwrap();
    let mut table_iterator = engine.table_iterator(None, &[]);

    let projection_schema = table_def.to_schema();

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

pub fn read_information_schema_columns(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<HashMap<ObjectName, Vec<SparrowColumnDef>>> {
    let table_def = information_schema::columns(global_context.clone());

    let engine = engine_util::TableEngineFactory::try_new_with_table(global_context.clone(), table_def.clone()).unwrap();
    let mut table_iterator = engine.table_iterator(None, &[]);

    let projection_schema = table_def.to_schema();

    let column_index_of_db_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_SCHEMA).unwrap();
    let column_index_of_table_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_NAME).unwrap();
    let column_index_of_column_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_COLUMN_NAME).unwrap();
    let projection_index_of_store_id = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_STORE_ID).unwrap();
    let column_index_of_ordinal_position = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_ORDINAL_POSITION).unwrap();
    let column_index_of_is_nullable = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_IS_NULLABLE).unwrap();
    let column_index_of_data_type = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_DATA_TYPE).unwrap();

    let mut schema_column: HashMap<ObjectName, Vec<SparrowColumnDef>> = HashMap::new();
    loop {
        match table_iterator.next() {
            Some(item) => {
                match item {
                    Ok(record_batch) => {
                        let column_of_db_name: &StringArray = as_string_array(record_batch.column(column_index_of_db_name));
                        let column_of_table_name: &StringArray = as_string_array(record_batch.column(column_index_of_table_name));
                        let column_of_column_name: &StringArray = as_string_array(record_batch.column(column_index_of_column_name));
                        let store_id_row: &Int32Array = as_primitive_array(record_batch.column(projection_index_of_store_id));
                        let column_of_ordinal_position: &Int32Array = as_primitive_array(record_batch.column(column_index_of_ordinal_position));
                        let column_of_is_nullable: &StringArray = as_string_array(record_batch.column(column_index_of_is_nullable));
                        let column_of_data_type: &StringArray = as_string_array(record_batch.column(column_index_of_data_type));

                        for row_index in 0..record_batch.num_rows() {
                            let db_name = column_of_db_name.value(row_index).to_string();
                            let table_name = column_of_table_name.value(row_index).to_string();
                            let column_name = column_of_column_name.value(row_index).to_string();
                            let store_id = store_id_row.value(row_index);
                            let ordinal_position = column_of_ordinal_position.value(row_index);
                            let is_nullable = column_of_is_nullable.value(row_index).to_string();
                            let data_type = column_of_data_type.value(row_index).to_string();

                            let full_table_name = meta_util::create_full_table_name(meta_const::CATALOG_NAME, db_name.as_str(), table_name.as_str());

                            let sql_data_type = meta_util::text_to_sql_data_type(data_type.as_str()).unwrap();
                            let nullable = meta_util::text_to_null(is_nullable.as_str()).unwrap();

                            let sql_column = meta_util::create_sql_column(column_name.as_str(), sql_data_type, nullable);
                            let sparrow_column = meta_util::create_sparrow_column(store_id, ordinal_position, sql_column);

                            schema_column.entry(full_table_name.clone()).or_insert(Vec::new()).push(sparrow_column);
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
    let table_def = performance_schema::global_variables(global_context.clone());

    let engine = engine_util::TableEngineFactory::try_new_with_table(global_context.clone(), table_def.clone()).unwrap();
    let mut table_iterator = engine.table_iterator(None, &[]);

    let projection_schema = performance_schema::global_variables(global_context.clone()).to_schema();

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

pub fn add_def_mysql_users(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<u64> {
    let table_of_def_mysql_users = mysql::users(global_context.clone());

    let mut column_name_list = vec![];
    for sql_column in table_of_def_mysql_users.column.sql_column_list.clone() {
        column_name_list.push(sql_column.name.to_string());
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

    let insert = PhysicalPlanInsert::new(
        global_context.clone(),
        table_of_def_mysql_users,
        column_name_list.clone(),
        vec![],
        column_value_map_list.clone(),
    );
    let total = insert.execute().unwrap();

    Ok(total)
}

pub fn add_def_performance_schmea_global_variables(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<u64> {
    let mut column_name_list = vec![];
    for column_def in performance_schema::global_variables(global_context.clone()).get_columns() {
        column_name_list.push(column_def.sql_column.name.to_string());
    }

    let mut column_value_map_list: Vec<HashMap<Ident, ScalarValue>> = vec![];
    let mut column_value_map = HashMap::new();
    column_value_map.insert("variable_name".to_ident(), ScalarValue::Utf8(Some("auto_increment_increment".to_string())));
    column_value_map.insert("variable_value".to_ident(), ScalarValue::Utf8(Some("0".to_string())));
    column_value_map_list.push(column_value_map);
    let mut column_value_map = HashMap::new();
    column_value_map.insert("variable_name".to_ident(), ScalarValue::Utf8(Some("lower_case_table_names".to_string())));
    column_value_map.insert("variable_value".to_ident(), ScalarValue::Utf8(Some("1".to_string())));
    column_value_map_list.push(column_value_map);
    let mut column_value_map = HashMap::new();
    column_value_map.insert("variable_name".to_ident(), ScalarValue::Utf8(Some("transaction_isolation".to_string())));
    column_value_map.insert("variable_value".to_ident(), ScalarValue::Utf8(None));
    column_value_map_list.push(column_value_map);
    let mut column_value_map = HashMap::new();
    column_value_map.insert("variable_name".to_ident(), ScalarValue::Utf8(Some("transaction_read_only".to_string())));
    column_value_map.insert("variable_value".to_ident(), ScalarValue::Utf8(Some("0".to_string())));
    column_value_map_list.push(column_value_map);

    let table_def = performance_schema::global_variables(global_context.clone());

    let insert = PhysicalPlanInsert::new(
        global_context.clone(),
        table_def,
        column_name_list.clone(),
        vec![],
        column_value_map_list.clone(),
    );
    let total = insert.execute().unwrap();

    Ok(total)
}

pub fn get_full_table_name_list(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<Vec<ObjectName>> {
    let meta_table = information_schema::tables(global_context.clone());

    let engine = TableEngineFactory::try_new_with_table(global_context.clone(), meta_table).unwrap();
    let mut table_iterator = engine.table_iterator(None, &[]);

    let projection_schema = information_schema::tables(global_context.clone()).to_schema();

    let column_index_of_db_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_SCHEMA).unwrap();
    let column_index_of_table_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_NAME).unwrap();

    let mut table_list: Vec<ObjectName> = vec![];
    loop {
        match table_iterator.next() {
            Some(item) => {
                match item {
                    Ok(record_batch) => {
                        let db_name_row: &StringArray = as_string_array(record_batch.column(column_index_of_db_name));
                        let table_name_row: &StringArray = as_string_array(record_batch.column(column_index_of_table_name));

                        for row_index in 0..record_batch.num_rows() {
                            let db_name = db_name_row.value(row_index).to_string();
                            let table_name = table_name_row.value(row_index).to_string();

                            let full_table_name = meta_util::create_full_table_name(meta_const::CATALOG_NAME, db_name.as_str(), table_name.as_str());
                            table_list.push(full_table_name);
                        }
                    }
                    Err(arrow_error) => return Err(MysqlError::from(arrow_error)),
                }
            }
            None => break,
        }
    }

    Ok(table_list)
}
