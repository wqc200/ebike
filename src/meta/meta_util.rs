use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::env;
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::io::prelude::*;
use std::panic::set_hook;
use std::process::*;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, as_primitive_array, as_string_array};
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
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use arrow::datatypes::ToByteSlice;
use arrow::record_batch::RecordBatch;
use bytes::Buf;
use datafusion::catalog::ResolvedTableReference;
use datafusion::catalog::TableReference;
use datafusion::datasource::csv::{CsvFile, CsvReadOptions};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_plan::{col, DFField, DFSchema, DFSchemaRef, Expr};
use datafusion::scalar::ScalarValue;
use parquet::data_type::AsBytes;
use sqlparser::ast::{Assignment, BinaryOperator, ColumnDef as SQLColumnDef, ColumnOption, ColumnOptionDef, DataType as SQLDataType, Expr as SQLExpr, Ident, ObjectName, SqlOption, TableConstraint, Value};
use tempdir::TempDir;
use uuid::Uuid;

use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::meta::{initial, meta_const, meta_util};
use crate::meta::def::{SparrowColumnDef, SchemaDef, StatisticsColumn, TableDef, TableOptionDef, TableIndexDef};
use crate::meta::initial::information_schema::{key_column_usage, table_constraints};
use crate::meta::initial::initial_util::{SaveKeyColumnUsage, SaveStatistics, SaveTableConstraints};
use crate::meta::initial::initial_util;
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::physical_plan::create_table::CreateTable;
use crate::physical_plan::delete::Delete;
use crate::physical_plan::insert::PhysicalPlanInsert;
use crate::store::engine::engine_util;
use crate::store::engine::engine_util::{TableEngine, TableEngineFactory, StoreEngine, StoreEngineFactory};
use crate::store::reader::rocksdb::RocksdbReader;
use crate::store::rocksdb::db::DB;
use crate::store::rocksdb::option::Options;
use crate::util::convert::{ToIdent, ToObjectName};

use super::super::util;
use futures::StreamExt;

// pub const INFORMATION_SCHEMA_NUMBER_SCHEMATA: &str = "information_schema.number_schemata";
// pub const INFORMATION_SCHEMA_NUMBER_TABLE: &str = "information_schema.number_table";
// pub const INFORMATION_SCHEMA_NUMBER_COLUMN: &str = "information_schema.number_column";

pub fn get_table(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) -> MysqlResult<TableDef> {
    let table_map = global_context.lock().unwrap().meta_cache.get_table_map();
    match table_map.get(&full_table_name) {
        Some(table) => {
            Ok((table.clone()))
        }
        None => {
            let message = format!("Table '{}' doesn't exist", full_table_name.to_string());
            log::error!("{}", message);
            Err(MysqlError::new_server_error(
                1146,
                "42S02",
                message.as_str(),
            ))
        }
    }
}

pub fn get_table_index_list(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) -> MysqlResult<Vec<TableIndexDef>> {
    let table = meta_util::get_table(global_context.clone(), full_table_name.clone()).unwrap();

    let mut all_index = vec![];
    for table_constraint in table.get_constraints() {
        match table_constraint {
            TableConstraint::Unique { name, columns, is_primary } => {
                let index_name = name.clone().unwrap().value.to_string();

                let mut column_name_list = vec![];
                for column_name in columns {
                    column_name_list.push(column_name.clone());
                }

                let mut level = 0;
                if is_primary.clone() {
                    level = 1;
                } else {
                    level = 2;
                }

                let table_index = TableIndexDef {
                    index_name,
                    level,
                    column_name_list,
                };

                all_index.push(table_index);
            }
            _ => {}
        }
    }

    Ok(all_index)
}

pub fn create_sparrow_column(store_id: i32, ordinal_position: i32, sql_column: SQLColumnDef) -> SparrowColumnDef {
    SparrowColumnDef {
        store_id,
        ordinal_position,
        sql_column: sql_column,
    }
}

pub fn create_sql_column(column_name: &str, data_type: SQLDataType, nullable: ColumnOption) -> SQLColumnDef {
    SQLColumnDef {
        name: Ident { value: column_name.to_string(), quote_style: None },
        data_type,
        collation: None,
        options: vec![ColumnOptionDef { name: Some(Ident { value: "nullable".to_string(), quote_style: None }), option: nullable }],
    }
}

pub fn create_schema_name(catalog: &str, db_name: &str, table_name: &str) -> String {
    let data: String = vec![catalog, db_name, table_name].join(".");
    data
}

pub fn create_full_schema_name(catalog_name: &str, schema_name: &str) -> ObjectName {
    let mut idents = vec![];
    let ident = Ident::new(catalog_name);
    idents.push(ident);
    let ident = Ident::new(schema_name);
    idents.push(ident);

    let object_name = ObjectName(idents);
    object_name
}

pub fn create_full_table_name(catalog_name: &str, schema_name: &str, table_name: &str) -> ObjectName {
    let mut idents = vec![];
    let ident = Ident::new(catalog_name);
    idents.push(ident);
    let ident = Ident::new(schema_name);
    idents.push(ident);
    let ident = Ident::new(table_name);
    idents.push(ident);
    let object_name = ObjectName(idents);

    object_name
}

pub fn create_full_column_name(catalog_name: &str, schema_name: &str, table_name: &str, column_name: &str) -> ObjectName {
    let mut idents = vec![];
    let ident = Ident::new(catalog_name);
    idents.push(ident);
    let ident = Ident::new(schema_name);
    idents.push(ident);
    let ident = Ident::new(table_name);
    idents.push(ident);
    let ident = Ident::new(column_name);
    idents.push(ident);

    let object_name = ObjectName(idents);
    object_name
}

pub fn object_name_remove_quote(mut object_name: ObjectName) -> ObjectName {
    let mut idents = vec![];
    for ident in object_name.0 {
        let new_ident = Ident::from(ident.value.as_str());
        idents.push(new_ident.clone());
    }
    ObjectName(idents)
}

pub fn cut_out_catalog_name(mut full_catalog_name: ObjectName) -> ObjectName {
    let mut idents = vec![];
    if let Some(ident) = full_catalog_name.0.get(0) {
        idents.push(ident.clone());
    }
    ObjectName(idents)
}

pub fn cut_out_schema_name(mut full_schema_name: ObjectName) -> ObjectName {
    let mut idents = vec![];
    if let Some(ident) = full_schema_name.0.get(1) {
        idents.push(ident.clone());
    }
    ObjectName(idents)
}

pub fn cut_out_table_name(mut full_table_name: ObjectName) -> ObjectName {
    let mut idents = vec![];
    if let Some(ident) = full_table_name.0.get(2) {
        idents.push(ident.clone());
    }
    ObjectName(idents)
}

pub fn cut_out_column_name(mut full_column_name: ObjectName) -> ObjectName {
    let mut idents = vec![];
    if let Some(ident) = full_column_name.0.get(3) {
        idents.push(ident.clone());
    }
    ObjectName(idents)
}

pub fn fill_up_schema_name(session_context: &mut SessionContext, mut db_name: ObjectName) -> MysqlResult<ObjectName> {
    if db_name.0.len() == 1 {
        let captured_name = session_context.current_catalog.clone();
        let captured_name = captured_name.lock().expect("mutex poisoned");
        if let Some(catalog_name) = captured_name.as_ref() {
            let ident = Ident::new(catalog_name);
            db_name.0.insert(0, ident);
        }
    }

    Ok(db_name)
}

pub fn fill_up_table_name(session_context: &mut SessionContext, mut table_name: ObjectName) -> MysqlResult<ObjectName> {
    if table_name.0.len() == 1 {
        let captured_name = session_context.current_schema.clone();
        let captured_name = captured_name.lock().expect("mutex poisoned");
        if let Some(name) = captured_name.as_ref() {
            let ident = Ident::new(name);
            table_name.0.insert(0, ident);
        }
    }

    if table_name.0.len() == 2 {
        let captured_name = session_context.current_catalog.clone();
        let captured_name = captured_name.lock().expect("mutex poisoned");
        if let Some(catalog_name) = captured_name.as_ref() {
            let ident = Ident::new(catalog_name);
            table_name.0.insert(0, ident);
        }
    }

    Ok(table_name)
}

pub fn fill_up_column_name(session_context: &mut SessionContext, mut original_column_name: ObjectName) -> MysqlResult<ObjectName> {
    let column_name = original_column_name.0.pop().unwrap();
    let table_name = original_column_name;

    let full_table_name = fill_up_table_name(session_context, table_name).unwrap();

    let mut full_column_name = full_table_name;
    full_column_name.0.push(column_name);

    Ok(full_column_name)
}

// pub fn convert_to_table_reference<'a>(full_table_name: &'a ObjectName) -> TableReference<'a> {
//     let catalog = full_table_name.0[0].value.clone();
//     let schema = full_table_name.0[1].value.clone();
//     let table = full_table_name.0[2].value.clone();
//     let tables_reference = TableReference::Full { catalog: catalog.as_str(), schema: schema.as_str(), table: table.as_str() };
//     tables_reference
// }

pub fn convert_to_object_name(schema_name: &str) -> ObjectName {
    let mut object_names: Vec<&str> = schema_name.split(".").collect();

    let mut idents = vec![];
    for object_name in object_names {
        let ident = Ident::new(object_name);
        idents.push(ident);
    }

    let object_name = ObjectName(idents);
    object_name
}

pub fn load_global_variable(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<()> {
    let variable_map = initial_util::read_performance_schema_global_variables(global_context.clone()).unwrap();
    global_context.lock().unwrap().variable.add_variable_map(variable_map);
    Ok(())
}

pub fn load_all_table(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<()> {
    let result = initial_util::read_all_table(global_context.clone());
    match result {
        Ok(table_map) => {
            global_context.lock().unwrap().meta_cache.add_all_table(table_map);
            Ok(())
        }
        Err(mysql_error) => {
            log::error!("load all table error: {}", mysql_error);
            Err(mysql_error)
        }
    }
}

pub async fn init_meta(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<()> {
    let mut init_tables: HashMap<ObjectName, TableDef> = HashMap::new();
    init_tables.insert(meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES.to_object_name(), initial::information_schema::table_tables());
    init_tables.insert(meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS.to_object_name(), initial::information_schema::table_columns());
    init_tables.insert(meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA.to_object_name(), initial::information_schema::table_schemata());
    init_tables.insert(meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS.to_object_name(), initial::information_schema::table_statistics());
    init_tables.insert(meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_KEY_COLUMN_USAGE.to_object_name(), key_column_usage());
    init_tables.insert(meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLE_CONSTRAINTS.to_object_name(), table_constraints());
    init_tables.insert(meta_const::FULL_TABLE_NAME_OF_DEF_MYSQL_USERS.to_object_name(), initial::mysql::users());
    init_tables.insert(meta_const::FULL_TABLE_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES.to_object_name(), initial::performance_schema::global_variables());

    if !meta_has_create() {
        for (full_table_name, table) in init_tables.iter() {
            initial_util::add_information_schema_tables(global_context.clone(), table.option.clone());
            initial_util::add_information_schema_columns(global_context.clone(), table.option.clone(), table.column.sparrow_column_list.clone());
            save_table_constraint(global_context.clone(), table.option.clone(), table.get_constraints().clone());
        }

        initial_util::create_schema(global_context.clone(), meta_const::FULL_SCHEMA_NAME_OF_DEF_MYSQL.to_object_name());
        initial_util::create_schema(global_context.clone(), meta_const::FULL_SCHEMA_NAME_OF_DEF_PERFORMANCE_SCHEMA.to_object_name());

        let result = initial::mysql::users_data(global_context.clone());
        if let Err(e) = result {
            return Err(e);
        }
        let result = initial::performance_schema::global_variables_data(global_context.clone());
        if let Err(e) = result {
            return Err(e);
        }

        meta_create_lock();
    }

    Ok(())
}

pub fn meta_create_lock() {
    let lock_file = format!("{}/LOCK", "/tmp/rocksdb");
    let result = File::create(lock_file.as_str());
}

pub fn meta_has_create() -> bool {
    let lock_file = format!("{}/LOCK", "/tmp/rocksdb");
    let result = File::open(lock_file.as_str());
    match result {
        Ok(_) => {
            true
        }
        _ => {
            false
        }
    }
}

pub fn cache_delete_table(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) {
    global_context.lock().unwrap().meta_cache.delete_table(full_table_name.clone());
}

pub fn store_delete_current_serial_number(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) {
    let store_engine = StoreEngineFactory::try_new_schema_engine(global_context.clone()).unwrap();

    let key = util::dbkey::create_current_serial_number(full_table_name.clone());
    store_engine.delete_key(key);
}

pub fn store_delete_column_serial_number(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName, columns: Vec<Ident>) {
    let store_engine = StoreEngineFactory::try_new_schema_engine(global_context.clone()).unwrap();

    for column_name in columns {
        let key = util::dbkey::create_column_id(full_table_name.clone(), column_name.clone());
        let result = store_engine.delete_key(key);
    }
}

pub fn cache_delete_column_serial_number(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName, columns: Vec<Ident>) {
    for column_name in columns {
        global_context.lock().unwrap().meta_cache.delete_serial_number(full_table_name.clone(), column_name.clone());
    }
}

pub fn get_current_serial_number(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) -> MysqlResult<usize> {
    let store_engine = StoreEngineFactory::try_new_schema_engine(global_context.clone()).unwrap();

    let first = 0 as usize;

    let key = util::dbkey::create_current_serial_number(full_table_name);
    let result = store_engine.get_key(key);
    match result {
        Ok(result) => {
            match result {
                Some(value) => match std::str::from_utf8(&value) {
                    Ok(v) => Ok(v.to_string().parse::<usize>().unwrap()),
                    Err(error) => return Err(MysqlError::new_global_error(meta_const::MYSQL_ERROR_CODE_UNKNOWN_ERROR, format!("did not read valid utf-8 out of the db, error: {:?}", error).as_str())),
                },
                None => Ok(first),
            }
        }
        Err(error) => return Err(MysqlError::new_global_error(meta_const::MYSQL_ERROR_CODE_UNKNOWN_ERROR, format!("rocksdb get error, error: {:?}", error).as_str())),
    }
}

pub fn store_add_column_serial_number(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName, columns: Vec<SQLColumnDef>) -> MysqlResult<()> {
    let store_engine = StoreEngineFactory::try_new_schema_engine(global_context.clone()).unwrap();

    let result = get_current_serial_number(global_context.clone(), full_table_name.clone());
    if let Err(e) = result {
        return Err(e);
    }

    let mut orm_id = 0;
    if let Ok(index) = result {
        orm_id = index;
    };

    for column_def in columns {
        orm_id += 1;

        let column_name = column_def.name;

        let key = util::dbkey::create_column_id(full_table_name.clone(), column_name.clone());
        let value = orm_id.to_string();
        let result = store_engine.put_key(key, value.as_bytes());
    }
    let key = util::dbkey::create_current_serial_number(full_table_name.clone());
    let value = orm_id.to_string();
    store_engine.put_key(key, value.as_bytes());
    Ok(())
}
//
// pub fn cache_add_column_serial_number(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName, columns: Vec<SQLColumnDef>) {
//     for column_def in columns {
//         let column_name = column_def.name;
//
//         let result = store_get_column_id(global_context.clone(), full_table_name.clone(), column_name.clone());
//         let orm_id = match result {
//             Ok(result) => {
//                 match result {
//                     None => {
//                         let message = format!("table column store serial_number not found, table: {}, column: {}", full_table_name, column_name);
//                         log::error!("{}", message);
//                         panic!(message)
//                     }
//                     Some(value) => {
//                         value
//                     }
//                 }
//             }
//             Err(e) => {
//                 log::error!("{}", e);
//                 panic!(e)
//             }
//         };
//         global_context.lock().unwrap().meta_cache.add_serial_number(full_table_name.clone(), column_name.clone(), orm_id);
//     }
// }

pub fn schema_name_not_allow_exist(global_context: Arc<Mutex<GlobalContext>>, session_context: &mut SessionContext, table_name: ObjectName) -> MysqlResult<()> {
    let full_table_name = meta_util::fill_up_table_name(session_context, table_name.clone()).unwrap();

    let map_table_schema = initial_util::read_all_table(global_context.clone()).unwrap();
    if map_table_schema.contains_key(&full_table_name) {
        return Err(MysqlError::new_server_error(
            1007,
            "HY000",
            format!("Table '{}' already exists", table_name).as_str(),
        ));
    }

    Ok(())
}

pub fn mysql_error_unknown_table(global_context: Arc<Mutex<GlobalContext>>, session_context: &mut SessionContext, table_name: ObjectName) -> MysqlResult<()> {
    let full_table_name = meta_util::fill_up_table_name(session_context, table_name.clone()).unwrap();

    let map_table_schema = initial_util::read_all_table(global_context.clone()).unwrap();
    if !map_table_schema.contains_key(&full_table_name) {
        return Err(MysqlError::new_server_error(
            1051,
            "42S02",
            format!("Unknown table '{}'", table_name.to_string()).as_str(),
        ));
    }

    Ok(())
}

pub fn catalog_schema_table_name_must_exist(global_context: Arc<Mutex<GlobalContext>>, session_context: &mut SessionContext, table_name: ObjectName) -> MysqlResult<()> {
    let full_table_name = meta_util::fill_up_table_name(session_context, table_name.clone()).unwrap();

    let map_table_schema = initial_util::read_all_table(global_context.clone()).unwrap();
    if !map_table_schema.contains_key(&full_table_name) {
        return Err(MysqlError::new_server_error(
            1146,
            "42S02",
            format!("Table '{}' doesn't exist", table_name.to_string()).as_str(),
        ));
    }

    Ok(())
}

pub fn text_to_null(is_nullable: &str) -> Result<ColumnOption> {
    if is_nullable.eq("YES") {
        Ok(ColumnOption::Null)
    } else {
        Ok(ColumnOption::NotNull)
    }
}

pub fn text_to_sql_data_type(text: &str) -> Result<SQLDataType> {
    match text {
        meta_const::MYSQL_DATA_TYPE_SMALLINT => Ok(SQLDataType::SmallInt),
        meta_const::MYSQL_DATA_TYPE_INT => Ok(SQLDataType::Int),
        meta_const::MYSQL_DATA_TYPE_BIGINT => Ok(SQLDataType::BigInt),
        meta_const::MYSQL_DATA_TYPE_DECIMAL => {
            let numeric_precision = Some(10);
            let numeric_scale = Some(2);
            Ok(SQLDataType::Decimal(numeric_precision, numeric_scale))
        }
        meta_const::MYSQL_DATA_TYPE_CHAR => {
            let character_maximum_length = Some(255);
            Ok(SQLDataType::Char(character_maximum_length))
        }
        meta_const::MYSQL_DATA_TYPE_VARCHAR => {
            let character_maximum_length = Some(255);
            Ok(SQLDataType::Varchar(character_maximum_length))
        }
        meta_const::MYSQL_DATA_TYPE_ENUM => {
            let character_maximum_length = Some(255);
            Ok(SQLDataType::Varchar(character_maximum_length))
        }
        _ => {
            Err(DataFusionError::Execution(format!(
                "Unsupported data type: {:?}.",
                text
            )))
        }
    }
}

pub fn sql_data_type_to_expr(sql_type: &SQLDataType) -> Result<(Expr, Expr, Expr, Expr, Expr)> {
    match sql_type {
        SQLDataType::SmallInt => Ok((
            Expr::Literal(ScalarValue::Utf8(Some(meta_const::MYSQL_DATA_TYPE_SMALLINT.to_string()))),
            Expr::Literal(ScalarValue::UInt64(None)),
            Expr::Literal(ScalarValue::UInt64(None)),
            Expr::Literal(ScalarValue::UInt64(Some(5))),
            Expr::Literal(ScalarValue::UInt64(Some(0))),
        )),
        SQLDataType::Int => Ok((
            Expr::Literal(ScalarValue::Utf8(Some(meta_const::MYSQL_DATA_TYPE_INT.to_string()))),
            Expr::Literal(ScalarValue::UInt64(None)),
            Expr::Literal(ScalarValue::UInt64(None)),
            Expr::Literal(ScalarValue::UInt64(Some(10))),
            Expr::Literal(ScalarValue::UInt64(Some(0))),
        )),
        SQLDataType::BigInt => Ok((
            Expr::Literal(ScalarValue::Utf8(Some(meta_const::MYSQL_DATA_TYPE_BIGINT.to_string()))),
            Expr::Literal(ScalarValue::UInt64(None)),
            Expr::Literal(ScalarValue::UInt64(None)),
            Expr::Literal(ScalarValue::UInt64(Some(19))),
            Expr::Literal(ScalarValue::UInt64(Some(0))),
        )),
        SQLDataType::Decimal(percision, numeric_scale) => Ok((
            Expr::Literal(ScalarValue::Utf8(Some(meta_const::MYSQL_DATA_TYPE_DECIMAL.to_string()))),
            Expr::Literal(ScalarValue::UInt64(None)),
            Expr::Literal(ScalarValue::UInt64(None)),
            Expr::Literal(ScalarValue::UInt64(percision.clone())),
            Expr::Literal(ScalarValue::UInt64(numeric_scale.clone())),
        )),
        SQLDataType::Char(character_maximum_length) => {
            let character_octet_length = match character_maximum_length.clone() {
                Some(length) => Some(length * 4),
                None => None,
            };

            Ok((
                Expr::Literal(ScalarValue::Utf8(Some(meta_const::MYSQL_DATA_TYPE_CHAR.to_string()))),
                Expr::Literal(ScalarValue::UInt64(character_maximum_length.clone())),
                Expr::Literal(ScalarValue::UInt64(character_octet_length)),
                Expr::Literal(ScalarValue::UInt64(None)),
                Expr::Literal(ScalarValue::UInt64(None)),
            ))
        }
        SQLDataType::Varchar(character_maximum_length) => {
            let character_octet_length = match character_maximum_length.clone() {
                Some(length) => Some(length * 4),
                None => None,
            };

            Ok((
                Expr::Literal(ScalarValue::Utf8(Some(meta_const::MYSQL_DATA_TYPE_CHAR.to_string()))),
                Expr::Literal(ScalarValue::UInt64(character_maximum_length.clone())),
                Expr::Literal(ScalarValue::UInt64(character_octet_length)),
                Expr::Literal(ScalarValue::UInt64(None)),
                Expr::Literal(ScalarValue::UInt64(None)),
            ))
        }
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported data type: {:?}.",
            sql_type
        ))),
    }
}

/// if the scalar value is number, add 0 before the number, until the number lenth is 19
pub fn convert_scalar_value_to_index_string(scalar_value: ScalarValue) -> MysqlResult<Option<String>> {
    match scalar_value {
        // ScalarValue::Binary(limit) => {
        //     if let Some(value) = limit {
        //         Ok(Some(value.as_bytes().to_hex()))
        //     } else {
        //         Ok(None)
        //     }
        // }
        ScalarValue::Int32(limit) => {
            if let Some(value) = limit {
                let new_value = (value as u64) ^ meta_const::SIGN_MASK;
                Ok(Some(new_value.to_string()))
            } else {
                Ok(None)
            }
        }
        ScalarValue::Int64(limit) => {
            if let Some(value) = limit {
                let new_value = (value as u64) ^ meta_const::SIGN_MASK;
                Ok(Some(new_value.to_string()))
            } else {
                Ok(None)
            }
        }
        ScalarValue::Utf8(limit) => {
            if let Some(value) = limit {
                Ok(Some(value.to_string()))
            } else {
                Ok(None)
            }
        }
        _ => Err(MysqlError::new_global_error(
            meta_const::MYSQL_ERROR_CODE_UNKNOWN_ERROR,
            format!("Unsupported convert scalar value to string: {:?}", scalar_value).as_str(),
        )),
    }
}

pub fn convert_sql_data_type_to_text(sql_type: &SQLDataType) -> MysqlResult<String> {
    match sql_type {
        SQLDataType::BigInt => Ok(meta_const::MYSQL_DATA_TYPE_BIGINT.to_string()),
        SQLDataType::Int => Ok(meta_const::MYSQL_DATA_TYPE_INT.to_string()),
        SQLDataType::SmallInt => {
            Ok(meta_const::MYSQL_DATA_TYPE_SMALLINT.to_string())
        }
        SQLDataType::Char(_) => {
            Ok(meta_const::MYSQL_DATA_TYPE_CHAR.to_string())
        }
        SQLDataType::Varchar(_) => {
            Ok(meta_const::MYSQL_DATA_TYPE_VARCHAR.to_string())
        }
        SQLDataType::Text => {
            Ok(meta_const::MYSQL_DATA_TYPE_TEXT.to_string())
        }
        SQLDataType::Custom(_) => {
            Ok(meta_const::MYSQL_DATA_TYPE_ENUM.to_string())
        }
        _ => Err(MysqlError::new_global_error(
            meta_const::MYSQL_ERROR_CODE_UNKNOWN_ERROR,
            format!("Unsupported convert sql data type: {:?} to text.", sql_type).as_str(),
        )),
    }
}

pub fn convert_sql_data_type_to_arrow_data_type(sql_type: &SQLDataType) -> MysqlResult<DataType> {
    match sql_type {
        SQLDataType::BigInt => Ok(DataType::Int64),
        SQLDataType::Int => Ok(DataType::Int32),
        SQLDataType::SmallInt => Ok(DataType::Int16),
        SQLDataType::Char(_) | SQLDataType::Varchar(_) | SQLDataType::Text | SQLDataType::Custom(_) => {
            Ok(DataType::Utf8)
        }
        SQLDataType::Decimal(_, _) => Ok(DataType::Float64),
        SQLDataType::Float(_) => Ok(DataType::Float32),
        SQLDataType::Real | SQLDataType::Double => Ok(DataType::Float64),
        SQLDataType::Boolean => Ok(DataType::Boolean),
        _ => Err(MysqlError::new_global_error(
            meta_const::MYSQL_ERROR_CODE_UNKNOWN_ERROR,
            format!("Unsupported convert sql data type: {:?} to arrow data type.", sql_type).as_str(),
        )),
    }
}

pub fn get_table_column(table: TableDef, column_name: &str) -> Option<SparrowColumnDef> {
    let first = None;

    let column = table.get_columns().to_vec()
        .iter()
        .fold(first, |current, item| {
            if item.sql_column.name.value.eq(column_name) {
                Some(item.clone())
            } else {
                current
            }
        });
    column
}

// pub fn get_table_max_ordinal_position(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) -> MysqlResult<i32> {
//     let first = 0;
//
//     let table = get_table(global_context.clone(), full_table_name).unwrap();
//     let ordinal_position = table.column.sparrow_column_list.to_vec()
//         .iter()
//         .fold(first, |current, b| {
//             let cmp = b.ordinal_position.partial_cmp(&current).unwrap();
//             let max = if let std::cmp::Ordering::Greater = cmp {
//                 b.ordinal_position
//             } else {
//                 current
//             };
//             max
//         });
//     Ok(ordinal_position)
// }

pub fn table_has_primary_key(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) -> bool {
    let schema_table_constraints = initial_util::read_information_schema_statistics(global_context.clone()).unwrap();
    match schema_table_constraints.get(&full_table_name) {
        Some(vec_table_constraint) => {
            let a = &vec_table_constraint.to_vec().iter().any(|table_constraint| {
                match table_constraint {
                    TableConstraint::Unique { is_primary, .. } => {
                        if is_primary.clone() {
                            true
                        } else {
                            false
                        }
                    }
                    _ => false,
                }
            });
            a.clone()
        }
        None => false,
    }
}

pub fn save_table_constraint(global_context: Arc<Mutex<GlobalContext>>, table_option: TableOptionDef, constraints: Vec<TableConstraint>) {
    let catalog_name = table_option.catalog_name;
    let schema_name = table_option.schema_name;
    let table_name = table_option.table_name;

    let mut save_create_statistics = SaveStatistics::new(global_context.clone(), catalog_name.as_str(), schema_name.as_str(), table_name.as_str());
    let mut save_key_column_usage = SaveKeyColumnUsage::new(global_context.clone(), catalog_name.as_str(), schema_name.as_str(), table_name.as_str());
    let mut save_table_constraints = SaveTableConstraints::new(global_context.clone(), catalog_name.as_str(), schema_name.as_str(), table_name.as_str());

    for constraint in constraints {
        match constraint {
            TableConstraint::Unique { name, columns, is_primary } => {
                let constraint_name = if is_primary == true {
                    meta_const::NAME_OF_PRIMARY.to_string()
                } else {
                    match name {
                        Some(n) => {
                            n.to_string()
                        }
                        _ => {
                            create_unique_index_name(columns.clone())
                        }
                    }
                };

                let constraint_type = if is_primary == true {
                    meta_const::CONSTRAINT_TYPE_PRIMARY
                } else {
                    meta_const::CONSTRAINT_TYPE_UNIQUE
                };

                let mut seq_in_index = 0;
                for column_name in columns {
                    seq_in_index += 1;
                    save_create_statistics.add_row(constraint_name.as_str(), seq_in_index, column_name.to_string().as_str());
                    save_key_column_usage.add_row(constraint_name.as_str(), seq_in_index, column_name.to_string().as_str());
                    save_table_constraints.add_row(constraint_name.as_str(), constraint_type);
                }
            }
            _ => continue,
        }
    }

    save_create_statistics.save();
    save_key_column_usage.save();
    save_table_constraints.save();
}

pub fn create_unique_index_name(columns: Vec<Ident>) -> String {
    format!("Unique_{}", columns.iter().map(|column_name| column_name.to_string()).collect::<Vec<_>>().join("_"))
}

pub fn read_all_schema(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<HashMap<ObjectName, SchemaDef>> {
    initial_util::read_information_schema_schemata(global_context.clone())
}

pub fn cache_add_all_table(global_context: Arc<Mutex<GlobalContext>>) {
    let all_table = initial_util::read_all_table(global_context.clone()).unwrap();
    global_context.lock().unwrap().meta_cache.add_all_table(all_table);
}
//
// pub fn read_information_schema_tables_record(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<Vec<RecordBatch>> {
//     let mut reader = RocksdbReader::new(
//         global_context.clone(),
//         initial::information_schema::table_tables(),
//         meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES.to_object_name(),
//         1024,
//         None,
//         &[],
//     );
//
//     let mut partition: Vec<RecordBatch> = vec![];
//
//     loop {
//         match reader.next() {
//             Some(item) => {
//                 match item {
//                     Ok(record_batch) => {
//                         partition.push(record_batch);
//                     }
//                     Err(arrow_error) => return Err(MysqlError::from(arrow_error)),
//                 }
//             }
//             None => break,
//         }
//     }
//
//     Ok(partition)
// }
//
// pub fn store_get_column_id(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName, column_name: Ident) -> MysqlResult<Option<usize>> {
//     let store_engine = StoreEngineFactory::try_new_schema_engine(global_context.clone()).unwrap();
//
//     let key = util::dbkey::create_column_id(full_table_name, column_name.clone());
//     match store_engine.get_key(key) {
//         Ok(result) => {
//             match result {
//                 Some(value) => match std::str::from_utf8(&value) {
//                     Ok(v) => Ok(Some(v.to_string().parse::<usize>().unwrap())),
//                     Err(_) => Err(MysqlError::new_global_error(
//                         meta_const::MYSQL_ERROR_CODE_UNKNOWN_ERROR,
//                         "did not read valid utf-8 out of the db",
//                     )),
//                 },
//                 None => Ok(None),
//             }
//         }
//         Err(_) => Err(MysqlError::new_global_error(
//             meta_const::MYSQL_ERROR_CODE_UNKNOWN_ERROR,
//             "rocksdb get error",
//         )),
//     }
// }

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;

    use crate::meta::meta_util::convert_scalar_value_to_index_string;

    #[test]
    fn check_valid() {
        let a = ScalarValue::Int32(Some(3));

        // let b = 0x00;
        // let a = ScalarValue::Binary(Some(b.encode_fixed_vec()));

        let b = convert_scalar_value_to_index_string(a).unwrap();
        println!("b: {:?}", b);
    }

    #[test]
    fn check_hex() {
        let aa = hex::encode("Hello world!");
        assert_eq!(aa, "48656c6c6f20776f726c6421");

        let aa = hex::encode("");
        assert_eq!(aa, "48656c6c6f20776f726c6421");
    }
}
