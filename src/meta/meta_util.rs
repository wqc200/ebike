use std::collections::{HashMap};
use std::io::prelude::*;
use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType};
use datafusion::error::{DataFusionError, Result};
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption, ColumnOptionDef, DataType as SQLDataType, Ident, ObjectName, TableConstraint};

use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::meta::{def, initial, meta_const, meta_util};
use crate::meta;
use crate::meta::def::information_schema::{key_column_usage, table_constraints};
use crate::meta::initial::{SaveKeyColumnUsage, SaveStatistics, SaveTableConstraints, get_full_table_name_list};
use crate::meta::meta_def::{SchemaDef, SparrowColumnDef, TableDef, TableIndexDef, TableOptionDef};
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::util::convert::{ToObjectName};

pub fn get_table(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) -> MysqlResult<TableDef> {
    let gc = global_context.lock().unwrap();
    let result = gc.meta_data.get_table(full_table_name.clone());
    match result {
        Some(table) => Ok((table.clone())),
        None => {
            return Err(error_of_table_doesnt_exists(full_table_name.clone()));
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

pub fn resolve_table_name(session_context: &mut SessionContext, table_name: &ObjectName) -> MysqlResult<ObjectName> {
    let full_table_name = meta_util::fill_up_table_name(session_context, table_name.clone()).unwrap();
    if full_table_name.0.len() < 3 {
        return Err(MysqlError::new_server_error(1046, "3D000", "No database selected"));
    }

    Ok(full_table_name)
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
    let variable_map = initial::read_performance_schema_global_variables(global_context.clone()).unwrap();
    global_context.lock().unwrap().variable.add_variable_map(variable_map);
    Ok(())
}

pub fn load_all_table(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<()> {
    let result = initial::read_all_table(global_context.clone());
    match result {
        Ok(table_map) => {
            global_context.lock().unwrap().meta_data.add_all_table(table_map);
            Ok(())
        }
        Err(mysql_error) => {
            log::error!("load all table error: {}", mysql_error);
            Err(mysql_error)
        }
    }
}

pub async fn init_meta(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<()> {
    let mut init_tables = vec![];
    init_tables.push(def::information_schema::tables(global_context.clone()));
    init_tables.push(def::information_schema::columns(global_context.clone()));
    init_tables.push(def::information_schema::schemata(global_context.clone()));
    init_tables.push(def::information_schema::statistics(global_context.clone()));
    init_tables.push(key_column_usage(global_context.clone()));
    init_tables.push(table_constraints(global_context.clone()));
    init_tables.push(def::mysql::users(global_context.clone()));
    init_tables.push(def::performance_schema::global_variables(global_context.clone()));

    let full_table_names = get_full_table_name_list(global_context.clone()).unwrap();
    if full_table_names.len() < 1 {
        for table in init_tables.iter() {
            initial::add_information_schema_tables(global_context.clone(), table.option.clone());
            initial::add_information_schema_columns(global_context.clone(), table.option.clone(), table.column.sparrow_column_list.clone());
            save_table_constraint(global_context.clone(), table.option.clone(), table.get_constraints().clone());
        }

        initial::create_schema(global_context.clone(), meta_const::FULL_SCHEMA_NAME_OF_DEF_MYSQL.to_object_name());
        initial::create_schema(global_context.clone(), meta_const::FULL_SCHEMA_NAME_OF_DEF_PERFORMANCE_SCHEMA.to_object_name());

        let result = meta::initial::add_def_mysql_users(global_context.clone());
        if let Err(e) = result {
            return Err(e);
        }
        let result = meta::initial::add_def_performance_schmea_global_variables(global_context.clone());
        if let Err(e) = result {
            return Err(e);
        }
    }

    Ok(())
}

pub fn schema_name_not_allow_exist(global_context: Arc<Mutex<GlobalContext>>, session_context: &mut SessionContext, table_name: ObjectName) -> MysqlResult<()> {
    let full_table_name = meta_util::fill_up_table_name(session_context, table_name.clone()).unwrap();

    let map_table_schema = initial::read_all_table(global_context.clone()).unwrap();
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

    let map_table_schema = initial::read_all_table(global_context.clone()).unwrap();
    if !map_table_schema.contains_key(&full_table_name) {
        return Err(MysqlError::new_server_error(
            1051,
            "42S02",
            format!("Unknown table '{}'", table_name.to_string()).as_str(),
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
        meta_const::MYSQL_DATA_TYPE_SMALLINT => Ok(SQLDataType::SmallInt(Some(11))),
        meta_const::MYSQL_DATA_TYPE_INT => Ok(SQLDataType::Int(Some(11))),
        meta_const::MYSQL_DATA_TYPE_BIGINT => Ok(SQLDataType::BigInt(Some(11))),
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

/// if the scalar value is number, add 0 before the number, until the number lenth is 19
pub fn convert_scalar_value_to_string(scalar_value: ScalarValue) -> MysqlResult<Option<String>> {
    match scalar_value {
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
        SQLDataType::BigInt(_) => Ok(meta_const::MYSQL_DATA_TYPE_BIGINT.to_string()),
        SQLDataType::Int(_) => Ok(meta_const::MYSQL_DATA_TYPE_INT.to_string()),
        SQLDataType::SmallInt(_) => {
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
        SQLDataType::BigInt(_) => Ok(DataType::Int64),
        SQLDataType::Int(_) => Ok(DataType::Int32),
        SQLDataType::SmallInt(_) => Ok(DataType::Int16),
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

pub fn table_has_primary_key(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) -> bool {
    let schema_table_constraints = initial::read_information_schema_statistics(global_context.clone()).unwrap();
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
    initial::read_information_schema_schemata(global_context.clone())
}

pub fn cache_add_all_table(global_context: Arc<Mutex<GlobalContext>>) {
    let all_table = initial::read_all_table(global_context.clone()).unwrap();
    global_context.lock().unwrap().meta_data.add_all_table(all_table);
}

pub fn check_table_exists_with_full_name(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) -> MysqlResult<()> {
    let gc = global_context.lock().unwrap();

    let table_map = gc.meta_data.get_table_map();
    if table_map.get(&full_table_name).is_none() {
        return Err(error_of_table_doesnt_exists(full_table_name.clone()));
    }

    Ok(())
}

pub fn error_of_table_doesnt_exists(full_table_name: ObjectName) -> MysqlError {
    let message = format!("Table '{}' doesn't exist", full_table_name.to_string());
    log::error!("{}", message);
    MysqlError::new_server_error(
        1146,
        "42S02",
        message.as_str(),
    )
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;

    use crate::meta::meta_util::convert_scalar_value_to_string;

    #[test]
    fn check_valid() {
        let a = ScalarValue::Int32(Some(3));

        // let b = 0x00;
        // let a = ScalarValue::Binary(Some(b.encode_fixed_vec()));

        let b = convert_scalar_value_to_string(a).unwrap();
        println!("b: {:?}", b);
    }

    #[test]
    fn check_hex() {
        let aa = hex::encode("Hello world!");
        assert_eq!(aa, "48656c6c6f20776f726c6421");
    }
}
