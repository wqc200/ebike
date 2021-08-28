use std::sync::{Arc, Mutex};

use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use sqlparser::ast::ObjectName;
use uuid::Uuid;

use crate::core::core_util;
use crate::core::global_context::GlobalContext;
use crate::core::logical_plan::CoreLogicalPlan;
use crate::core::output::CoreOutput;
use crate::meta::meta_util;
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::physical_plan::alter_table::AlterTable;
use crate::physical_plan::com_field_list::ComFieldList;
use crate::physical_plan::create_db::CreateDb;
use crate::physical_plan::create_table::CreateTable;
use crate::physical_plan::delete::Delete;
use crate::physical_plan::drop_db::DropDB;
use crate::physical_plan::drop_table::DropTable;
use crate::physical_plan::insert::PhysicalPlanInsert;
use crate::physical_plan::select::Select;
use crate::physical_plan::set_default_schema::SetDefaultSchema;
use crate::physical_plan::set_variable::SetVariable;
use crate::physical_plan::show_charset::ShowCharset;
use crate::physical_plan::show_collation::ShowCollation;
use crate::physical_plan::show_columns_from::ShowColumnsFrom;
use crate::physical_plan::show_create_table::ShowCreateTable;
use crate::physical_plan::show_engines::ShowEngines;
use crate::physical_plan::show_grants::ShowGrants;
use crate::physical_plan::show_privileges::ShowPrivileges;
use crate::physical_plan::update::Update;
use crate::store::engine::engine_util;
use crate::store::engine::engine_util::TableEngine;
use crate::util;
use crate::util::convert::ToIdent;

pub enum CorePhysicalPlan {
    AlterTableAddColumn(AlterTable),
    AlterTableDropColumn(AlterTable, Delete, Update),
    CreateDb(CreateDb),
    CreateTable(CreateTable),
    SetDefaultSchema(SetDefaultSchema),
    DropTable(DropTable, Delete, Delete, Delete),
    DropDB(DropDB),
    Delete(Delete),
    Insert(PhysicalPlanInsert),
    Select(Select),
    Update(Update),
    SetVariable(SetVariable),
    ShowCreateTable(ShowCreateTable, Select, Select, Select),
    ShowColumnsFrom(ShowColumnsFrom, Select, Select),
    ShowGrants(ShowGrants),
    ShowPrivileges(ShowPrivileges),
    ShowEngines(ShowEngines),
    ShowCharset(ShowCharset),
    ShowCollation(ShowCollation),
    ComFieldList(ComFieldList),
    Commit,
}

pub fn add_rows(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName, add_entry_type: engine_util::ADD_ENTRY_TYPE, column_names: Vec<String>, rows: Vec<Vec<ScalarValue>>) -> MysqlResult<u64> {
    let table_def = global_context.lock().unwrap().meta_cache.get_table(full_table_name.clone()).unwrap().clone();

    let result = engine_util::EngineFactory::try_new_with_table(global_context.clone(), full_table_name.clone(), table_def.clone());
    let engine = match result {
        Ok(engine) => engine,
        Err(mysql_error) => return Err(mysql_error),
    };

    let table_name = meta_util::cut_out_table_name(full_table_name.clone()).to_string();

    let all_table_index = meta_util::get_all_table_index(global_context.clone(), full_table_name.clone()).unwrap();

    let serial_number_map = global_context.lock().unwrap().meta_cache.get_serial_number_map(full_table_name.clone()).unwrap();

    let mut row_index_keys = vec![];
    for row_index in 0..rows.len() {
        let row = rows[row_index].clone();

        let mut index_keys = vec![];
        for (index_name, level, column_name_vec) in all_table_index.clone() {
            let column_name_value_map = engine_util::build_column_name_value(column_name_vec.clone(), row.clone()).unwrap();
            let serial_number_value_vec = engine_util::build_column_serial_number_value(column_name_vec.clone(), serial_number_map.clone(), column_name_value_map).unwrap();
            let result = util::dbkey::create_index(full_table_name.clone(), index_name.as_str(), serial_number_value_vec);
            let index_key = match result {
                Ok(index_key) => index_key,
                Err(mysql_error) => return Err(mysql_error)
            };
            index_keys.push((index_name.clone(), level, index_key.clone()));
        }

        row_index_keys.push(index_keys);
    }

    for index_keys in row_index_keys.clone() {
        for (index_name, level, index_key) in index_keys {
            if level == 1 || level == 2 {
                match engine.get_key(index_key.clone()).unwrap() {
                    None => {}
                    Some(_) => {
                        if add_entry_type == engine_util::ADD_ENTRY_TYPE::INSERT {
                            return Err(MysqlError::new_server_error(
                                1062,
                                "23000",
                                format!(
                                    "Duplicate entry '{:?}' for key '{:?}.{:?}'",
                                    index_key,
                                    table_name,
                                    index_name,
                                ).as_str(),
                            ));
                        }
                    }
                }
            }
        }
    }

    for row_index in 0..rows.len() {
        let rowid = Uuid::new_v4().to_simple().encode_lower(&mut Uuid::encode_buffer()).to_string();
        let index_keys = row_index_keys[row_index].clone();

        let rowid_key = util::dbkey::create_column_rowid_key(full_table_name.clone(), rowid.as_str());
        log::debug!("rowid_key: {:?}", String::from_utf8_lossy(rowid_key.to_vec().as_slice()));
        engine.put_key(rowid_key, rowid.as_bytes());

        if index_keys.len() > 0 {
            for (index_name, level, index_key) in index_keys {
                engine.put_key(index_key, rowid.as_bytes());
            }
        }

        for index in 0..column_names.to_vec().len() {
            let column_name = column_names[index].to_ident();
            let column_orm_id = global_context.lock().unwrap().meta_cache.get_serial_number(full_table_name.clone(), column_name.clone()).unwrap();
            let column_key = util::dbkey::create_column_key(full_table_name.clone(), column_orm_id, rowid.as_str());
            log::debug!("column_key: {:?}", String::from_utf8_lossy(column_key.to_vec().as_slice()));
            let column_value = core_util::convert_scalar_value(rows[row_index][index].clone()).unwrap();
            log::debug!("column_value: {:?}", column_value);
            if let Some(value) = column_value {
                engine.put_key(column_key, value.as_bytes());
            }
        }
    }

    Ok(rows.len() as u64)
}

