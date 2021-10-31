use std::sync::{Arc, Mutex};

use datafusion::execution::context::ExecutionContext;
use sqlparser::ast::{AlterTableOperation, ObjectName, Ident};

use crate::core::core_util::register_all_table;
use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::meta::{initial, meta_const};
use crate::meta::meta_def::{SparrowColumnDef, TableDef};
use crate::meta::meta_util;
use crate::meta::meta_util::load_all_table;
use crate::mysql::error::{MysqlResult};
use crate::core::core_util;
use crate::execute_impl::delete_from::DeleteFrom;
use crate::execute_impl::update_set::UpdateSet;

pub struct DropColumn {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl DropColumn {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        session_context: SessionContext,
        execution_context: ExecutionContext,
    ) -> Self {
        Self {
            global_context,
            session_context,
            execution_context,
        }
    }

    pub async fn execute(&mut self, table_name: ObjectName, column_name: Ident) -> MysqlResult<u64> {
        let full_table_name =
            meta_util::fill_up_table_name(&mut self.session_context, table_name.clone()).unwrap();

        let table_map = self
            .global_context
            .lock()
            .unwrap()
            .meta_data
            .get_table_map();
        let table_def = match table_map.get(&full_table_name) {
            None => {
                return Err(meta_util::error_of_table_doesnt_exists(
                    full_table_name.clone(),
                ))
            }
            Some(table_def) => table_def.clone(),
        };

        // delete column from information_schema.columns
        let result = self.update_metadata(table_def.clone(), column_name.clone()).await;
        if let Err(error) = result {
            return Err(error);
        }

        // update ordinal_position from information_schema.columns
        let result = self.delete_metadata(table_def.clone(), column_name.clone()).await;
        if let Err(error) = result {
            return Err(error);
        }

        meta_util::cache_add_all_table(self.global_context.clone());

        let result = load_all_table(self.global_context.clone());
        if let Err(error) = result {
            return Err(error);
        }

        let result = register_all_table(self.global_context.clone(), &mut self.execution_context.clone());
        if let Err(mysql_error) = result {
            return Err(mysql_error);
        }

        Ok(1)
    }

    async fn update_metadata(&mut self, table_def: TableDef, column_name: Ident) -> MysqlResult<u64> {
        let sparrow_column = table_def
            .column
            .get_sparrow_column(column_name.clone())
            .unwrap();

        let metadata_table_name = meta_util::create_full_table_name(
            meta_const::CATALOG_NAME,
            meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
            meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS,
        );
        let ordinal_position = sparrow_column.ordinal_position;
        let assignments = core_util::build_update_column_assignments();
        let selection = core_util::build_find_column_ordinal_position_sqlwhere(
            table_def.option.catalog_name.as_ref(),
            table_def.option.schema_name.as_str(),
            table_def.option.table_name.as_str(),
            ordinal_position,
        );
        let mut update_set = UpdateSet::new(
            self.global_context.clone(),
            self.session_context.clone(),
            self.execution_context.clone(),
        );
        let result = update_set.execute(metadata_table_name, assignments, Some(selection)).await;
        result
    }

    async fn delete_metadata(&mut self, table_def: TableDef, column_name: Ident) -> MysqlResult<u64> {
        let metadata_table_name = meta_util::create_full_table_name(
            meta_const::CATALOG_NAME,
            meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
            meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS,
        );
        let selection = core_util::build_find_column_sqlwhere(
            table_def.option.catalog_name.as_ref(),
            table_def.option.schema_name.as_str(),
            table_def.option.table_name.as_str(),
            column_name.to_string().as_str(),
        );
        let mut delete_from = DeleteFrom::new(
            self.global_context.clone(),
            self.session_context.clone(),
            self.execution_context.clone(),
        );
        let result = delete_from.execute(metadata_table_name, Some(selection)).await;
        result
    }
}
