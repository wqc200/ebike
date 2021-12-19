use std::sync::{Arc, Mutex};

use datafusion::execution::context::ExecutionContext;
use sqlparser::ast::{AlterTableOperation, Ident, ObjectName};

use crate::core::core_util;
use crate::core::core_util::register_all_table;
use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::execute_impl::delete::DeleteFrom;
use crate::execute_impl::update_set::UpdateSet;
use crate::meta::meta_def::{SparrowColumnDef, TableDef};
use crate::meta::meta_util;
use crate::meta::meta_util::load_all_table;
use crate::meta::{initial, meta_const};
use crate::mysql::error::{MysqlError, MysqlResult};

pub struct DropTable {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl DropTable {
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

    pub async fn execute(&mut self, table_name: ObjectName) -> MysqlResult<u64> {
        let full_table_name =
            meta_util::fill_up_table_name(&mut self.session_context, table_name.clone()).unwrap();

        let result = meta_util::get_table(self.global_context.clone(), full_table_name.clone());
        let table_def = match result {
            Ok(table) => table.clone(),
            Err(mysql_error) => return Err(mysql_error),
        };

        // delete from information_schema.columns
        let result = self.delete_metadata_column(table_def.clone()).await;
        if let Err(error) = result {
            return Err(error);
        }

        // delete from information_schema.statistics
        let result = self.delete_metadata_statistic(table_def.clone()).await;
        if let Err(error) = result {
            return Err(error);
        }

        // delete from information_schema.tables
        let result = self.delete_metadata_table(table_def.clone()).await;
        if let Err(error) = result {
            return Err(error);
        }

        let mut gc = self.global_context.lock().unwrap();
        gc.meta_data.delete_table(full_table_name.clone());

        let result = self
            .execution_context
            .deregister_table(full_table_name.to_string().as_str());
        if let Err(error) = result {
            return Err(MysqlError::from(error));
        }

        Ok(1)
    }

    async fn delete_metadata_column(&mut self, table_def: TableDef) -> MysqlResult<u64> {
        let metadata_table_name = meta_util::create_full_table_name(
            meta_const::CATALOG_NAME,
            meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
            meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS,
        );
        let selection = core_util::build_find_table_sqlwhere(
            table_def.option.catalog_name.as_ref(),
            table_def.option.schema_name.as_str(),
            table_def.option.table_name.as_str(),
        );
        let mut delete_from = DeleteFrom::new(
            self.global_context.clone(),
            self.session_context.clone(),
            self.execution_context.clone(),
        );
        let result = delete_from
            .execute(metadata_table_name, Some(selection))
            .await;
        result
    }

    async fn delete_metadata_statistic(&mut self, table_def: TableDef) -> MysqlResult<u64> {
        let metadata_table_name = meta_util::create_full_table_name(
            meta_const::CATALOG_NAME,
            meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
            meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS,
        );
        let selection = core_util::build_find_table_sqlwhere(
            table_def.option.catalog_name.as_ref(),
            table_def.option.schema_name.as_str(),
            table_def.option.table_name.as_str(),
        );
        let mut delete_from = DeleteFrom::new(
            self.global_context.clone(),
            self.session_context.clone(),
            self.execution_context.clone(),
        );
        let result = delete_from
            .execute(metadata_table_name, Some(selection))
            .await;
        result
    }

    async fn delete_metadata_table(&mut self, table_def: TableDef) -> MysqlResult<u64> {
        let metadata_table_name = meta_util::create_full_table_name(
            meta_const::CATALOG_NAME,
            meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
            meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES,
        );
        let selection = core_util::build_find_table_sqlwhere(
            table_def.option.catalog_name.as_ref(),
            table_def.option.schema_name.as_str(),
            table_def.option.table_name.as_str(),
        );
        let mut delete_from = DeleteFrom::new(
            self.global_context.clone(),
            self.session_context.clone(),
            self.execution_context.clone(),
        );
        let result = delete_from
            .execute(metadata_table_name, Some(selection))
            .await;
        result
    }
}
