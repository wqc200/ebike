use std::sync::{Arc, Mutex};

use datafusion::execution::context::ExecutionContext;
use sqlparser::ast::{AlterTableOperation, Ident, ObjectName};

use crate::core::core_util;
use crate::core::core_util::register_all_table;
use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::execute_impl::delete::DeleteFrom;
use crate::execute_impl::update::Update;
use crate::meta::meta_def::{SparrowColumnDef, TableDef};
use crate::meta::meta_util;
use crate::meta::meta_util::load_all_table;
use crate::meta::{initial, meta_const};
use crate::mysql::error::{MysqlError, MysqlResult};

pub struct DropSchema {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl DropSchema {
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

    pub async fn execute(&mut self, schema_name: ObjectName) -> MysqlResult<u64> {
        let full_schema_name =
            meta_util::fill_up_table_name(&mut self.session_context, schema_name.clone()).unwrap();

        let schema_map = meta_util::read_all_schema(self.global_context.clone()).unwrap();
        if !schema_map.contains_key(&full_schema_name) {
            return Err(MysqlError::new_global_error(
                1008,
                format!(
                    "Can't drop database '{}'; database doesn't exist",
                    original_schema_name
                )
                .as_str(),
            ));
        }

        let result = meta_util::get_schema(self.global_context.clone(), full_table_name.clone());
        let table_def = match result {
            Ok(table) => table.clone(),
            Err(mysql_error) => return Err(mysql_error),
        };

        // delete from information_schema.schemata
        let result = self.delete_from_information_schema_schemata(table_def.clone()).await;
        if let Err(error) = result {
            return Err(error);
        }

        let mut gc = self.global_context.lock().unwrap();
        gc.meta_data.delete_schema(full_schema_name.clone());

        Ok(1)
    }

    async fn delete_from_information_schema_schemata(&mut self, table_def: TableDef) -> MysqlResult<u64> {
        let metadata_table_name = meta_util::create_full_table_name(
            meta_const::CATALOG_NAME,
            meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
            meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA,
        );
        let selection = core_util::selection_information_schema_schemata(
            table_def.option.catalog_name.as_ref(),
            table_def.option.schema_name.as_str(),
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
