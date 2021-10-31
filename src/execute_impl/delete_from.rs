use std::sync::{Arc, Mutex};

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use datafusion::physical_plan::{collect, ExecutionPlan};

use crate::core::core_util;
use crate::core::global_context::GlobalContext;
use crate::core::session_context::SessionContext;
use crate::execute_impl::select_from::SelectFrom;
use crate::meta::meta_def::TableDef;
use crate::meta::{meta_const, meta_util};
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::store::engine::engine_util::StoreEngineFactory;
use crate::util;
use sqlparser::ast::{ObjectName, Query, SetExpr, Expr as SQLExpr};

pub struct DeleteFrom {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl DeleteFrom {
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

    pub async fn execute(
        &mut self,
        table_name: ObjectName,
        selection: Option<SQLExpr>,
    ) -> MysqlResult<u64> {
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
                let message = format!("Table '{}' doesn't exist", table_name.to_string());
                log::error!("{}", message);
                return Err(MysqlError::new_server_error(
                    1146,
                    "42S02",
                    message.as_str(),
                ));
            }
            Some(table) => table.clone(),
        };

        let select = core_util::build_select_rowid_sqlselect(full_table_name.clone(), selection);
        let query = Box::new(Query {
            with: None,
            body: SetExpr::Select(Box::new(select)),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        });
        let mut select_from = SelectFrom::new(
            self.global_context.clone(),
            self.session_context.clone(),
            self.execution_context.clone(),
        );
        let result = select_from.execute(&query).await;
        return match result {
            Ok(result_set) => {
                let result = self.delete_record_batches(table_def, result_set.record_batches);
                result
            }
            Err(mysql_error) => Err(mysql_error),
        };
    }

    fn delete_record_batches(&self, table_def: TableDef, record_batches: Vec<RecordBatch>) -> MysqlResult<u64> {
        let mut total = 0;
        for record_batch in record_batches {
            let result = self.delete_record_batch(table_def.clone(), record_batch);
            match result {
                Ok(count) => total += count,
                Err(mysql_error) => return Err(mysql_error),
            }
        }
        Ok(total)
    }

    fn delete_record_batch(&self, table_def: TableDef, record_batch: RecordBatch) -> MysqlResult<u64> {
        let store_engine =
            StoreEngineFactory::try_new_with_table(self.global_context.clone(), table_def.clone())
                .unwrap();

        let rowid_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for row_index in 0..rowid_array.len() {
            let rowid = rowid_array.value(row_index);

            let record_rowid_key = util::dbkey::create_record_rowid(
                table_def.option.full_table_name.clone(),
                rowid.as_ref(),
            );
            log::debug!("record_rowid_key: {:?}", record_rowid_key);
            let result = store_engine.delete_key(record_rowid_key);
            if let Err(e) = result {
                return Err(e);
            }

            for sql_column in table_def.get_table_column().sql_column_list {
                let column_name = sql_column.name;
                if column_name.to_string().contains(meta_const::COLUMN_ROWID) {
                    continue;
                }

                let sparrow_column = table_def
                    .get_table_column()
                    .get_sparrow_column(column_name)
                    .unwrap();
                let store_id = sparrow_column.store_id;

                let record_column_key = util::dbkey::create_column_key(
                    table_def.option.full_table_name.clone(),
                    store_id,
                    rowid.as_ref(),
                );
                let result = store_engine.delete_key(record_column_key.clone());
                match result {
                    Err(error) => {
                        return Err(MysqlError::new_global_error(1105, format!(
                            "Unknown error. An error occurred while deleting the key, key: {:?}, error: {:?}",
                            record_column_key.clone(),
                            error,
                        ).as_str()));
                    }
                    _ => {}
                }
            }
        }

        Ok(rowid_array.len() as u64)
    }
}
