use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use sqlparser::ast::{AlterTableOperation, ObjectName, Query, SetExpr, Statement};

use crate::core::core_util;
use crate::core::core_util::{check_table_exists, register_all_table};
use crate::core::global_context::GlobalContext;
use crate::core::output::ResultSet;
use crate::core::session_context::SessionContext;
use crate::execute_impl::select::SelectFrom;
use crate::meta::meta_def::{SparrowColumnDef, TableDef};
use crate::meta::meta_util;
use crate::meta::meta_util::load_all_table;
use crate::meta::{initial, meta_const, scalar_value};
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::util::convert::ToObjectName;

pub struct ShowColumns {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl ShowColumns {
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

    pub async fn execute(&mut self, origin_table_name: &ObjectName) -> MysqlResult<ResultSet> {
        let result = meta_util::resolve_table_name(&mut self.session_context, &origin_table_name);
        let full_table_name = match result {
            Ok(full_table_name) => full_table_name,
            Err(mysql_error) => return Err(mysql_error),
        };

        let result = meta_util::get_table(self.global_context.clone(), full_table_name.clone());
        let table = match result {
            Ok(table) => table.clone(),
            Err(mysql_error) => return Err(mysql_error),
        };

        let catalog_name = table.option.catalog_name.to_string();
        let schema_name = table.option.schema_name.to_string();
        let table_name = table.option.table_name.to_string();

        let columns = self.get_columns(catalog_name.clone(), schema_name.clone(), table_name.clone()).await?;
        let statistics = self
            .get_statistics(
                catalog_name.clone(),
                schema_name.clone(),
                table_name.clone(),
            )
            .await
            .unwrap();

        self.create_result(columns.record_batches, statistics.record_batches)
    }

    async fn get_statistics(
        &self,
        catalog_name: String,
        schema_name: String,
        table_name: String,
    ) -> MysqlResult<ResultSet> {
        let selection = core_util::build_find_table_sqlwhere(
            catalog_name.as_str(),
            schema_name.as_str(),
            table_name.as_str(),
        );

        let select = core_util::build_select_wildcard_sqlselect(
            meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS.to_object_name(),
            Some(selection.clone()),
        );
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
        select_from.execute(&query).await
    }

    async fn get_columns(
        &self,
        catalog_name: String,
        schema_name: String,
        table_name: String,
    ) -> MysqlResult<ResultSet> {
        let selection = core_util::build_find_table_sqlwhere(
            catalog_name.as_str(),
            schema_name.as_str(),
            table_name.as_str(),
        );

        let select = core_util::build_select_wildcard_sqlselect(
            meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS.to_object_name(),
            Some(selection.clone()),
        );
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
        select_from.execute(&query).await
    }

    fn create_result(
        &self,
        columns: Vec<RecordBatch>,
        statistics: Vec<RecordBatch>,
    ) -> MysqlResult<ResultSet> {
        let mut statistics_map: HashMap<String, String> = HashMap::new();

        if statistics.len() > 0 {
            let record_batch = statistics.get(0).unwrap();
            let schema = record_batch.schema();
            let column_index_of_column_name = schema.index_of("COLUMN_NAME").unwrap();
            let column_index_of_index_name = schema.index_of("INDEX_NAME").unwrap();
            let statistics_rows = core_util::convert_record_to_scalar_value(record_batch.clone());
            for row_index in 0..record_batch.num_rows() {
                let row = statistics_rows.get(row_index).unwrap();

                // column name
                let value = row.get(column_index_of_column_name).unwrap();
                let column_name = scalar_value::to_utf8(value.clone()).unwrap();
                // index name
                let value = row.get(column_index_of_index_name).unwrap();
                let index_name = scalar_value::to_utf8(value.clone()).unwrap();

                statistics_map.insert(column_name, index_name);
            }
        }

        let record_batch = columns.get(0).unwrap();
        let schema = record_batch.schema();
        let column_index_of_column_name = schema.index_of("COLUMN_NAME").unwrap();
        let column_index_of_data_type = schema.index_of("DATA_TYPE").unwrap();
        let column_index_of_is_nullable = schema.index_of("IS_NULLABLE").unwrap();
        let columns_rows = core_util::convert_record_to_scalar_value(record_batch.clone());
        let mut column_fields = vec![];
        let mut column_types = vec![];
        let mut column_nulls = vec![];
        let mut column_keys = vec![];
        for row_index in 0..record_batch.num_rows() {
            let row = columns_rows.get(row_index).unwrap();

            // column name
            let value = row.get(column_index_of_column_name).unwrap();
            let column_name = scalar_value::to_utf8(value.clone()).unwrap();
            column_fields.push(column_name.clone());
            // data type
            let value = row.get(column_index_of_data_type).unwrap();
            let data_type = scalar_value::to_utf8(value.clone()).unwrap();
            let sql_data_type = meta_util::create_sql_data_type(data_type.as_str()).unwrap();
            column_types.push(sql_data_type.to_string());
            // nullable
            let value = row.get(column_index_of_is_nullable).unwrap();
            let text_is_nullable = scalar_value::to_utf8(value.clone()).unwrap();
            let nullable = meta_util::text_to_null(text_is_nullable.as_str()).unwrap();
            column_nulls.push(nullable.to_string());
            // key
            if let Some(value) = statistics_map.get(column_name.clone().as_str()) {
                column_keys.push(value.clone());
            } else {
                column_keys.push("".to_string());
            }
        }

        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("Field", DataType::Utf8, false),
            Field::new("Type", DataType::Utf8, false),
            Field::new("Null", DataType::Utf8, false),
            Field::new("Key", DataType::Utf8, false),
        ]));

        let column_fields = column_fields.iter().map(|x| x.as_str()).collect::<Vec<_>>();
        let column_types = column_types.iter().map(|x| x.as_str()).collect::<Vec<_>>();
        let column_nulls = column_nulls.iter().map(|x| x.as_str()).collect::<Vec<_>>();
        let column_keys = column_keys.iter().map(|x| x.as_str()).collect::<Vec<_>>();
        let column_table_name = StringArray::from(column_fields);
        let column_type = StringArray::from(column_types);
        let column_null = StringArray::from(column_nulls);
        let column_keys = StringArray::from(column_keys);
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(column_table_name),
                Arc::new(column_type),
                Arc::new(column_null),
                Arc::new(column_keys),
            ],
        )
        .unwrap();

        Ok(ResultSet::new(schema, vec![record_batch]))
    }
}
