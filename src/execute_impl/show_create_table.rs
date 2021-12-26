use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use sqlparser::ast::{
    AlterTableOperation, Expr as SQLExpr, HiveDistributionStyle, Ident, ObjectName, OrderByExpr,
    Query, SetExpr, Statement,
};

use crate::core::core_util;
use crate::core::core_util::{check_table_exists, register_all_table};
use crate::core::global_context::GlobalContext;
use crate::core::output::ResultSet;
use crate::core::session_context::SessionContext;
use crate::execute_impl::select::SelectFrom;
use crate::meta::meta_def::{SparrowColumnDef, TableDef};
use crate::meta::meta_util::load_all_table;
use crate::meta::{def, meta_util};
use crate::meta::{initial, meta_const, scalar_value};
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::util::convert::ToObjectName;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::collections::HashMap;

pub struct ShowCreateTable {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
}

impl ShowCreateTable {
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

    pub async fn execute(&mut self, table_name: &ObjectName) -> MysqlResult<ResultSet> {
        let full_table_name =
            meta_util::fill_up_table_name(&mut self.session_context, table_name.clone()).unwrap();

        let result = meta_util::get_table(self.global_context.clone(), full_table_name.clone());
        let table_def = match result {
            Ok(table) => table.clone(),
            Err(mysql_error) => return Err(mysql_error),
        };

        let catalog_name = table_def.option.catalog_name.to_string();
        let schema_name = table_def.option.schema_name.to_string();
        let table_name = table_def.option.table_name.to_string();

        let columns = self.get_columns(catalog_name.clone(), schema_name.clone(), table_name.clone()).await?;
        let statistics = self.get_statistics(
            catalog_name.clone(),
            schema_name.clone(),
            table_name.clone(),
        ).await?;
        let tables = self.get_tables(
            catalog_name.clone(),
            schema_name.clone(),
            table_name.clone(),
        ).await?;

        self.create_result(
            table_def,
            columns.record_batches,
            statistics.record_batches,
            tables.record_batches,
        )
    }

    async fn get_tables(
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
            meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES.to_object_name(),
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
        // order by
        let order_by = OrderByExpr {
            expr: SQLExpr::Identifier(Ident::new(
                meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_ORDINAL_POSITION,
            )),
            asc: None,
            nulls_first: None,
        };
        let query = Box::new(Query {
            with: None,
            body: SetExpr::Select(Box::new(select)),
            order_by: vec![order_by],
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
        table_def: TableDef,
        columns: Vec<RecordBatch>,
        statistics: Vec<RecordBatch>,
        tables: Vec<RecordBatch>,
    ) -> MysqlResult<ResultSet> {
        let table_name = table_def.option.table_name.clone();
        let table_constraints = table_def.constraints.clone();

        let schema_of_columns =
            def::information_schema::columns(self.global_context.clone()).to_schema_ref();

        let record_batch = columns.get(0).unwrap();
        let column_index_of_column_name = schema_of_columns
            .index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_COLUMN_NAME)
            .unwrap();
        let column_index_of_data_type = schema_of_columns
            .index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_DATA_TYPE)
            .unwrap();
        let column_index_of_is_nullable = schema_of_columns
            .index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_IS_NULLABLE)
            .unwrap();
        let columns_rows = core_util::convert_record_to_scalar_value(record_batch.clone());
        let mut columns = vec![];
        for row_index in 0..record_batch.num_rows() {
            let row = columns_rows.get(row_index).unwrap();

            // column name
            let value = row.get(column_index_of_column_name).unwrap();
            let column_name = scalar_value::to_utf8(value.clone()).unwrap();
            // data type
            let value = row.get(column_index_of_data_type).unwrap();
            let data_type = scalar_value::to_utf8(value.clone()).unwrap();
            let sql_data_type = meta_util::create_sql_data_type(data_type.as_str()).unwrap();
            // nullable
            let value = row.get(column_index_of_is_nullable).unwrap();
            let text_is_nullable = scalar_value::to_utf8(value.clone()).unwrap();
            let nullable = meta_util::text_to_null(text_is_nullable.as_str()).unwrap();
            // create sql column
            let sql_column =
                meta_util::create_sql_column(column_name.as_str(), sql_data_type, nullable);

            columns.push(sql_column);
        }

        let object_name = meta_util::convert_to_object_name(table_name.as_str());
        let create_table = Statement::CreateTable {
            or_replace: false,
            temporary: false,
            external: false,
            if_not_exists: false,
            name: object_name,
            columns,
            constraints: table_constraints,
            hive_distribution: HiveDistributionStyle::NONE,
            hive_formats: None,
            table_properties: vec![],
            with_options: vec![],
            file_format: None,
            location: None,
            query: None,
            without_rowid: false,
            like: None,
            table_options: vec![],
        };

        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("Table", DataType::Utf8, false),
            Field::new("Create Table", DataType::Utf8, false),
        ]));
        let column_table_name = StringArray::from(vec![table_name.as_str()]);
        let column_create_table = StringArray::from(vec![create_table.to_string().as_str()]);
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(column_table_name), Arc::new(column_create_table)],
        )
        .unwrap();

        Ok(ResultSet::new(schema, vec![record_batch]))
    }
}
