use std::sync::{Mutex, Arc};

use arrow::array::{StringArray};
use arrow::datatypes::{SchemaRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sqlparser::ast::{HiveDistributionStyle, Statement};

use crate::core::global_context::GlobalContext;
use crate::meta::{meta_util, scalar_value, def, meta_const};
use crate::mysql::error::{MysqlResult};

use crate::core::core_util;
use crate::meta::meta_def::TableDef;

pub struct ShowCreateTable {
    global_context: Arc<Mutex<GlobalContext>>,
    table: TableDef,
}

impl ShowCreateTable {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table: TableDef,
    ) -> Self {
        Self {
            global_context,
            table,
        }
    }

    pub fn execute(&self, columns_record: Vec<RecordBatch>, statistics_record: Vec<RecordBatch>, tables_record: Vec<RecordBatch>) -> MysqlResult<(SchemaRef, Vec<RecordBatch>)> {
        let table_name = self.table.option.table_name.clone();
        let table_constraints = self.table.constraints.clone();

        let schema_of_columns = def::information_schema::columns(self.global_context.clone()).to_schema_ref();

        let record_batch = columns_record.get(0).unwrap();
        let column_index_of_column_name = schema_of_columns.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_COLUMN_NAME).unwrap();
        let column_index_of_data_type = schema_of_columns.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_DATA_TYPE).unwrap();
        let column_index_of_is_nullable = schema_of_columns.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_IS_NULLABLE).unwrap();
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
            let sql_column = meta_util::create_sql_column(column_name.as_str(), sql_data_type, nullable);

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
        let record_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(column_table_name), Arc::new(column_create_table)]).unwrap();

        Ok((schema.clone(), vec![record_batch]))
    }
}
