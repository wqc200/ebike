use crate::store::engine::engine_util::TableEngineFactory;
use std::sync::{Arc, Mutex};
use crate::core::global_context::GlobalContext;
use crate::meta::initial::information_schema;
use sqlparser::ast::{Assignment, BinaryOperator, ColumnDef as SQLColumnDef, ColumnOption, ColumnOptionDef, DataType as SQLDataType, Expr as SQLExpr, Ident, ObjectName, SqlOption, TableConstraint, Value};
use crate::meta::{meta_const, meta_util};
use std::collections::HashMap;
use arrow::array::{as_primitive_array, as_string_array, Int32Array, StringArray};
use crate::meta::def::{SparrowColumnDef, StatisticsColumn};
use crate::mysql::error::{MysqlError, MysqlResult};
use datafusion::catalog::TableReference;
use datafusion::logical_plan::Expr as DatafusionExpr;
use std::convert::TryFrom;

pub fn get_all_full_table_names(global_context: Arc<Mutex<GlobalContext>>) -> MysqlResult<Vec<ObjectName>> {
    let meta_table = information_schema::tables(global_context.clone());

    let engine = TableEngineFactory::try_new_with_table(global_context.clone(), meta_table).unwrap();
    let mut table_iterator = engine.table_iterator(None, &[]);

    let projection_schema = information_schema::tables(global_context.clone()).to_schema();

    let column_index_of_db_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_SCHEMA).unwrap();
    let column_index_of_table_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_NAME).unwrap();

    let mut table_list: Vec<ObjectName> = vec![];
    loop {
        match table_iterator.next() {
            Some(item) => {
                match item {
                    Ok(record_batch) => {
                        let db_name_row: &StringArray = as_string_array(record_batch.column(column_index_of_db_name));
                        let table_name_row: &StringArray = as_string_array(record_batch.column(column_index_of_table_name));

                        for row_index in 0..record_batch.num_rows() {
                            let db_name = db_name_row.value(row_index).to_string();
                            let table_name = table_name_row.value(row_index).to_string();

                            let full_table_name = meta_util::create_full_table_name(meta_const::CATALOG_NAME, db_name.as_str(), table_name.as_str());
                            table_list.push(full_table_name);
                        }
                    }
                    Err(arrow_error) => return Err(MysqlError::from(arrow_error)),
                }
            }
            None => break,
        }
    }

    Ok(table_list)
}
//
// pub fn get_table_options(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) -> MysqlResult<Vec<SqlOption>> {
//     let table_def = information_schema::table_tables();
//
//     let tables_reference = TableReference::try_from(&full_table_name).unwrap();
//     let resolved_table_reference = tables_reference.resolve(meta_const::CATALOG_NAME, meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA);
//     let catalog_name = resolved_table_reference.catalog.to_string();
//     let schema_name = resolved_table_reference.schema.to_string();
//     let table_name = resolved_table_reference.table.to_string();
//
//     let selection_catalog = SQLExpr::BinaryOp {
//         left: Box::new(SQLExpr::Identifier(Ident::new(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_CATALOG))),
//         op: BinaryOperator::Eq,
//         right: Box::new(SQLExpr::Value(Value::SingleQuotedString(catalog_name.to_string()))),
//     };
//     let selection_schema = SQLExpr::BinaryOp {
//         left: Box::new(SQLExpr::Identifier(Ident::new(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_SCHEMA))),
//         op: BinaryOperator::Eq,
//         right: Box::new(SQLExpr::Value(Value::SingleQuotedString(schema_name.to_string()))),
//     };
//     let selection_table = SQLExpr::BinaryOp {
//         left: Box::new(SQLExpr::Identifier(Ident::new(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_NAME))),
//         op: BinaryOperator::Eq,
//         right: Box::new(SQLExpr::Value(Value::SingleQuotedString(table_name.to_string()))),
//     };
//     let selection = SQLExpr::BinaryOp {
//         left: Box::new(selection_catalog),
//         op: BinaryOperator::And,
//         right: Box::new(selection_schema),
//     };
//     let filter = DatafusionExpr::BinaryExpr {
//         left: Box::new(selection),
//         op: BinaryOperator::And,
//         right: Box::new(selection_table),
//     };
//
//     let engine = TableEngineFactory::try_new_with_table_def(global_context.clone(), table_def).unwrap();
//     let mut table_iterator = engine.table_iterator(None, &[filter]);
//
//     let projection_schema = table_def.to_schema();
//
//     let projection_index_of_table_type = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_TYPE).unwrap();
//     let projection_index_of_engine = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_ENGINE).unwrap();
//
//     let mut table_options: Vec<SqlOption> = vec![];
//     loop {
//         match table_iterator.next() {
//             Some(item) => {
//                 match item {
//                     Ok(record_batch) => {
//                         let table_type_row: &StringArray = as_string_array(record_batch.column(projection_index_of_table_type));
//                         let engine_row: &StringArray = as_string_array(record_batch.column(projection_index_of_engine));
//
//                         for row_index in 0..record_batch.num_rows() {
//                             let table_type = table_type_row.value(row_index).to_string();
//                             let engine = engine_row.value(row_index).to_string();
//
//                             let sql_option = SqlOption { name: Ident { value: meta_const::OPTION_TABLE_TYPE.to_string(), quote_style: None }, value: Value::SingleQuotedString(table_type.clone()) };
//                             table_options.push(sql_option);
//                             let sql_option = SqlOption { name: Ident { value: meta_const::OPTION_ENGINE.to_string(), quote_style: None }, value: Value::SingleQuotedString(engine.clone()) };
//                             table_options.push(sql_option);
//                         }
//                     }
//                     Err(arrow_error) => return Err(MysqlError::from(arrow_error)),
//                 }
//             }
//             None => break,
//         }
//     }
//
//     Ok(table_options)
// }
//
// pub fn get_table_columns(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) -> MysqlResult<Vec<SparrowColumnDef>> {
//     let table_def = information_schema::table_columns();
//
//     let tables_reference = TableReference::try_from(&full_table_name).unwrap();
//     let resolved_table_reference = tables_reference.resolve(meta_const::CATALOG_NAME, meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA);
//     let catalog_name = resolved_table_reference.catalog.to_string();
//     let schema_name = resolved_table_reference.schema.to_string();
//     let table_name = resolved_table_reference.table.to_string();
//
//     let selection_catalog = SQLExpr::BinaryOp {
//         left: Box::new(SQLExpr::Identifier(Ident::new(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_CATALOG))),
//         op: BinaryOperator::Eq,
//         right: Box::new(SQLExpr::Value(Value::SingleQuotedString(catalog_name.to_string()))),
//     };
//     let selection_schema = SQLExpr::BinaryOp {
//         left: Box::new(SQLExpr::Identifier(Ident::new(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_SCHEMA))),
//         op: BinaryOperator::Eq,
//         right: Box::new(SQLExpr::Value(Value::SingleQuotedString(schema_name.to_string()))),
//     };
//     let selection_table = SQLExpr::BinaryOp {
//         left: Box::new(SQLExpr::Identifier(Ident::new(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_NAME))),
//         op: BinaryOperator::Eq,
//         right: Box::new(SQLExpr::Value(Value::SingleQuotedString(table_name.to_string()))),
//     };
//     let selection = SQLExpr::BinaryOp {
//         left: Box::new(selection_catalog),
//         op: BinaryOperator::And,
//         right: Box::new(selection_schema),
//     };
//     let filter = SQLExpr::BinaryOp {
//         left: Box::new(selection),
//         op: BinaryOperator::And,
//         right: Box::new(selection_table),
//     };
//
//     let engine = TableEngineFactory::try_new_with_table_def(global_context.clone(), table_def.clone()).unwrap();
//     let mut table_iterator = engine.table_iterator(None, &[filter]);
//
//     let projection_schema = table_def.to_schema();
//
//     let projection_index_of_column_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_COLUMN_NAME).unwrap();
//     let projection_index_of_store_id = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_STORE_ID).unwrap();
//     let projection_index_of_ordinal_position = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_ORDINAL_POSITION).unwrap();
//     let projection_index_of_is_nullable = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_IS_NULLABLE).unwrap();
//     let projection_index_of_data_type = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_DATA_TYPE).unwrap();
//
//     let mut column_list: Vec<SparrowColumnDef> = vec![];
//     loop {
//         match table_iterator.next() {
//             Some(item) => {
//                 match item {
//                     Ok(record_batch) => {
//                         let column_of_column_name: &StringArray = as_string_array(record_batch.column(projection_index_of_column_name));
//                         let store_id_row: &StringArray = as_string_array(record_batch.column(projection_index_of_store_id));
//                         let column_of_ordinal_position: &Int32Array = as_primitive_array(record_batch.column(projection_index_of_ordinal_position));
//                         let column_of_is_nullable: &StringArray = as_string_array(record_batch.column(projection_index_of_is_nullable));
//                         let column_of_data_type: &StringArray = as_string_array(record_batch.column(projection_index_of_data_type));
//
//                         for row_index in 0..record_batch.num_rows() {
//                             let column_name = column_of_column_name.value(row_index).to_string();
//                             let store_id = store_id_row.value(row_index) as usize;
//                             let ordinal_position = column_of_ordinal_position.value(row_index) as usize;
//                             let is_nullable = column_of_is_nullable.value(row_index).to_string();
//                             let data_type = column_of_data_type.value(row_index).to_string();
//
//                             let sql_data_type = meta_util::text_to_sql_data_type(data_type.as_str()).unwrap();
//                             let nullable = meta_util::text_to_null(is_nullable.as_str()).unwrap();
//
//                             let sql_column = meta_util::create_sql_column(column_name.as_str(), sql_data_type, nullable);
//                             let column = meta_util::create_sparrow_column(store_id, ordinal_position, sql_column);
//
//                             column_list.push(column);
//                         }
//                     }
//                     Err(arrow_error) => return Err(MysqlError::from(arrow_error)),
//                 }
//             }
//             None => break,
//         }
//     }
//
//     column_list.sort_by(|a, b| a.ordinal_position.cmp(&b.ordinal_position));
//
//     Ok(column_list)
// }
//
//
// pub fn get_table_constraints(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName) -> MysqlResult<Vec<TableConstraint>> {
//     let table_def = information_schema::table_statistics();
//
//     let tables_reference = TableReference::try_from(&full_table_name).unwrap();
//     let resolved_table_reference = tables_reference.resolve(meta_const::CATALOG_NAME, meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA);
//     let catalog_name = resolved_table_reference.catalog.to_string();
//     let schema_name = resolved_table_reference.schema.to_string();
//     let table_name = resolved_table_reference.table.to_string();
//
//     let selection_catalog = SQLExpr::BinaryOp {
//         left: Box::new(SQLExpr::Identifier(Ident::new(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_CATALOG))),
//         op: BinaryOperator::Eq,
//         right: Box::new(SQLExpr::Value(Value::SingleQuotedString(catalog_name.to_string()))),
//     };
//     let selection_schema = SQLExpr::BinaryOp {
//         left: Box::new(SQLExpr::Identifier(Ident::new(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_SCHEMA))),
//         op: BinaryOperator::Eq,
//         right: Box::new(SQLExpr::Value(Value::SingleQuotedString(schema_name.to_string()))),
//     };
//     let selection_table = SQLExpr::BinaryOp {
//         left: Box::new(SQLExpr::Identifier(Ident::new(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_TABLE_NAME))),
//         op: BinaryOperator::Eq,
//         right: Box::new(SQLExpr::Value(Value::SingleQuotedString(table_name.to_string()))),
//     };
//     let selection = SQLExpr::BinaryOp {
//         left: Box::new(selection_catalog),
//         op: BinaryOperator::And,
//         right: Box::new(selection_schema),
//     };
//     let filter = SQLExpr::BinaryOp {
//         left: Box::new(selection),
//         op: BinaryOperator::And,
//         right: Box::new(selection_table),
//     };
//
//     let engine = TableEngineFactory::try_new_with_table_def(global_context.clone(), table_def.clone()).unwrap();
//     let mut table_iterator = engine.table_iterator(None);
//
//     let projection_schema = table_def.to_schema();
//
//     let column_index_of_index_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_INDEX_NAME).unwrap();
//     let column_index_of_seq_in_index = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_SEQ_IN_INDEX).unwrap();
//     let column_index_of_column_name = projection_schema.index_of(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS_COLUMN_NAME).unwrap();
//
//     let mut schema_table_index: HashMap<String, Vec<StatisticsColumn>> = HashMap::new();
//     loop {
//         match table_iterator.next() {
//             Some(item) => {
//                 match item {
//                     Ok(record_batch) => {
//                         let column_of_index_name: &StringArray = as_string_array(record_batch.column(column_index_of_index_name));
//                         let column_of_seq_in_index: &Int32Array = as_primitive_array(record_batch.column(column_index_of_seq_in_index));
//                         let column_of_column_name: &StringArray = as_string_array(record_batch.column(column_index_of_column_name));
//
//                         for row_index in 0..record_batch.num_rows() {
//                             let index_name = column_of_index_name.value(row_index).to_string();
//                             let seq_in_index = column_of_seq_in_index.value(row_index) as usize;
//                             let column_name = column_of_column_name.value(row_index).to_string();
//
//                             let sc = StatisticsColumn {
//                                 column_name: column_name.clone(),
//                                 seq_in_index,
//                             };
//
//                             schema_table_index
//                                 .entry(index_name.clone()).or_insert(Vec::new())
//                                 .push(sc);
//                         }
//                     }
//                     Err(arrow_error) => return Err(MysqlError::from(arrow_error)),
//                 }
//             }
//             None => break,
//         }
//     }
//
//     let mut table_constraint_list = vec![];
//     for (index_name, column_field) in schema_table_index.iter() {
//         let mut column_field = column_field.to_vec();
//         column_field.sort_by(|a, b| a.seq_in_index.cmp(&b.seq_in_index));
//
//         let columns = column_field.iter().map(|statistics_column| Ident { value: statistics_column.column_name.clone(), quote_style: None }).collect::<Vec<_>>();
//         let table_constraint = TableConstraint::Unique {
//             name: Some(Ident { value: index_name.to_string(), quote_style: None }),
//             columns,
//             is_primary: true,
//         };
//         table_constraint_list.push(table_constraint);
//     }
//
//     Ok(table_constraint_list)
// }
