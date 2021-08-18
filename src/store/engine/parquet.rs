// use std::collections::HashMap;
// use std::fs;
// use std::path::Path;
// use uuid::Uuid;
//
// use arrow::array::ArrayRef;
// use arrow::csv::reader;
// use arrow::datatypes::SchemaRef;
// use arrow::record_batch::RecordBatch;
// use csv::StringRecord;
// use datafusion::error::{DataFusionError, Result};
// use datafusion::datasource::TableProvider;
// use datafusion::logical_plan::{Expr};
// use parquet::arrow::ArrowWriter;
// use parquet::file::properties::WriterProperties;
//
// use crate::core::context::CoreContext;
// use crate::core::core_util;
// use crate::datafusion_impl::datasource::rocksdb::RocksdbTable;
// use crate::meta::meta_util;
// use crate::util;
//
// use super::engine_util::Engine;
//
//
// pub struct Parquet {
//     core_context: CoreContext,
//     table_name: String,
//     table_schema: meta_util::TableSchema,
// }
//
// impl Parquet {
//     pub fn new(
//         core_context: CoreContext,
//         table_name: &str,
//         table_schema: meta_util::TableSchema,
//     ) -> Self {
//         Self {
//             core_context,
//             table_name: table_name.to_string(),
//             table_schema,
//         }
//     }
// }
//
// impl Engine for Parquet {
//     fn select(&self) -> Box<dyn TableProvider + Send + Sync> {
//         let provider = RocksdbTable::try_new(self.core_context.clone(), self.table_schema.clone(), "/tmp/rocksdb/a", self.table_name.as_str()).unwrap();
//         Box::new(provider)
//     }
//
//     fn insert(&self, column_names: Vec<String>, rows: Vec<Vec<Expr>>) -> Result<usize> {
//         let mut projection = vec![];
//         for column_name in column_names {
//             let column_index = self.core_context.meta_context.get_column_index(self.table_name.as_str(), column_name.as_str()).unwrap();
//             projection.push(column_index);
//         }
//
//         let schema_ref: SchemaRef = self.table_schema.to_dfschema().unwrap().into();
//         let fields = schema_ref.fields();
//
//         let mut string_records: Vec<StringRecord> = vec![];
//         for row in rows {
//             let mut row_string: Vec<String> = vec![];
//             for column in row {
//                 let column_value = core_util::get_real_value(column).unwrap();
//                 row_string.push(column_value);
//             }
//             let string_record = StringRecord::from(row_string);
//             string_records.push(string_record);
//         }
//
//         let record_batch = reader::parse(string_records.as_slice(), fields.as_slice(), &Some(projection.clone()), 0).unwrap();
//
//         let path = "/tmp";
//         let fs_path = Path::new(&path);
//         match fs::create_dir(fs_path) {
//             Ok(()) => {
//                 let filename = format!("part-1.parquet");
//                 let fs_path = Path::new(&path);
//                 let path = fs_path.join(&filename);
//                 let file = fs::File::create(path)?;
//                 let mut writer = ArrowWriter::try_new(
//                     file.try_clone().unwrap(),
//                     schema_ref,
//                     None,
//                 )?;
//                 writer.write(&record_batch);
//                 writer.close().map_err(DataFusionError::from);
//
//                 Ok(())
//             }
//             Err(e) => Err(DataFusionError::Execution(format!(
//                 "Could not create directory {}: {:?}",
//                 path, e
//             ))),
//         }
//
//         Ok(self.rows.len())
//     }
// }
