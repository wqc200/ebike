// use std::sync::Arc;
//
// use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
// use arrow::datatypes::{ToByteSlice};
//
// use datafusion::logical_plan::{Expr, LogicalPlan, ScalarValue};
// use sled::Db;
// use uuid::Uuid;
//
// use crate::core::context::CoreContext;
// use crate::core::util::get_real_value;
// use crate::meta::context::TableContext;
//
// use crate::util;
//
// use super::rocksdb::aggr_test_rocksdb_schema;
//
// pub fn add_rocksdb_test_schema(db_name: String) {
//     let schema = aggr_test_rocksdb_schema();
//     let key = util::dbkey::create_table_key(db_name.clone());
//     let mut sled_operator = SledOperator::new("/tmp/sled");
//     sled_operator.write(Vec::from(key), schema.to_json().to_string().into_bytes());
// }
//
// pub fn sled_path() -> String {
//     let path = "/tmp/sled/";
//     return path.to_string();
// }
//
// pub fn new_sled() -> Db {
//     let config = sled::Config::new().temporary(false).path(sled_path());
//     let sled_db = config.open().unwrap();
//     return sled_db;
// }
//
// pub fn information_schema_tables() -> Arc<Schema> {
//     Arc::new(
//         Schema::new(
//             vec![
//                 Field::new("rowid", DataType::Utf8, false),
//                 Field::new("TABLE_CATALOG", DataType::Utf8, false),
//                 Field::new("TABLE_SCHEMA", DataType::Utf8, false),
//                 Field::new("TABLE_NAME", DataType::Utf8, false),
//             ]
//         )
//     )
// }
//
// pub fn meta_information_schema_columns() -> Arc<Schema> {
//     Arc::new(
//         Schema::new(
//             vec![
//                 Field::new("rowid", DataType::Utf8, false),
//                 Field::new("TABLE_CATALOG", DataType::Utf8, false),
//                 Field::new("TABLE_SCHEMA", DataType::Utf8, false),
//                 Field::new("TABLE_NAME", DataType::Utf8, false),
//                 Field::new("COLUMN_NAME", DataType::Utf8, false),
//                 Field::new("IS_NULLABLE", DataType::Utf8, false),
//                 Field::new("DATA_TYPE", DataType::Utf8, false),
//             ]
//         )
//     )
// }
//
// #[test]
// fn test_add_data() {
//     println!(
//         "RecordBatch has rows and columns",
//     );
//
//     add_rocksdb_test_schema("aggregate_rocksdb".to_string());
// }
//
// #[test]
// fn test_add_data2() {
//     let s = String::from("hello dj");
//     //?????????????????????????????????????????????????????????
//     let ss = "hello dj";
//     //&s[..]???????????????ss
//     let s1 = first_word(&s[..]);
//     println!("s1 is {}", s1);
//
//     let s2 = first_word(ss);
//     println!("s2 is {}", s2);
//
//     let s = String::from("hello world");
//     let hello = &s[0..1];
//     let world = &s[6..11];
//     println!("hello:{},world:{}", hello, world);
// }
//
// //&str???????????? slice ??????
// fn first_word(s: &str) -> &str {
//     //as_bytes ????????? String ?????????????????????
//     let bytes = s.as_bytes().to_vec();
//     println!("ssssss is {}", bytes[0].to_string());
//     //??????enumerate?????????????????????????????????????????????????????????&item???
//     for (i, &item) in bytes.iter().enumerate() {
//         if item == b' ' {
//             return &s[0..i];
//         }
//     }
//     //??????????????????????????????
//     &s[..]
// }
//
// #[test]
// fn test_init_two_sled() {
//     let core_context = CoreContext::new();
//     let tc = TableContext::new(Arc::from(core_context));
//     tc.create_information_schema_columns();
// }