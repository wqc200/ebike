// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Common unit test utility methods

use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::sync::{Arc, Mutex};
use std::fs;
use std::process::*;
use std::collections::HashMap;

use tempdir::TempDir;

use arrow::array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::datatypes::{ToByteSlice};
use arrow::record_batch::RecordBatch;
use arrow::array::{
    ArrayData,
    BinaryArray,
    Int8Array,
    Int16Array,
    Int32Array,
    Int64Array,
    UInt8Array,
    UInt16Array,
    UInt32Array,
    UInt64Array,
    Float32Array,
    Float64Array,
    StringArray,
};
use datafusion::error::Result;
use datafusion::datasource::csv::{CsvReadOptions, CsvFile};
use datafusion::datasource::TableProvider;
use uuid::Uuid;

use super::super::store::rocksdb::db::DB;
use super::super::store::rocksdb::option::Options;
use super::super::util;

use crate::physical_plan::create_table::CreateTable;
use crate::core::global_context::GlobalContext;
use futures::io::Error;

pub fn aggr_test_csv_schema() -> Arc<Schema> {
    let mut metadata: HashMap<String, String> = HashMap::new();
    metadata.insert("engine".to_string(), "csv".to_string());

    Arc::new(
        Schema::new_with_metadata(
            vec![
                Field::new("c1", DataType::Utf8, false),
                Field::new("c2", DataType::UInt32, false),
                Field::new("c3", DataType::Int8, false),
                Field::new("c4", DataType::Int16, false),
                Field::new("c5", DataType::Int32, false),
                Field::new("c6", DataType::Int64, false),
                Field::new("c7", DataType::UInt8, false),
                Field::new("c8", DataType::UInt16, false),
                Field::new("c9", DataType::UInt32, false),
                Field::new("c10", DataType::UInt64, false),
                Field::new("c11", DataType::Float32, false),
                Field::new("c12", DataType::Float64, false),
                Field::new("c13", DataType::Utf8, false),
            ],
            metadata
        )
    )
}

pub fn aggr_test_rocksdb_schema() -> Arc<Schema> {
    Arc::new(
        Schema::new(
            vec![
                Field::new("rowid", DataType::Utf8, false),
                Field::new("c1", DataType::Utf8, false),
                Field::new("c2", DataType::UInt32, false),
                Field::new("c3", DataType::Int8, false),
                Field::new("c4", DataType::Int16, false),
                Field::new("c5", DataType::Int32, false),
                Field::new("c6", DataType::Int64, false),
                Field::new("c7", DataType::UInt8, false),
                Field::new("c8", DataType::UInt16, false),
                Field::new("c9", DataType::UInt32, false),
                Field::new("c10", DataType::UInt64, false),
                Field::new("c11", DataType::Float32, false),
                Field::new("c12", DataType::Float64, false),
                Field::new("c13", DataType::Utf8, false),
            ]
        )
    )
}

pub fn rocksdb_path() -> String {
    let path = "/tmp/rocksdb/_rust_test_physical_plan_selection";
    return path.to_string();
}

pub fn new_rocksdb() -> DB {
    let mut opts = Options::default();
    // fn first_three(k: &[u8]) -> &[u8] { &k }
    // let prefix_extractor = SliceTransform::create("first_three", first_three, None);
    opts.create_if_missing(true);
    //opts.set_prefix_extractor(prefix_extractor);
    let db = DB::open(&opts, rocksdb_path().as_str()).unwrap();
    return db;
}

pub fn rocksdb_create_lock() {
    let lock_file = format!("{}/inital", rocksdb_path());
    let result = File::create(lock_file.as_str());
}

pub fn rocksdb_is_inital() -> bool {
    let lock_file = format!("{}/inital", rocksdb_path());
    let result = File::open(lock_file.as_str());
    match result {
        Ok(_) => {
            true
        }
        _ => {
            false
        }
    }
}

// pub fn destroy_rocksdb() {
//     let opts = Options::default();
//     let _ = DB::destroy(&opts, rocksdb_path().as_str());
//`
//     // let output = Command::new("/bin/rm")
//     //     .arg(rocksdb_path().as_str())
//     //     .arg("-rf")
//     //     .output()
//     //     .unwrap_or_else(|e| panic!("wg panic because:{}", e));
//     // println!("output:");
// }

/// Get the value of the ARROW_TEST_DATA environment variable
pub fn csv_testdata_path() -> String {
    //env::var("ARROW_TEST_DATA").expect("ARROW_TEST_DATA not defined");
    "/home/wuqingcheng/Workspace/sparrow/components/arrow/testing/data".to_string()
}

pub fn csv_testdata_file() -> String {
    let testdata = csv_testdata_path();
    let testfile =  format!("{}/csv/aggregate_test_100.csv", testdata);
    return testfile
}

// pub fn add_csv_testdata() {
//     let csv_schema = aggr_test_csv_schema();
//
//     let t = CsvFile::try_new(
//         csv_testdata_file().as_str(),
//         CsvReadOptions::new()
//             .schema(&csv_schema)
//             .has_header(true),
//     ).unwrap();
//
//     let projection = None;
//     let exec = t.scan(&projection, 1024 * 1024, &[]).unwrap();
//
//
//     let mut results: Vec<RecordBatch> = vec![];
//     for partition in 0..exec.output_partitioning().partition_count() {
//         let it = exec.execute(partition).unwrap();
//         let batch = it.lock().expect("mutex lock").next_batch().unwrap().unwrap();
//         results.push(batch);
//     }
//
//     let rocksdb_db = new_rocksdb();
//
//     let db_name = "test";
//     let table_name = "test1";
//
//     let mut numRows = 0;
//
//     // iterate over the results
//     results.iter().for_each(|batch| {
//         println!(
//             "RecordBatch has {} rows and {} columns",
//             batch.num_rows(),
//             batch.num_columns()
//         );
//
//         numRows = numRows + batch.num_rows();
//
//         for row_index in 0..batch.num_rows() {
//             let u = Uuid::new_v4();
//             let uuid = u.to_simple().encode_lower(&mut Uuid::encode_buffer()).to_string();
//
//             let recordPutKey = util::dbkey::create_record_primary(db_name, table_name, vec![uuid.clone()]);
//             println!("recordPutKey pk: {:?}", String::from_utf8_lossy(recordPutKey.to_vec().as_slice()));
//             rocksdb_db.put(recordPutKey.to_vec(), "");
//
//             let record = util::convert::convert(batch, row_index).unwrap();
//             for col_index in 0..batch.num_columns() {
//                 let recordPutKey = util::dbkey::create_record_column("default", "test", (col_index + 1), uuid.as_str());
//                 println!("recordPutKey co: {:?}", String::from_utf8_lossy(recordPutKey.to_vec().as_slice()));
//                 let column_value = record.get(col_index).unwrap().clone().into_bytes();
//                 println!("recordPutKey value: {:?}", String::from_utf8_lossy(column_value.clone().as_slice()));
//                 rocksdb_db.put(recordPutKey.to_vec(), column_value);
//             }
//         }
//     });
// }

pub fn information_schema_columns() -> Arc<Schema> {
    Arc::new(
        Schema::new(
            vec![
                Field::new("TABLE_CATALOG", DataType::Utf8, false),
                Field::new("TABLE_SCHEMA", DataType::Utf8, false),
                Field::new("TABLE_NAME", DataType::Utf8, false),
                Field::new("COLUMN_NAME", DataType::Utf8, false),
                Field::new("IS_NULLABLE", DataType::Utf8, false),
                Field::new("DATA_TYPE", DataType::Utf8, false),
            ]
        )
    )
}

#[test]
fn test_init_two_rocksdb() {
    println!(
        "init two rocksdb",
    );

    new_rocksdb();
    new_rocksdb();
}

// #[test]
// fn test_add_data() {
//     println!(
//         "RecordBatch has rows and columns",
//     );
//
//     let schema_name = "information_schema";
//     let table_name = "columns";
//     let schema_ref = information_schema_columns();
//
//     let mut global_context = Arc::new(Mutex::new(GlobalContext::new()));
//     let pa = CreateTable::new(global_context.clone(), schema_name, table_name, schema_ref);
//     pa.execute(&mut global_context);
// }


