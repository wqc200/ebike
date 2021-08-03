use std::sync::Arc;

use uuid::Uuid;
use arrow::error::{ArrowError, Result};
use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema, DataType, ToByteSlice};
use arrow::record_batch::RecordBatch;
use sled::{Db, Iter};

use super::super::rocksdb::slice_transform::SliceTransform;
use super::super::rocksdb::db::DB;
use super::super::rocksdb::iterator::DBRawIterator;
use super::super::rocksdb::option::{Options, ReadOptions};
use crate::util;
use crate::core::context::CoreContext;

pub struct Reader {
    core_context: CoreContext,
    schema: Arc<Schema>,
    path: String,
    schema_name: String,
    table_name: String,
    projection: Option<Vec<usize>>,
    projected_schema: Arc<Schema>,
    batch_size: usize,
}

impl Reader {
    pub fn new(
        core_context: CoreContext,
        schema: Arc<Schema>,
        path: &str,
        schema_name: &str,
        table_name: &str,
        batch_size: usize,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let projected_schema = match projection.clone() {
            Some(projection) => {
                let fields = schema.fields();
                let projected_fields: Vec<Field> =
                    projection.iter().map(|i| fields[*i].clone()).collect();

                Arc::new(Schema::new(projected_fields))
            }
            None => schema.clone(),
        };

        let dbpath = String::from(path);

        Self {
            core_context,
            schema,
            path: dbpath,
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            projection,
            projected_schema,
            batch_size,
        }
    }

    pub fn schema(&self) -> Arc<Schema> {
        match &self.projection {
            Some(projection) => {
                let fields = self.schema.fields();
                let projected_fields: Vec<Field> =
                    projection.iter().map(|i| fields[*i].clone()).collect();

                Arc::new(Schema::new(projected_fields))
            }
            None => self.schema.clone(),
        }
    }

    pub fn next(&mut self) -> Result<Option<RecordBatch>> {
        let sled_db = self.core_context.get_sled_db();

        let dbkey = util::dbkey::scan_record_rowid(&self.schema_name, &self.table_name);
        println!("scan_record_column: {:?}", String::from_utf8_lossy(dbkey.to_vec().as_slice()));

        let mut iter = sled_db.scan_prefix(dbkey);

        let mut rowid_list: Vec<String> = vec![];
        loop {
            match iter.next() {
                Some(item) => {
                    let kv = item.unwrap();
                    let db_key = kv.0.to_vec();
                    let rowid = util::dbkey::parse_record_rowid(db_key.into_boxed_slice()).unwrap();
                    rowid_list.push(rowid);
                }
                None => {
                    break;
                }
            }
            if rowid_list.len() == self.batch_size {
                break;
            }
        }

        if rowid_list.is_empty() {
            return Ok(None);
        }

        println!("rowid list: {:?}", rowid_list);

        let mut columns: Vec<ArrayRef> = vec![];

        for i in 0..self.projected_schema.clone().fields().len() {
            let field = Arc::from(self.projected_schema.field(i));
            let column_name = field.clone().name();
            let column_index = self.core_context.get_column_index(&self.schema_name, &self.table_name, column_name).unwrap();

            let mut c: Vec<Option<String>> = vec![];

            for rowid in rowid_list.clone() {
                let mut v: Option<String> = None;

                if column_name == "rowid" {
                    v = Some(rowid);
                } else {
                    let db_key = util::dbkey::create_record_column(self.schema_name.as_str(), self.table_name.as_str(), column_index.as_str(), rowid.as_str());
                    let db_value = sled_db.get(db_key.clone());

                    if let Ok(a) = db_value {
                        if let Some(b) = a {
                            if let Ok(c) = std::str::from_utf8(b.to_vec().as_slice()) {
                                v = Some(String::from(c));
                            }
                        }
                    }

                    println!("b db_key: {:?} db_value: {:?}", String::from_utf8_lossy(db_key.clone().to_vec().as_slice()), v);
                }

                c.push(v);
            }

            let arrays = util::convert::build_primitive_array3(field, c);
            match arrays {
                Ok(s) => {
                    columns.push(s);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        let record_batch = RecordBatch::try_new(self.projected_schema.clone(), columns.clone()).unwrap();
        Ok(Option::from(record_batch))
    }
}