use bstr::ByteSlice;
use std::sync::{Arc, Mutex};

use arrow::error::{ArrowError, Result};
use arrow::array::ArrayRef;
use arrow::array::StructBuilder;
use arrow::array::{Float32Builder, Int32Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{Field, Schema, DataType, ToByteSlice, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::logical_plan::Expr;
use uuid::Uuid;
use sled::{Db as SledDb, Iter, IVec, Error};
use sled::Iter as SledIter;

use crate::core::global_context::GlobalContext;
use crate::meta::{meta_util as MetaUtil, meta_const, meta_util};
use crate::store::reader::reader_util;
use crate::store::rocksdb::db::DB;
use crate::store::rocksdb::iterator::DBRawIterator;
use crate::store::rocksdb::option::{Options, ReadOptions};
use crate::store::rocksdb::slice_transform::SliceTransform;
use crate::util;
use crate::util::dbkey;
use crate::store::reader::reader_util::{SeekType, ScanOrder, PointType};
use std::cmp::Ordering;
use crate::util::dbkey::CreateScanKey;
use sqlparser::ast::ObjectName;
use crate::mysql::error::MysqlError;
use crate::util::convert::{ToObjectName, ToIdent};
use crate::meta::meta_def::TableDef;

pub struct Seek {
    iter: SledIter,
    start: CreateScanKey,
    end: CreateScanKey,
}

pub struct SledReader {
    global_context: Arc<Mutex<GlobalContext>>,
    table: TableDef,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    batch_size: usize,
    seek: Seek,
}

impl SledReader {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table: TableDef,
        batch_size: usize,
        projection: Option<Vec<usize>>,
        filters: &[Expr],
    ) -> Self {
        let schema_ref = table.to_schema_ref();
        let full_table_name = table.option.full_table_name.clone();

        let projected_schema = match projection.clone() {
            Some(projection) => {
                let fields = schema_ref.fields();
                let projected_fields: Vec<Field> =
                    projection.iter().map(|i| fields[*i].clone()).collect();

                Arc::new(Schema::new(projected_fields))
            }
            None => schema_ref.clone(),
        };

        let table_index_prefix = reader_util::get_seek_prefix(global_context.clone(), full_table_name.clone(), table.clone(), filters.clone()).unwrap();
        let seek = match table_index_prefix {
            SeekType::FullTableScan { start, end} => {
                let iter = global_context.lock().unwrap().engine.sled_db.as_ref().unwrap().scan_prefix(start.key.clone());
                Seek {
                    iter,
                    start,
                    end,
                }
            }
            SeekType::UsingTheIndex { index_name, order, start, end} => {
                let iter = global_context.lock().unwrap().engine.sled_db.as_ref().unwrap().scan_prefix(start.key.clone());
                Seek {
                    iter,
                    start,
                    end,
                }
            }
        };

        Self {
            global_context,
            table,
            projection,
            projected_schema,
            batch_size,
            seek,
        }
    }

    pub fn projected_schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }
}

impl Iterator for SledReader {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let global_context = &self.global_context.lock().unwrap();
        let sled_db = global_context.engine.sled_db.as_ref().unwrap();
        let table_column = self.table.get_table_column();

        let mut rowids: Vec<String> = vec![];
        loop {
            let result = self.seek.iter.next();
            let (key, value) = match result {
                Some(item) => {
                    match item {
                        Ok((key, value)) => {
                            (key, value)
                        }
                        Err(error) => {
                            return Some(Err(ArrowError::IoError(format!(
                                "Error iter from sled: '{:?}'",
                                error
                            ))));
                        }
                    }
                }
                _ => break,
            };

            let key = String::from_utf8(key.to_vec()).expect("Found invalid UTF-8");

            match self.seek.start.point_type() {
                PointType::Open => {
                    if key.starts_with(self.seek.start.key().as_str()) {
                        continue;
                    }
                }
                PointType::Closed => {}
            }
            match self.seek.end.point_type() {
                PointType::Open => {
                    if key.starts_with(self.seek.end.key().as_str()) {
                        break;
                    }
                }
                PointType::Closed => {}
            }
            if !key.starts_with(self.seek.end.key().as_str()) {
                match key.as_str().partial_cmp(self.seek.end.key().as_str()) {
                    None => break,
                    Some(a) => {
                        match a {
                            Ordering::Less => {}
                            Ordering::Equal => {}
                            Ordering::Greater => break,
                        }
                    }
                }
            }

            let value = String::from_utf8(value.to_vec()).expect("Found invalid UTF-8");

            rowids.push(value);

            if rowids.len() == self.batch_size {
                break;
            }
        }

        if rowids.len() < 1 {
            return None;
        }

        let mut struct_builder = StructBuilder::from_fields(self.projected_schema.clone().fields().clone(), rowids.len());
        for _ in rowids.clone() {
            struct_builder.append(true);
        }

        for i in 0..self.projected_schema.clone().fields().len() {
            let field = Arc::from(self.projected_schema.field(i).clone());
            let field_name = field.name();
            let field_data_type = field.data_type();

            if field_name.contains(meta_const::COLUMN_ROWID) {
                for rowid in rowids.clone() {
                    struct_builder.field_builder::<StringBuilder>(i).unwrap().append_value(rowid);
                }
            } else {
                let column_name = field_name.to_ident();
                let sparrow_column = table_column.get_sparrow_column(column_name).unwrap();

                for rowid in rowids.clone() {
                    let db_key = util::dbkey::create_column_key(self.table.option.full_table_name.clone(), sparrow_column.store_id, rowid.as_str());
                    let db_value = sled_db.get(db_key.clone());

                    match db_value {
                        Ok(value) => {
                            match value {
                                Some(value) => {
                                    match field_data_type {
                                        DataType::Utf8 => {
                                            match std::str::from_utf8(value.as_ref()) {
                                                Ok(value) => {
                                                    struct_builder.field_builder::<StringBuilder>(i).unwrap().append_value(value);
                                                }
                                                Err(error) => {
                                                    return Some(Err(ArrowError::CastError(format!(
                                                        "Error parsing '{:?}' as utf8: {:?}",
                                                        value,
                                                        error
                                                    ))));
                                                }
                                            }
                                        }
                                        DataType::Int32 => {
                                            let value = lexical::parse::<i32, _>(value.as_bytes()).unwrap();
                                            struct_builder.field_builder::<Int32Builder>(i).unwrap().append_value(value);
                                        }
                                        DataType::Int64 => {
                                            let value = lexical::parse::<i64, _>(value.as_bytes()).unwrap();
                                            struct_builder.field_builder::<Int64Builder>(i).unwrap().append_value(value);
                                        }
                                        _ => {
                                            return Some(Err(ArrowError::CastError(format!(
                                                "Unsupported data type: {:?}",
                                                field_data_type,
                                            ))));
                                        }
                                    }
                                }
                                None => {
                                    match field.data_type() {
                                        DataType::Utf8 => {
                                            struct_builder.field_builder::<StringBuilder>(i).unwrap().append_null();
                                        }
                                        DataType::Int32 => {
                                            struct_builder.field_builder::<Int32Builder>(i).unwrap().append_null();
                                        }
                                        DataType::Int64 => {
                                            struct_builder.field_builder::<Int64Builder>(i).unwrap().append_null();
                                        }
                                        _ => {
                                            return Some(Err(ArrowError::CastError(format!(
                                                "Unsupported data type: {:?}",
                                                field_data_type,
                                            ))));
                                        }
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            return Some(Err(ArrowError::IoError(format!(
                                "Error get key from sled, key: {:?}, error: {:?}",
                                db_key,
                                error
                            ))));
                        }
                    }
                }
            }
        }

        let struct_array = struct_builder.finish();
        let record_batch = RecordBatch::from(&struct_array);

        Some(Ok(record_batch))
    }
}