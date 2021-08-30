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

use crate::core::global_context::GlobalContext;
use crate::meta::{meta_util as MetaUtil, meta_const, meta_util};
use crate::store::reader::reader_util;
use crate::store::rocksdb::db::DB as RocksdbDB;
use crate::store::rocksdb::iterator::DBRawIterator;
use crate::store::rocksdb::option::{Options, ReadOptions};
use crate::store::rocksdb::slice_transform::SliceTransform;
use crate::util;
use crate::util::dbkey;
use crate::store::reader::reader_util::{SeekType, ScanOrder, Interval};
use std::cmp::Ordering;
use crate::util::dbkey::CreateScanKey;
use sqlparser::ast::ObjectName;
use crate::mysql::error::MysqlError;
use crate::util::convert::{ToObjectName, ToIdent};
use crate::meta::def::TableDef;

pub struct RocksdbReader {
    global_context: Arc<Mutex<GlobalContext>>,
    table_schema: TableDef,
    full_table_name: ObjectName,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    batch_size: usize,
    rocksdb_iter: DBRawIterator,
    start_scan_key: CreateScanKey,
    end_scan_key: CreateScanKey,
}

impl RocksdbReader {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        table_schema: TableDef,
        full_table_name: ObjectName,
        batch_size: usize,
        projection: Option<Vec<usize>>,
        filters: &[Expr],
    ) -> Self {
        let schema_ref = table_schema.to_schemaref();

        let projected_schema = match projection.clone() {
            Some(projection) => {
                let fields = schema_ref.fields();
                let projected_fields: Vec<Field> =
                    projection.iter().map(|i| fields[*i].clone()).collect();

                Arc::new(Schema::new(projected_fields))
            }
            None => schema_ref.clone(),
        };

        let mut rocksdb_iter = global_context.lock().unwrap().engine.rocksdb_db.as_ref().unwrap().raw_iterator();

        let mut start_scan_key = CreateScanKey::new("");
        let mut end_scan_key = CreateScanKey::new("");
        let table_index_prefix = reader_util::get_seek_prefix(global_context.clone(), full_table_name.clone(), table_schema.clone(), filters.clone()).unwrap();
        match table_index_prefix {
            SeekType::NoRecord => {},
            SeekType::FullTableScan { start, end} => {
                rocksdb_iter.seek(start.clone());
                start_scan_key = CreateScanKey::new(start.clone().as_str());
                end_scan_key = CreateScanKey::new(end.clone().as_str());
            }
            SeekType::UsingTheIndex { index_name, order, start, end} => {
                rocksdb_iter.seek(start.key().clone());
                start_scan_key = start;
                end_scan_key = end;
            }
        };

        Self {
            global_context,
            table_schema,
            full_table_name,
            projection,
            projected_schema,
            batch_size,
            rocksdb_iter,
            start_scan_key,
            end_scan_key,
        }
    }

    pub fn projected_schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }
}

impl Iterator for RocksdbReader {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let global_context = &self.global_context.lock().unwrap();
        let rocksdb_db = global_context.engine.rocksdb_db.as_ref().unwrap();

        let mut rowids: Vec<String> = vec![];

        let catalog_name = meta_util::cut_out_catalog_name(self.full_table_name.clone());
        let schema_name = meta_util::cut_out_schema_name(self.full_table_name.clone());
        let table_name = meta_util::cut_out_table_name(self.full_table_name.clone());

        while self.rocksdb_iter.valid() {
            let key = self.rocksdb_iter.key().unwrap();
            let key = String::from_utf8(key.to_vec()).expect("Found invalid UTF-8");
            log::debug!("row key: {:?}", key);

            match self.start_scan_key.interval() {
                Interval::Open => {
                    if key.starts_with(self.start_scan_key.key().as_str()) {
                        continue;
                    }
                }
                Interval::Closed => {}
            }
            match self.end_scan_key.interval() {
                Interval::Open => {
                    if key.starts_with(self.end_scan_key.key().as_str()) {
                        break;
                    }
                }
                Interval::Closed => {}
            }
            if !key.starts_with(self.end_scan_key.key().as_str()) {
                match key.as_str().partial_cmp(self.end_scan_key.key().as_str()) {
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

            let value = self.rocksdb_iter.value().unwrap();
            let value = String::from_utf8(value.to_vec()).expect("Found invalid UTF-8");
            log::debug!("row value: {:?}", value);

            rowids.push(value);

            self.rocksdb_iter.next();

            if rowids.len() == self.batch_size {
                break;
            }
        }

        log::debug!("rowids: {:?}", rowids);

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

                let result = global_context.meta_cache.get_serial_number(self.full_table_name.clone(), column_name.clone());
                let column_index = match result {
                    Ok(value) => value,
                    Err(error) => {
                        return Some(Err(ArrowError::SchemaError(format!(
                            "Error get serial number '{:?}'",
                            error
                        ))));
                    }
                };

                for rowid in rowids.clone() {
                    let record_column_key = util::dbkey::create_record_column(self.full_table_name.clone(), column_index, rowid.as_str());
                    let db_value = rocksdb_db.get(record_column_key.clone());

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
                                "Error get '{:?}' from rocksdb: {:?}",
                                record_column_key.clone(),
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