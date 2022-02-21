use bstr::ByteSlice;
use std::sync::{Arc, Mutex};

use arrow::array::StructBuilder;
use arrow::array::{Float32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::{ArrowError, Result};
use arrow::record_batch::RecordBatch;
use datafusion::logical_plan::Expr;
use sled::Iter as SledIter;
use sqlparser::ast::DataType as SQLDataType;
use std::cmp::Ordering;

use crate::core::global_context::GlobalContext;
use crate::meta::meta_const;
use crate::meta::meta_def::TableDef;
use crate::store::reader::reader_util;
use crate::store::reader::reader_util::{PointType, SeekType};
use crate::util;
use crate::util::convert::ToIdent;
use crate::util::dbkey::CreateScanKey;
use lexical::Error;

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
            None => table.to_schema_ref().clone(),
        };

        let table_index_prefix = reader_util::get_seek_prefix(
            global_context.clone(),
            full_table_name.clone(),
            table.clone(),
            filters.clone(),
        )
        .unwrap();
        let seek = match table_index_prefix {
            SeekType::FullTableScan { start, end } => {
                let iter = global_context
                    .lock()
                    .unwrap()
                    .engine
                    .sled_db
                    .as_ref()
                    .unwrap()
                    .scan_prefix(start.key.clone());
                Seek { iter, start, end }
            }
            SeekType::UsingTheIndex { start, end, .. } => {
                let iter = global_context
                    .lock()
                    .unwrap()
                    .engine
                    .sled_db
                    .as_ref()
                    .unwrap()
                    .scan_prefix(start.key.clone());
                Seek { iter, start, end }
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
                Some(item) => match item {
                    Ok((key, value)) => (key, value),
                    Err(error) => {
                        return Some(Err(ArrowError::IoError(format!(
                            "Error iter from sled: '{:?}'",
                            error
                        ))));
                    }
                },
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
                    Some(a) => match a {
                        Ordering::Less => {}
                        Ordering::Equal => {}
                        Ordering::Greater => break,
                    },
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

        let mut struct_builder = StructBuilder::from_fields(
            self.projected_schema.clone().fields().clone(),
            rowids.len(),
        );
        for _ in rowids.clone() {
            let result = struct_builder.append(true);
            if let Err(e) = result {
                return Some(Err(e));
            }
        }

        for i in 0..self.projected_schema.clone().fields().len() {
            let field = Arc::from(self.projected_schema.field(i).clone());
            let field_name = field.name();

            if field_name.eq(meta_const::COLUMN_ROWID) {
                for rowid in rowids.clone() {
                    let result = struct_builder
                        .field_builder::<StringBuilder>(i)
                        .unwrap()
                        .append_value(rowid);
                    if let Err(e) = result {
                        return Some(Err(e));
                    }
                }
            } else {
                let column_name = field_name.to_ident();
                let sparrow_column = table_column.get_sparrow_column(column_name.clone()).unwrap();
                let sql_data_type = sparrow_column.sql_column.data_type;

                for rowid in rowids.clone() {
                    let db_key = util::dbkey::create_column_key(
                        self.table.option.full_table_name.clone(),
                        sparrow_column.store_id,
                        rowid.as_str(),
                    );
                    let result = sled_db.get(db_key.clone());

                    let mut db_value;
                    match result {
                        Ok(get_value) => match get_value {
                            Some(store_value) => {
                                let bytes = store_value.as_ref().to_vec();
                                // value is null
                                if bytes.len() == 1 && bytes[0] == 0x00 {
                                    db_value = None;
                                } else {
                                    db_value = Some(bytes)
                                }
                            }
                            None => db_value = None,
                        },
                        Err(error) => {
                            return Some(Err(ArrowError::IoError(format!(
                                "Error get key from sled, key: {:?}, error: {:?}",
                                db_key, error
                            ))));
                        }
                    }

                    match db_value {
                        Some(value) => match sql_data_type {
                            SQLDataType::Char(_) => match std::str::from_utf8(value.as_ref()) {
                                Ok(value) => {
                                    let result = struct_builder
                                        .field_builder::<StringBuilder>(i)
                                        .unwrap()
                                        .append_value(value);
                                    if let Err(e) = result {
                                        return Some(Err(e));
                                    }
                                }
                                Err(error) => {
                                    return Some(Err(ArrowError::CastError(format!(
                                        "Error parsing '{:?}' as utf8: {:?}",
                                        value, error
                                    ))));
                                }
                            },
                            SQLDataType::Int(_) => {
                                let result = lexical::parse::<i64, _>(value.as_bytes());
                                match result {
                                    Ok(value) => {
                                        let result = struct_builder
                                            .field_builder::<Int64Builder>(i)
                                            .unwrap()
                                            .append_value(value);
                                        if let Err(e) = result {
                                            return Some(Err(e));
                                        }
                                    }
                                    Err(err) => {
                                        let error = format!("{}, rowid: {}, column name: {}, value: {:?}", err, rowid, column_name.clone(), value);
                                        return Some(Err(ArrowError::ParseError(error)));
                                    }
                                }
                            }
                            SQLDataType::Float(_) => {
                                let result = lexical::parse::<f64, _>(value.as_bytes());
                                match result {
                                    Ok(value) => {
                                        let result = struct_builder
                                            .field_builder::<Float64Builder>(i)
                                            .unwrap()
                                            .append_value(value);
                                        if let Err(e) = result {
                                            return Some(Err(e));
                                        }
                                    }
                                    Err(err) => {
                                        return Some(Err(ArrowError::ParseError(err.to_string())));
                                    }
                                }
                            }
                            _ => {
                                return Some(Err(ArrowError::CastError(format!(
                                    "Unsupported sql data type: {:?}",
                                    sql_data_type,
                                ))));
                            }
                        },
                        None => match sql_data_type {
                            SQLDataType::Char(_) => {
                                let result = struct_builder
                                    .field_builder::<StringBuilder>(i)
                                    .unwrap()
                                    .append_null();
                                if let Err(e) = result {
                                    return Some(Err(e));
                                }
                            }
                            SQLDataType::Int(_) => {
                                let result = struct_builder
                                    .field_builder::<Int64Builder>(i)
                                    .unwrap()
                                    .append_null();
                                if let Err(e) = result {
                                    return Some(Err(e));
                                }
                            }
                            SQLDataType::Float(_) => {
                                let result = struct_builder
                                    .field_builder::<Float64Builder>(i)
                                    .unwrap()
                                    .append_null();
                                if let Err(e) = result {
                                    return Some(Err(e));
                                }
                            }
                            _ => {
                                return Some(Err(ArrowError::CastError(format!(
                                    "Unsupported sql data type: {:?}",
                                    sql_data_type,
                                ))));
                            }
                        },
                    }
                }
            }
        }

        let struct_array = struct_builder.finish();
        let record_batch = RecordBatch::from(&struct_array);

        Some(Ok(record_batch))
    }
}
