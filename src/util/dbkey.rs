use datafusion::error::{DataFusionError, Result};
use datafusion::logical_plan::{Expr};

use crate::core::core_util;
use crate::meta_util;
use crate::store::reader::reader_util::{CompareValue, ScanOrder, Interval};
use datafusion::scalar::ScalarValue;
use crate::mysql::error::{MysqlResult, MysqlError};
use crate::meta::meta_const::MYSQL_ERROR_CODE_UNKNOWN_ERROR;
use sqlparser::ast::{ObjectName, Ident};
use crate::meta::meta_const;

pub fn scan_column_name(schema_name: &str) -> Box<[u8]> {
    let mut k = String::from("/Schema/ColumnName/ToIndex");

    k.push_str("/");
    k.push_str(schema_name);

    key(k.as_bytes())
}

pub fn create_current_serial_number(full_table_name: ObjectName) -> String {
    let mut k = String::from("/Schema/ColumnIndex/Current");

    k.push_str("/");
    k.push_str(full_table_name.to_string().as_str());

    k
}

pub fn create_column_index_to_name(schema_name: &str, column_index: usize) -> Box<[u8]> {
    let mut k = String::from("/Schema/ColumnIndex/ToName");

    k.push_str("/");
    k.push_str(schema_name);

    let ci = (column_index as i32).to_string();
    k.push_str("/");
    k.push_str(ci.as_str());

    key(k.as_bytes())
}

pub fn create_column_serial_number(full_table_name: ObjectName, column_name: Ident) -> String {
    let mut k = String::from("/Schema/ColumnName/ToIndex");

    k.push_str("/");
    k.push_str(full_table_name.to_string().as_str());

    k.push_str("/");
    k.push_str(column_name.to_string().as_str());

    k
}

pub fn create_database_key(schema_name: String) -> Box<[u8]> {
    let k = format!("/System/database/name/{}", schema_name);
    key(k.as_bytes())
}

pub fn create_table_key(schema_name: String) -> Box<[u8]> {
    let k = format!("/System/table/name/{}", schema_name);
    key(k.as_bytes())
}

pub fn create_record_rowid(full_table_name: ObjectName, uuid: &str) -> String {
    let mut k = String::from("/Table/rowid");

    k.push_str("/");
    k.push_str(full_table_name.to_string().as_str());

    k.push_str("/");
    k.push_str(uuid);

    k
}

pub fn create_column_rowid_key(full_table_name: ObjectName, uuid: &str) -> String {
    let mut k = String::from("/Table/rowid");

    k.push_str("/");
    k.push_str(full_table_name.to_string().as_str());

    k.push_str("/");
    k.push_str(uuid);

    k
}

pub fn create_column_key(full_table_name: ObjectName, orm_id: usize, uuid: &str) -> String {
    let mut k = String::from("/Table/index/column");

    k.push_str("/");
    k.push_str(full_table_name.to_string().as_str());

    k.push_str("/");
    k.push_str(orm_id.to_string().as_str());

    k.push_str("/");
    k.push_str(&uuid);

    k
}

pub fn parse_record_rowid(key: String) -> Result<String> {
    let v: Vec<&str> = key.split("/").collect();
    if v.len() < 6 {
        return Err(DataFusionError::Execution(format!(
            "row id not found: {:?}",
            key
        )));
    }
    Ok(v[5].to_string())
}

pub fn scan_record_rowid(full_table_name: ObjectName) -> String {
    let mut k = String::from("/Table/rowid/");

    k.push_str(full_table_name.to_string().as_str());
    k.push_str("/");

    k
}

pub fn create_index(full_table_name: ObjectName, index_name: &str, serial_number_value_vec: Vec<(usize, ScalarValue)>) -> MysqlResult<String> {
    let mut k = String::from("/Table/index/key/");
    k.push_str(full_table_name.to_string().as_str());
    k.push_str("/");
    k.push_str(index_name);
    k.push_str("/");

    for (serial_number, scalar_value) in serial_number_value_vec {
        k.push_str(serial_number.to_string().as_str());
        k.push_str("/");

        match scalar_value {
            // ScalarValue::Binary(limit) => {
            //     if let Some(value) = limit {
            //         Ok(Some(value.as_bytes().to_hex()))
            //     } else {
            //         Ok(None)
            //     }
            // }
            ScalarValue::Int32(limit) => {
                if let Some(value) = limit {
                    let new_value = (value as u64) ^ meta_const::SIGN_MASK;
                    k.push_str("1/");
                    k.push_str(new_value.to_string().as_str());
                    k.push_str("/");
                } else {
                    k.push_str("0/");
                }
            }
            ScalarValue::Int64(limit) => {
                if let Some(value) = limit {
                    let new_value = (value as u64) ^ meta_const::SIGN_MASK;
                    k.push_str("1/");
                    k.push_str(new_value.to_string().as_str());
                    k.push_str("/");
                } else {
                    k.push_str("0/");
                }
            }
            ScalarValue::Utf8(limit) => {
                if let Some(value) = limit {
                    k.push_str("1/");
                    k.push_str(value.as_str());
                    k.push_str("/");
                } else {
                    k.push_str("0/");
                }
            }
            _ => return Err(MysqlError::new_global_error(
                MYSQL_ERROR_CODE_UNKNOWN_ERROR,
                format!("Unsupported convert scalar value to string: {:?}", scalar_value).as_str(),
            )),
        }
    }

    Ok(k)
}

pub struct CreateScanKey {
    key: String,
    interval: Interval,
}

impl CreateScanKey {
    pub fn new(prefix: &str) -> Self {
        let key = String::from(prefix);
        Self {
            key,
            interval: Interval::Closed,
        }
    }

    pub fn add_key(&mut self, key: &str) {
        self.key.push_str(key);
        self.key.push_str("/");
    }

    pub fn change_interval(&mut self, interval: Interval) {
        self.interval = interval
    }

    pub fn key(&self) -> String {
        self.key.clone()
    }

    pub fn interval(&self) -> Interval {
        self.interval.clone()
    }
}

pub fn create_scan_index(full_table_name: ObjectName, index_name: &str, column_index_values: Vec<(usize, CompareValue)>) -> (CreateScanKey, CreateScanKey) {
    let mut start = CreateScanKey::new("/Table/index/key/");
    let mut end = CreateScanKey::new("/Table/index/key/");
    start.add_key(full_table_name.to_string().as_str());
    end.add_key(full_table_name.to_string().as_str());

    start.add_key(index_name);
    end.add_key(index_name);

    for (column_index, compare_value) in column_index_values {
        start.add_key(column_index.to_string().as_str());
        end.add_key(column_index.to_string().as_str());

        match compare_value {
            CompareValue::Null => {
                start.add_key("0");
                end.add_key("0");
            }
            CompareValue::NotNull(not_null_value) => {
                if let Some((value, interval)) = not_null_value.getStart() {
                    start.add_key("1");
                    start.add_key(value.as_str());
                    start.change_interval(interval);
                }
                if let Some((value, interval)) = not_null_value.getEnd() {
                    end.add_key("1");
                    end.add_key(value.as_str());
                    end.change_interval(interval);
                }
            }
        }
    }

    (start, end)
}


pub fn scan_index(schema_name: &str, index_name: &str, column_index_values: Vec<(usize, CompareValue)>) -> (String, String) {
    let mut start = String::from("/Table/index/key/");
    start.push_str(schema_name);
    start.push_str("/");
    start.push_str(index_name);
    start.push_str("/");
    let mut end = start.clone();

    for (column_index, compare_value) in column_index_values {
        start.push_str(column_index.to_string().as_str());
        start.push_str("/");
        end.push_str(column_index.to_string().as_str());
        end.push_str("/");

        match compare_value {
            CompareValue::Null => {
                start.push_str("0");
                start.push_str("/");
                end.push_str("0");
                end.push_str("/");
            }
            CompareValue::NotNull(not_null_value) => {
                if let Some((value, interval)) = not_null_value.getStart() {
                    start.push_str("1/");
                    start.push_str(value.as_str());
                    start.push_str("/");
                }
                if let Some((value, interval)) = not_null_value.getEnd() {
                    end.push_str("1/");
                    end.push_str(value.as_str());
                    end.push_str("/");
                }
            }
        }
    }

    (start.clone(), end.clone())
}

pub fn create_record_primary(schema_name: &str, column_indexes: Vec<usize>, column_values: Vec<Expr>) -> Box<[u8]> {
    let mut k = String::from("/Table/primary/key");

    k.push_str("/");
    k.push_str(schema_name);

    for x in column_indexes {
        k.push_str("/");
        k.push_str(x.to_string().as_str());
    }

    for expr in column_values {
        let column_value = core_util::get_real_value(expr.clone()).unwrap();
        if let Some(value) = column_value {
            k.push_str("/");
            k.push_str(value.as_str());
        }
    }

    key(k.as_bytes())
}

pub fn parse_record_primary(key: Box<[u8]>) -> Vec<String> {
    let s = String::from_utf8(key.into_vec()).expect("Found invalid UTF-8");
    let v: Vec<&str> = s.split("/").collect();
    let mut a: Vec<String> = vec![];
    let mut i = 0;
    for x in v {
        if i < 5 {
            continue;
        }
        a.push(String::from(x));
        i += 1;
    }
    a
}

pub fn scan_record_primary(schema_name: &str) -> Box<[u8]> {
    let mut k = String::from("/Table/primary/key");

    k.push_str("/");
    k.push_str(schema_name);

    key(k.as_bytes())
}

pub fn create_record_unique(schema_name: &str, column_index_vec: Vec<usize>, column_value_vec: Vec<String>) -> Box<[u8]> {
    let mut k = String::from("/Table/unique");

    k.push_str("/");
    k.push_str(schema_name);

    for x in column_index_vec {
        k.push_str("/");
        k.push_str(x.to_string().as_str());
        let column_value = column_value_vec.get(x).unwrap();
        k.push_str("/");
        k.push_str(column_value.as_str());
    }

    key(k.as_bytes())
}

pub fn scan_record_unique(schema_name: &str, column_tuple_vec: Vec<(usize, String)>) -> Box<[u8]> {
    let mut k = String::from("/Table/unique");

    k.push_str("/");
    k.push_str(schema_name);

    for (column_index, column_value) in column_tuple_vec {
        k.push_str("/");
        k.push_str(column_index.to_string().as_str());
        k.push_str("/");
        k.push_str(column_value.to_string().as_str());
    }

    key(k.as_bytes())
}

pub fn create_record_column(full_table_name: ObjectName, orm_id: usize, uuid: &str) -> String {
    let mut k = String::from("/Table/index/column");

    k.push_str("/");
    k.push_str(full_table_name.to_string().as_str());

    k.push_str("/");
    k.push_str(orm_id.to_string().as_str());

    k.push_str("/");
    k.push_str(&uuid);

    k
}

pub fn parse_record_column(key: Box<[u8]>) -> (usize, usize) {
    let s = String::from_utf8(key.into_vec()).expect("Found invalid UTF-8");
    let v: Vec<&str> = s.split("/").collect();
    (v[6].to_string().parse::<usize>().unwrap(), v.get(7).unwrap().to_string().parse::<usize>().unwrap())
}

pub fn scan_record_column(schema_name: &str, col_index: isize, row_index: isize) -> Box<[u8]> {
    let mut k = String::from("/Table/index/column");

    k.push_str("/");
    k.push_str(schema_name);

    let one: isize = -1;

    let ci = (col_index as i32).to_string();
    let ri = (row_index as i32).to_string();

    if col_index > one {
        k.push_str("/");
        k.push_str(&*ci);
    }
    if row_index > one {
        k.push_str("/");
        k.push_str(&*ri);
    }

    key(k.as_bytes())
}

fn key(k: &[u8]) -> Box<[u8]> {
    k.to_vec().into_boxed_slice()
}
