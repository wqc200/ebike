use num::ToPrimitive;
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::{Result};
use sled::Db as sledDb;
use sqlparser::ast::{Assignment, ColumnDef, ColumnOption, ColumnOptionDef, DataType as SQLDataType, Ident, ObjectName, SqlOption, TableConstraint, Value};

use crate::mysql::error::{MysqlError, MysqlResult};
use crate::store::rocksdb::db::DB as rocksdbDB;
use crate::test;
use crate::util;

use crate::meta::meta_util;
use std::collections::hash_map::RandomState;
use parquet::data_type::AsBytes;

#[derive(Debug, Clone)]
pub struct Variable {
    variable_map: HashMap<String, String>,
}

impl Variable {
    pub fn new() -> Self {
        let variable_map: HashMap<String, String> = HashMap::new();
        Self {
            variable_map
        }
    }

    pub fn get_variable(&self, name: &str) -> Option<&String> {
        return self.variable_map.get(name)
    }

    pub fn add_variable_map(&mut self, variable_map: HashMap<String, String>) {
        for (variable_name, variable_value) in variable_map {
            self.variable_map.insert(variable_name, variable_value);
        }
    }
}
