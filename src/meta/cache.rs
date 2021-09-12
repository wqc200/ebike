use std::collections::{HashMap, HashSet};
use std::collections::hash_map::RandomState;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use num::ToPrimitive;
use parquet::data_type::AsBytes;
use sled::Db as sledDb;
use sqlparser::ast::{Assignment, ColumnDef, ColumnOption, ColumnOptionDef, DataType as SQLDataType, Ident, ObjectName, SqlOption, TableConstraint, Value};

use crate::meta::{meta_def, meta_util};
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::store::rocksdb::db::DB as rocksdbDB;
use crate::test;
use crate::util;
use crate::meta::meta_def::TableDef;

#[derive(Debug, Clone)]
pub struct MetaCache {
    schema_map: HashMap<ObjectName, meta_def::SchemaDef>,
    table_map: HashMap<ObjectName, meta_def::TableDef>,
    /// Map the column name to an serial number
    serial_number_map: HashMap<ObjectName, HashMap<Ident, usize>>,
}

impl MetaCache {
    pub fn new() -> Self {
        let schema_map: HashMap<ObjectName, meta_def::SchemaDef> = HashMap::new();
        let table_map: HashMap<ObjectName, meta_def::TableDef> = HashMap::new();
        let serial_number_map: HashMap<ObjectName, HashMap<Ident, usize>> = HashMap::new();

        Self {
            schema_map,
            table_map,
            serial_number_map,
        }
    }

    pub fn add_all_table(&mut self, table_def_map: HashMap<ObjectName, meta_def::TableDef>) {
        for (schema_name, table_def) in table_def_map.iter() {
            self.add_table(schema_name.clone(), table_def.clone());
        }
    }

    pub fn add_all_schema(&mut self, schema_map: HashMap<ObjectName, meta_def::SchemaDef>) {
        for (schema_name, schema_def) in schema_map.iter() {
            self.add_schema(schema_name.clone(), schema_def.clone());
        }
    }

    pub fn add_table(&mut self, full_table_name: ObjectName, table: meta_def::TableDef) {
        let t = self.table_map.entry(full_table_name.clone()).or_insert(table.clone());
        *t = table;
    }

    pub fn add_schema(&mut self, full_schema_name: ObjectName, schema_def: meta_def::SchemaDef) {
        self.schema_map.entry(full_schema_name.clone()).or_insert(schema_def.clone());
        ()
    }

    pub fn get_serial_number_map(&mut self, full_table_name: ObjectName) -> Option<HashMap<Ident, usize>> {
        let result = self.serial_number_map.get(&full_table_name);
        match result {
            None => None,
            Some(map) => Some(map.clone()),
        }
    }

    pub fn add_serial_number(&mut self, full_table_name: ObjectName, column_name: Ident, serial_number: usize) {
        self.serial_number_map
            .entry(full_table_name.clone()).or_insert(HashMap::new())
            .entry(column_name.clone()).or_insert(serial_number);
    }

    pub fn delete_serial_number(&mut self, full_table_name: ObjectName, column_name: Ident) {
        self.serial_number_map.entry(full_table_name.clone()).or_insert(HashMap::new()).remove(&column_name);
    }

    pub fn get_serial_number(&self, full_table_name: ObjectName, column_name: Ident) -> MysqlResult<usize> {
        match self.serial_number_map.get(&full_table_name) {
            None => {
                Err(MysqlError::new_global_error(1105, format!("Unknown error, Table not found, schema name: {:?}.", full_table_name).as_str()))
            },
            Some(map) => {
                match map.get(&column_name) {
                    None => Err(MysqlError::new_global_error(1105, format!("Unknown error, Serial number not found, column name: {:?}.", column_name).as_str())),
                    Some(value) => {
                        Ok(value.clone())
                    }
                }
            }
        }
    }

    pub fn delete_table(&mut self, schema_name: ObjectName) {
        self.table_map.remove(&schema_name).unwrap();
    }

    pub fn get_schema_map(&self) -> HashMap<ObjectName, meta_def::SchemaDef> {
        self.schema_map.clone()
    }

    pub fn get_table_map(&self) -> HashMap<ObjectName, meta_def::TableDef> {
        self.table_map.clone()
    }

    pub fn get_table(&self, full_table_name: ObjectName) -> Option<&TableDef> {
        let table_schema = self.table_map.get(&full_table_name);
        table_schema
    }
}
