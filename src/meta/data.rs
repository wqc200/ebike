use std::collections::{HashMap};

use sqlparser::ast::{Ident, ObjectName};

use crate::meta::{meta_def};
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::meta::meta_def::TableDef;
use crate::meta::meta_def::SchemaDef;

#[derive(Debug, Clone)]
pub struct MetaData {
    schema_map: HashMap<ObjectName, meta_def::SchemaDef>,
    table_map: HashMap<ObjectName, meta_def::TableDef>,
    /// Map the column name to an serial number
    serial_number_map: HashMap<ObjectName, HashMap<Ident, usize>>,
}

impl MetaData {
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
        for (full_table_name, table_def) in table_def_map.iter() {
            self.add_table(full_table_name.clone(), table_def.clone());
        }
    }

    pub fn add_all_schema(&mut self, schema_map: HashMap<ObjectName, SchemaDef>) {
        for (full_schema_name, schema_def) in schema_map.iter() {
            self.add_schema(full_schema_name.clone(), schema_def.clone());
        }
    }

    pub fn add_table(&mut self, full_table_name: ObjectName, table: meta_def::TableDef) {
        let t = self.table_map.entry(full_table_name.clone()).or_insert(table.clone());
        *t = table;
    }

    pub fn add_schema(&mut self, full_schema_name: ObjectName, schema_def: SchemaDef) {
        self.schema_map.entry(full_schema_name.clone()).or_insert(schema_def.clone());
        ()
    }

    pub fn delete_schema(&mut self, full_schema_name: ObjectName) {
        self.schema_map.remove(&full_schema_name).unwrap();
    }

    pub fn get_schema_map(&self) -> HashMap<ObjectName, meta_def::SchemaDef> {
        self.schema_map.clone()
    }

    pub fn get_schema(&self, full_schema_name: ObjectName) -> Option<&SchemaDef> {
        self.schema_map.get(&full_schema_name)
    }

    pub fn get_table_map(&self) -> HashMap<ObjectName, meta_def::TableDef> {
        self.table_map.clone()
    }

    pub fn get_table(&self, full_table_name: ObjectName) -> Option<&TableDef> {
        self.table_map.get(&full_table_name)
    }

    pub fn delete_table(&mut self, full_table_name: ObjectName) {
        self.table_map.remove(&full_table_name).unwrap();
    }
}
