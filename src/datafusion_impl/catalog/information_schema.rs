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

//! Implements the SQL [Information Schema] for DataFusion.
//!
//! Information Schema](https://en.wikipedia.org/wiki/Information_schema)

use std::{any, sync::Arc};

use arrow::{
    array::{StringBuilder, UInt64Builder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrow::array::{
    ArrayData,
    BinaryArray,
    Float32Array,
    Float64Array,
    Int16Array,
    Int32Array,
    Int64Array,
    Int8Array,
    StringArray,
    UInt16Array,
    UInt32Array,
    UInt64Array,
    UInt8Array,
};
use datafusion::catalog::{
    catalog::{CatalogList, CatalogProvider},
    schema::SchemaProvider,
};
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};

use crate::meta::{meta_const, meta_util, initial};
use datafusion::catalog::catalog::MemoryCatalogProvider;
use crate::core::global_context::GlobalContext;
use std::sync::Mutex;
use crate::store::engine::engine_util;
use crate::util::convert::ToObjectName;
use crate::meta::def::information_schema::key_column_usage;

/// Wraps another [`CatalogProvider`] and adds a "information_schema"
/// schema that can introspect on tables in the catalog_list
pub struct CatalogWithInformationSchemaProvider {
    global_context: Arc<Mutex<GlobalContext>>,
    catalog_list: Arc<dyn CatalogList>,
    /// wrapped provider
    inner: Arc<dyn CatalogProvider>,
}

impl CatalogWithInformationSchemaProvider {
    pub fn new(
        global_context: Arc<Mutex<GlobalContext>>,
        catalog_list: Arc<dyn CatalogList>,
        inner: Arc<dyn CatalogProvider>,
    ) -> Self {
        Self {
            global_context,
            catalog_list,
            inner,
        }
    }
}

impl CatalogWithInformationSchemaProvider {
    /// Adds a new schema to this catalog.
    /// If a schema of the same name existed before, it is replaced in the catalog and returned.
    pub fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Option<Arc<dyn SchemaProvider>> {
        let catalog_provider = self.inner.as_any()
            .downcast_ref::<MemoryCatalogProvider>()
            .expect("Catalog provider was a MemoryCatalogProvider");
        catalog_provider.register_schema(name, schema)
    }
}

impl CatalogProvider for CatalogWithInformationSchemaProvider {
    fn as_any(&self) -> &dyn any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name.eq_ignore_ascii_case(meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA) {
            Some(Arc::new(InformationSchemaProvider {
                global_context: self.global_context.clone(),
                catalog_list: self.catalog_list.clone(),
                inner: self.inner.schema(name).unwrap(),
            }))
        } else {
            self.inner.schema(name)
        }
    }
}

/// Implements the `information_schema` virtual schema and tables
///
/// The underlying tables in the `information_schema` are created on
/// demand. This means that if more tables are added to the underlying
/// providers, they will appear the next time the `information_schema`
/// table is queried.
struct InformationSchemaProvider {
    global_context: Arc<Mutex<GlobalContext>>,
    catalog_list: Arc<dyn CatalogList>,
    /// wrapped provider
    inner: Arc<dyn SchemaProvider>,
}

impl InformationSchemaProvider {
    fn make_dual(&self) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1)])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        ).unwrap();

        let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap();
        Arc::new(mem_table)
    }

    fn make_check_constraints(&self) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("constraint_catalog", DataType::Utf8, false),
            Field::new("constraint_schema", DataType::Utf8, false),
            Field::new("constraint_name", DataType::Utf8, false),
            Field::new("check_clause", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::new_empty(schema.clone());

        let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap();
        Arc::new(mem_table)
    }

    fn make_referential_constraints(&self) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("constraint_catalog", DataType::Utf8, false),
            Field::new("constraint_schema", DataType::Utf8, false),
            Field::new("constraint_name", DataType::Utf8, false),
            Field::new("unique_constraint_catalog", DataType::Utf8, false),
            Field::new("unique_constraint_schema", DataType::Utf8, false),
            Field::new("unique_constraint_name", DataType::Utf8, false),
            Field::new("match_option", DataType::Utf8, false),
            Field::new("update_rule", DataType::Utf8, false),
            Field::new("delete_rule", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("referenced_table_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::new_empty(schema.clone());

        let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap();
        Arc::new(mem_table)
    }
}

impl SchemaProvider for InformationSchemaProvider {
    fn as_any(&self) -> &(dyn any::Any + 'static) {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner
            .table_names()
            .into_iter()
            .chain(std::iter::once(meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_DUAL.to_string()))
            .chain(std::iter::once(meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_CHECK_CONSTRAINTS.to_string()))
            .chain(std::iter::once(meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_REFERENTIAL_CONSTRAINTS.to_string()))
            .collect::<Vec<String>>()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        if name.eq_ignore_ascii_case(meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_DUAL) {
            Some(self.make_dual())
        } else if name.eq_ignore_ascii_case(meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_CHECK_CONSTRAINTS) {
            Some(self.make_check_constraints())
        } else if name.eq_ignore_ascii_case(meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_REFERENTIAL_CONSTRAINTS) {
            Some(self.make_referential_constraints())
        } else {
            self.inner.table(name)
        }
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }
}
