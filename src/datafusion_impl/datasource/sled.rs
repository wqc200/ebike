use std::fs::File;
use std::string::String;
use std::sync::{Arc, Mutex};
use std::borrow::Cow;

use bitflags::_core::any::Any;

use arrow::csv;
use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema, DataType};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::datasource::{Statistics, TableProviderFilterPushDown};
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;
use sqlparser::ast::ObjectName;

use crate::datafusion_impl::physical_plan::sled::SledExec;
use crate::core::global_context::GlobalContext;
use crate::meta::def::TableDef;

pub struct SledTable {
    global_context: Arc<Mutex<GlobalContext>>,
    table_def: TableDef,
    full_table_name: ObjectName,
}

impl SledTable {
    #[allow(missing_docs)]
    pub fn new(global_context: Arc<Mutex<GlobalContext>>, table_def: TableDef, full_table_name: ObjectName) -> Self {
        Self {
            global_context,
            table_def,
            full_table_name,
        }
    }
}

impl TableProvider for SledTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.table_def.to_schemaref()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = SledExec::try_new(
            self.global_context.clone(),
            self.table_def.clone(),
            self.full_table_name.clone(),
            projection.clone(),
            batch_size,
            &[])?;
        Ok(Arc::new(exec))
    }

    fn statistics(&self) -> Statistics {
        let statistics = Statistics::default();
        statistics
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}