use std::borrow::Cow;
use std::fs::File;
use std::string::String;
use std::sync::{Arc, Mutex};

use arrow::array::ArrayRef;
use arrow::csv;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bitflags::_core::any::Any;
use datafusion::datasource::datasource::{Statistics, TableProviderFilterPushDown};
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;
use sqlparser::ast::ObjectName;

use crate::core::global_context::GlobalContext;
use crate::datafusion_impl::physical_plan::rocksdb::RocksdbExec;
use crate::meta::{def, meta_util};

pub struct RocksdbTable {
    core_context: Arc<Mutex<GlobalContext>>,
    table_schema: def::TableDef,
    full_table_name: ObjectName,
}

impl RocksdbTable {
    #[allow(missing_docs)]
    pub fn new(core_context: Arc<Mutex<GlobalContext>>, table_schema: def::TableDef, full_table_name: ObjectName) -> Self {
        Self {
            core_context,
            table_schema,
            full_table_name,
        }
    }
}

impl TableProvider for RocksdbTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::new(self.table_schema.to_dfschema().unwrap().into())
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = RocksdbExec::try_new(
            self.core_context.clone(),
            self.table_schema.clone(),
            self.full_table_name.clone(),
            projection.clone(),
            batch_size,
            filters.clone(),
        )?;
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