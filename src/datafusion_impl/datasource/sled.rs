use std::fs::File;
use std::string::String;
use std::sync::Arc;
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

use crate::datafusion_impl::physical_plan::sled::SledExec;
use crate::core::global_context::GlobalContext;

pub struct SledTable {
    global_context: GlobalContext,
    schema: Arc<Schema>,
    path: String,
    db_name: String,
    table_name: String,
}

impl SledTable {
    #[allow(missing_docs)]
    pub fn new(global_context: GlobalContext, schema: Arc<Schema>, path: &str, db_name: &str, table_name: &str) -> Self {
        Self {
            global_context,
            schema,
            path: path.to_string(),
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
        }
    }
}

impl TableProvider for SledTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
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
            self.schema.clone(),
            self.path.as_str(),
            self.db_name.as_str(),
            self.table_name.as_str(),
            projection.clone(),
            batch_size,
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