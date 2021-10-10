use std::sync::{Arc, Mutex};

use bitflags::_core::any::Any;

use arrow::datatypes::Schema;
use datafusion::datasource::datasource::{TableProviderFilterPushDown};
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;

use crate::datafusion_impl::physical_plan::sled::SledExec;
use crate::core::global_context::GlobalContext;
use crate::meta::meta_def::TableDef;

pub struct SledTable {
    global_context: Arc<Mutex<GlobalContext>>,
    table: TableDef,
}

impl SledTable {
    #[allow(missing_docs)]
    pub fn new(global_context: Arc<Mutex<GlobalContext>>, table: TableDef) -> Self {
        Self {
            global_context,
            table,
        }
    }
}

impl TableProvider for SledTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.table.to_schema_ref()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        _: &[Expr],
        _: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = SledExec::try_new(
            self.global_context.clone(),
            self.table.clone(),
            projection.clone(),
            batch_size,
            &[])?;
        Ok(Arc::new(exec))
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}