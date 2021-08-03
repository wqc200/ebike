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

//! Execution plan for reading CSV files

use std::fs::File;
use std::sync::{Arc, Mutex};
use std::borrow::Cow;
use std::borrow::Borrow;
use std::pin::Pin;
use std::task::{Context, Poll};

use bitflags::_core::any::Any;

use datafusion::error::{ExecutionError, Result};
use datafusion::physical_plan::{common, Partitioning, RecordBatchStream};
use datafusion::logical_plan::Expr;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow::error::Result as ArrowResult;
//use rocksdb::{Error, IteratorMode, Options, SliceTransform, Snapshot, WriteBatch, DB, DBRawIterator, ReadOptions};
use datafusion::physical_plan::ExecutionPlan;
use futures::Stream;

use crate::store::reader::sled::Reader;
use crate::store::rocksdb::option::{Options, ReadOptions};
use crate::store::rocksdb::slice_transform::SliceTransform;
use crate::store::rocksdb::db::DB;
use crate::core::context::CoreContext;
use crate::core::global_context::GlobalContext;
use crate::meta::def;
use sqlparser::ast::ObjectName;

#[derive(Debug, Clone)]
pub struct SledExec {
    core_context: CoreContext,
    schema: SchemaRef,
    path: String,
    db_name: String,
    table_name: String,
    projection: Option<Vec<usize>>,
    /// Schema after the projection has been applied
    projected_schema: SchemaRef,
    batch_size: usize,
}

impl SledExec {
    /// Create a new execution plan for reading a set of CSV files
    pub fn try_new(
        core_context: CoreContext,
        schema: SchemaRef,
        path: &str,
        db_name: &str,
        table_name: &str,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let projected_schema = match &projection {
            None => schema.clone(),
            Some(p) => SchemaRef::new(Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect())),
        };

        Ok(Self {
            core_context,
            path: path.to_string(),
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            schema,
            projection,
            projected_schema,
            batch_size,
        })
    }
}

impl ExecutionPlan for SledExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(ExecutionError::General(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        Ok(Arc::new(Mutex::new(SledIterator::try_new(
            self.core_context.clone(),
            self.schema.clone(),
            self.path.as_str(),
            self.db_name.as_str(),
            self.table_name.as_str(),
            self.projection.clone(),
            self.batch_size,
        )?)))
    }
}

struct SledStream {
    reader: Reader,
}

impl SledStream {
    pub fn try_new(
        core_context: Arc<Mutex<GlobalContext>>,
        schema: def::TableDef,
        path: &str,
        full_table_name: ObjectName,
        projection: Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
    ) -> Result<Self> {
        let reader = Reader::new(
            core_context,
            schema.clone(),
            path,
            full_table_name,
            batch_size,
            projection.clone(),
            filters,
        );

        Ok(Self { reader })
    }
}

impl Stream for SledStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.reader.next())
    }
}

impl RecordBatchStream for SledStream {
    fn schema(&self) -> SchemaRef {
        self.reader.projected_schema()
    }
}
