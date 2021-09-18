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
use std::any::Any;
use std::borrow::Borrow;
use std::borrow::Cow;
use std::fs::File;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow::array::ArrayRef;
use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
//use rocksdb::{Error, IteratorMode, Options, SliceTransform, Snapshot, WriteBatch, DB, DBRawIterator, ReadOptions};
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::{Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics};
use datafusion::physical_plan::common::SizedRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use futures::Stream;
use sqlparser::ast::ObjectName;

use crate::core::global_context::GlobalContext;
use crate::meta::{meta_def, meta_util};
use crate::store::reader::rocksdb::RocksdbReader;
use crate::store::rocksdb::db::DB;
use crate::store::rocksdb::option::{Options, ReadOptions};
use crate::store::rocksdb::slice_transform::SliceTransform;
use crate::util;

#[derive(Debug, Clone)]
pub struct RocksdbExec {
    global_context: Arc<Mutex<GlobalContext>>,
    table_def: meta_def::TableDef,
    projection: Option<Vec<usize>>,
    /// Schema after the projection has been applied
    projected_schema: SchemaRef,
    batch_size: usize,
    filters: Vec<Expr>,
}

impl RocksdbExec {
    /// Create a new execution plan for reading a set of CSV files
    pub fn try_new(
        global_context: Arc<Mutex<GlobalContext>>,
        table_def: meta_def::TableDef,
        projection: Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
    ) -> Result<Self> {
        let schema_ref = table_def.to_schema_ref();
        let projected_schema = match &projection {
            None => schema_ref,
            Some(p) => SchemaRef::new(Schema::new(p.iter().map(|i| schema_ref.field(*i).clone()).collect())),
        };

        Ok(Self {
            global_context,
            table_def,
            projection,
            projected_schema,
            batch_size,
            filters: filters.to_vec(),
        })
    }
}

#[async_trait]
impl ExecutionPlan for RocksdbExec {
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
            Err(DataFusionError::Execution(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let reader = RocksdbReader::new(
            self.global_context.clone(),
            self.table_def.clone(),
            self.batch_size,
            self.projection.clone(),
            self.filters.as_slice(),
        );

        Ok(Box::pin(RocksdbStream { reader}))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct RocksdbStream {
    reader: RocksdbReader,
}

impl Stream for RocksdbStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.reader.next())
    }
}

impl RecordBatchStream for RocksdbStream {
    fn schema(&self) -> SchemaRef {
        self.reader.projected_schema()
    }
}
