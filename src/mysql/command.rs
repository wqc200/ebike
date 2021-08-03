// #![feature(async_await)]
// #![feature(async_closure)]
//
// use std::sync::{Arc, Mutex};
// use std::collections::HashMap;
// use byteorder::{ByteOrder, LittleEndian};
// use byteorder::{ReadBytesExt, WriteBytesExt};
// use bytes::Bytes;
//
// use tokio::net::{TcpListener, TcpStream};
//
// use futures::SinkExt;
// use std::env;
// use std::error::Error;
//
// use tokio::prelude::*;
// use tokio_util::codec::{Framed, LinesCodec};
// use tokio_util::codec::{BytesCodec, Decoder};
// use tokio::stream::StreamExt;
// use tokio::{
//     io::{AsyncBufRead, AsyncRead, AsyncWrite},
//     stream::Stream,
// };
// use std::io;
// use std::net::SocketAddr;
// use std::pin::Pin;
// use std::task::{Context, Poll};
//
// use arrow::datatypes::{DataType, Field, Schema, ToByteSlice};
// use arrow::record_batch::RecordBatch;
// use arrow::array::{
//     ArrayData,
//     BinaryArray,
//     Int8Array,
//     Int16Array,
//     Int32Array,
//     Int64Array,
//     UInt8Array,
//     UInt16Array,
//     UInt32Array,
//     UInt64Array,
//     Float32Array,
//     Float64Array,
//     StringArray,
// };
// use arrow::compute::cast;
// use arrow::datatypes::DataType::UInt8;
// use arrow::buffer::Buffer;
//
// use datafusion::datasource::{CsvFile, MemTable, TableProvider};
// use datafusion::error::{ExecutionError, Result};
// use datafusion::execution;
// use datafusion::logical_plan::Operator;
// use datafusion::scalar::ScalarValue;
//
// use crate::datafusion_impl::datasource::rocksdb::RocksdbTable;
// use crate::core::output::CoreOutput;
// use crate::core::execution::CoreExecutionContext;
// use crate::core::context::CoreContext;
// //use utils::dbkey;
// //use crate::datafusion_impl::datasource::rocksdb::RocksdbTable;
//
// use super::request::RequestPayload;
// use super::response::ResponsePayload;
// use super::packet;
// use super::request;
// use super::message;
// use super::util;
//
// pub struct Request {
//     core_context: CoreContext,
//     core_execution_context: CoreExecutionContext,
// }
//
// impl Request
// {
//     pub fn Query(&mut self, sql: &str) -> Result<CoreOutput> {
//         let mut cectx = self.core_execution_context;
//         let core_logical_plan = cectx.create_logical_plan(sql)?;
//         let core_logical_plan = cectx.optimize(&core_logical_plan)?;
//         let core_physical_plan = cectx.create_physical_plan(&core_logical_plan)?;
//         let result = cectx.execution(&core_physical_plan);
//         result
//     }
//
//     pub fn new() -> Self {
//         let core_context = Arc::new(CoreContext::new());
//         let core_execution_context = CoreExecutionContext::new(core_context.clone());
//         Request {
//             core_context,
//             core_execution_context,
//         }
//     }
// }