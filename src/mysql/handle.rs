#![warn(rust_2018_idioms)]

use bytes::{BytesMut, Bytes, Buf};

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow::array::ArrayBuilder;
use arrow::array::{as_primitive_array, as_string_array};
use arrow::array::{Array, ArrayData, BinaryArray, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array, Float32Array, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, ToByteSlice};
use arrow::record_batch::RecordBatch;
use arrow::compute::cast;
use arrow::datatypes::DataType::UInt8;
use arrow::buffer::Buffer;

use datafusion::datasource::{CsvFile, MemTable, TableProvider};
use datafusion::error::{Result, DataFusionError};
use datafusion::execution;
use datafusion::logical_plan::Operator;
use datafusion::scalar::ScalarValue;
use futures::{SinkExt, Future};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, BytesCodec, Decoder};

use crate::core::global_context::GlobalContext;
use crate::core::execution::Execution;
use crate::core::output::{CoreOutput, OutputError};
use crate::core::output::FinalCount;
use crate::datafusion_impl::datasource::rocksdb::RocksdbTable;
use crate::mysql::{command, error::MysqlError, packet, request, response, message, metadata};
use crate::mysql::error::MysqlResult;
use crate::core::core_util;
use bstr::ByteSlice;
use crate::mysql::request::RequestPayload;
use crate::mysql::metadata::Column;

/// The state for each connected client.
pub struct Handle {
    core_context: Arc<Mutex<GlobalContext>>,
    socket: TcpStream,
    packet_message: packet::PacketMessage,
    core_execution: Execution,
}

impl Handle {
    /// Create a new instance of `Peer`.
    pub async fn new(
        socket: TcpStream,
        core_context: Arc<Mutex<GlobalContext>>,
    ) -> io::Result<Handle> {
        let mut core_execution = Execution::new(core_context.clone());
        let packet_message = packet::PacketMessage::new();
        Ok(Handle { socket, packet_message, core_context: core_context.clone(), core_execution })
    }

    pub fn payload_packet(&mut self, buf: &[u8]) -> request::RequestPayload {
        log::debug!("buf: {:?}", buf);
        let rp = request::RequestPayload::new(buf.to_vec());
        self.packet_message.sequence_increase();

        rp
    }

    pub async fn write_packet(&mut self, response_payload: response::ResponsePayload) {
        let mem = self.packet_message.create(response_payload).unwrap();
        log::debug!("write packet bytes: {:?}", mem);

        if let Err(e) = self.socket.write_all(mem.bytes()).await {
            log::debug!("error on sending response, error: {:?}", e);
        }

        self.packet_message.sequence_increase();
    }

    pub async fn run(&mut self) {
        let result = self.handshake().await;
        if let Err(mysql_error) = result {
            self.write_packet_error(mysql_error).await;
            return;
        }
        self.exec_command().await;
    }

    // pub async fn read_packet(&mut self) -> MysqlResult<Option<&[u8]>> {
    //     let mut buf = [0; 1024];
    //     let n = match self.socket.read(&mut buf).await {
    //         Ok(n) if n == 0 => return Ok(None),
    //         Ok(n) => n,
    //         Err(error) => {
    //             return Err(MysqlError::new_global_error(1105, format!("Unknown error. Failed to read from socket, error: {:?}", error).as_str()));
    //         }
    //     };
    //     let bytes = &buf[0..n];
    //
    //     Ok(Some(bytes))
    // }

    pub async fn handshake(&mut self) -> MysqlResult<()> {
        self.write_packet(message::handshark_message()).await;

        let mut buf = [0; 1024];
        let n = match self.socket.read(&mut buf).await {
            Ok(n) if n == 0 => return Ok(()),
            Ok(n) => n,
            Err(error) => {
                return Err(MysqlError::new_global_error(1105, format!("Unknown error. Failed to read from socket, error: {:?}", error).as_str()));
            }
        };
        let bytes = &buf[0..n];
        let rp = self.payload_packet(bytes);

        self.write_packet(message::handshark_auth_switch_request()).await;

        let mut buf = [0; 1024];
        let n = match self.socket.read(&mut buf).await {
            Ok(n) if n == 0 => return Ok(()),
            Ok(n) => n,
            Err(error) => {
                return Err(MysqlError::new_global_error(1105, format!("Unknown error. Failed to read from socket, error: {:?}", error).as_str()));
            }
        };
        let bytes = &buf[0..n];
        let rp = self.payload_packet(bytes);

        let result = self.core_execution.try_init();
        if let Err(mysql_error) = result {
            return Err(mysql_error);
        }

        let ok_message = message::ok_message(0, 0, metadata::StatusFlags::SERVER_STATUS_AUTOCOMMIT, 0, "success".to_string());
        self.write_packet(ok_message).await;

        self.packet_message.sequence_init();

        Ok(())
    }

    pub async fn write_packet_error(&mut self, mysql_error: MysqlError) {
        let payload = message::error_message(mysql_error.error_number(), mysql_error.sql_state().as_str(), mysql_error.message().as_str());
        self.write_packet(payload).await;
    }

    pub async fn exec_command(&mut self) {
        let mut buf = [0; 10240];

        loop {
            let n = match self.socket.read(&mut buf).await {
                Ok(n) if n == 0 => break,
                Ok(n) => n,
                Err(e) => {
                    log::error!("failed to read from socket; err = {:?}", e);
                    break;
                }
            };
            let bytes = &buf[0..n];

            let request_payload = self.payload_packet(bytes);
            let sql = match request_payload.get_query_sql().to_str() {
                Ok(sql) => {
                    sql.to_string()
                }
                Err(e) => {
                    log::error!("Unknown error, Error reading SQL, error: {:?}", e);
                    break;
                }
            };
            log::debug!("sql: {}", sql);

            let command_id = request_payload.get_command_id();
            log::debug!("command id: {}", command_id);

            let result = match command_id {
                0x01 => {
                    /// quit
                    break;
                }
                0x02 => {
                    /// ComInitDb
                    self.core_execution.set_default_schema(sql.as_str()).await
                }
                0x03 => {
                    /// ComQuery
                    self.core_execution.execute_query(sql.as_str()).await
                }
                0x04 => {
                    /// ComFieldList
                    let table_name = sql.trim_end_matches("\x00").to_string();
                    self.core_execution.field_list(table_name.as_str()).await
                }
                _ => {
                    log::error!("Unknown error. The command is not support, command id: {:?}", command_id.to_string());
                    break;
                }
            };

            match result {
                Ok(core_output) => {
                    self.send_message(core_output).await;
                }
                Err(mysql_error) => {
                    self.write_packet_error(mysql_error).await;
                }
            }

            self.packet_message.sequence_init();
        }

        log::debug!("loop break");
    }

    async fn send_message(&mut self, core_output: CoreOutput) {
        match core_output {
            CoreOutput::FinalCount(FinalCount { affect_rows, last_insert_id, message }) => {
                let ok_message = message::ok_message(affect_rows, last_insert_id, metadata::StatusFlags::SERVER_STATUS_AUTOCOMMIT, 0, message);
                self.write_packet(ok_message).await;
            }
            CoreOutput::ResultSet(schema_ref, results) => {
                let mut payload = message::column_count_message(schema_ref.fields().len());
                self.write_packet(payload).await;
                for field in schema_ref.fields() {
                    self.write_packet(message::column_definition_message(field)).await;
                }
                self.write_packet(message::eof_message(0, 0)).await;

                for record_batch in results {
                    let rows = core_util::convert_record_to_scalar_value(record_batch.clone());
                    for row_index in 0..record_batch.num_rows() {
                        let payload = message::row_message(rows.get(row_index).unwrap().clone());
                        self.write_packet(payload).await;
                    }
                }
                self.write_packet(message::eof_message(0, 0)).await;
            }
            CoreOutput::ComFieldList(schema_name, table_name, table_def) => {
                let columns = table_def.get_columns();
                for column_def in columns {
                    let column = Column::new(schema_name.clone(), table_name.clone(), column_def);
                    let payload = column.into();
                    self.write_packet(payload).await;
                }
                self.write_packet(message::eof_message(0, 0)).await;
            }
            _ => {}
        }
    }
}

