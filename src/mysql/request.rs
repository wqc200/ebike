use bstr::ByteSlice;
use byteorder::{ByteOrder, LittleEndian};

use sqlparser::ast::{Expr as SQLExpr, Value};

use crate::mysql::error::{MysqlError, MysqlResult};
use crate::mysql::mysql_error_code;
use crate::mysql::mysql_type_code;
use crate::mysql::packet::PacketType;
use crate::mysql::mysql_util::parse_length_encoded_bytes;

/// A payload is just a wrapper for a Vec<u8>
#[derive(Debug, PartialEq)]
pub struct RequestPayload {
    bytes: Vec<u8>,
}

impl RequestPayload {
    pub fn new(bytes: Vec<u8>) -> RequestPayload {
        RequestPayload { bytes }
    }

    /// https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integerlet
    pub fn com_stmt_execute(&mut self) -> MysqlResult<CoreOutput> {
        let mut pos = 0;

        // stmt id
        let end = pos + 4;
        let slice = self.bytes[pos..end];
        if end > self.bytes.len() {
            return Err(MysqlError::new_global_error(
                mysql_error_code::CR_MALFORMED_PACKET as u16,
                format!("reading statement ID failed").as_str(),
            ));
        }
        let stmtID = LittleEndian::read_u32(&slice);
        pos += 4;

        // tmp
        let num_params = 2;

        // cursor type flag
        let cursor_type_flag = self.bytes[pos];
        pos += 1;

        // iteration-count, always 1
        let iteration_count_offset = 4;
        pos += iteration_count_offset;

        if num_params > 0 {
            // null bitmaps
            let nullBitmapLen = (num_params + 7) / 8;
            if self.bytes.len() < (pos + nullBitmapLen + 1) {
                return Err(MysqlError::new_global_error(
                    mysql_error_code::CR_MALFORMED_PACKET as u16,
                    format!("malformed packet error").as_str(),
                ));
            }
            let end = pos + nullBitmapLen;
            nullBitmaps = self.bytes[pos..end];
            pos += nullBitmapLen;

            let mut param_types:Vec<u8> = vec![];
            let mut param_values:Vec<u8> = vec![];

            // new params bound flag
            let newParamsBoundFlag = self.bytes[pos];
            pos += 1;
            if (newParamsBoundFlag == 0x01) {
                let length = num_params * 2;

                let start = pos;
                let end = pos + length;
                param_types = self.bytes[start..end].to_owned();
                pos += length;

                let start = pos;
                param_values = self.bytes[start..].to_owned();
            }

            let mut param_type_pos = 0;
            let mut param_value_pos = 0;
            for i in 0..num_params {
                if (nullBitmaps[i/8] & (1 << (i%8) as u64)) > 0 {
                    SQLExpr::Value(Value::Null);
                }

                let mysql_type = param_types[param_type_pos] as u64;
                param_type_pos += 1;

                let type_flag = param_types[param_type_pos];
                param_type_pos += 1;

                match mysql_type {
                    mysql_type_code::TYPE_NULL => {},
                    mysql_type_code::TYPE_LONG_LONG => {
                        SQLExpr::Value(Value::Number());
                    },
                    mysql_type_code::TYPE_VARCHAR => {
                        let result = parse_length_encoded_bytes(param_values[param_value_pos..].to_owned());
                        match result {
                            None => {
                                return Err(MysqlError::new_global_error(
                                    mysql_error_code::CR_MALFORMED_PACKET as u16,
                                    format!("malformed packet error").as_str(),
                                ));
                            }
                            Some(bytes) => {
                                let val = match bytes.to_str() {
                                    Ok(val) => val.to_string(),
                                    Err(err) => {
                                        log::error!("Unknown error, Error reading REQUEST, error: {:?}", err);
                                        break;
                                    }
                                };

                                SQLExpr::Value(Value::SingleQuotedString(val));
                            }
                        }
                    },
                    _ => {}
                }
            }
        }

        Ok()
    }

    pub fn get_sequence_id(&self) -> u8 {
        self.bytes[3]
    }

    pub fn get_query_sql(&self) -> &[u8] {
        let a = self.bytes[0] as u32;
        let b = self.bytes[1] as u32;
        let c = self.bytes[2] as u32;

        let len = a | b << 8 | c << 16;
        let start = 1 as usize + 4;
        let end = len as usize + 4;
        let slice = &self.bytes[start..end];
        slice
    }

    pub fn get_query_sql2(&self) -> MysqlResult<&str> {
        let a = self.bytes[0] as u32;
        let b = self.bytes[1] as u32;
        let c = self.bytes[2] as u32;

        let len = a | b << 8 | c << 16;
        let start = 1 as usize + 4;
        let end = len as usize + 4;
        let slice = &self.bytes[start..end];

        let sql = slice.to_str();
        match sql {
            Ok(a) => Ok(a),
            Err(error) => Err(MysqlError::new_global_error(
                1105,
                format!("Unknown error, Error reading SQL, error: {:?}.", error).as_str(),
            )),
        }
    }

    pub fn get_command_id(&self) -> u8 {
        self.bytes[4]
    }

    pub fn get_packet_type(&self) -> MysqlResult<PacketType> {
        let a = self.bytes[4];
        match a {
            0x00 => Ok(PacketType::ComSleep),
            0x01 => Ok(PacketType::ComQuit),
            0x02 => Ok(PacketType::ComInitDb),
            0x03 => Ok(PacketType::ComQuery),
            0x04 => Ok(PacketType::ComFieldList),
            0x05 => Ok(PacketType::ComCreateDb),
            0x06 => Ok(PacketType::ComDropDb),
            0x07 => Ok(PacketType::ComRefresh),
            0x08 => Ok(PacketType::ComShutdown),
            0x09 => Ok(PacketType::ComStatistics),
            0x0a => Ok(PacketType::ComProcessInfo),
            0x0b => Ok(PacketType::ComConnect),
            0x0c => Ok(PacketType::ComProcessKill),
            0x0d => Ok(PacketType::ComDebug),
            0x0e => Ok(PacketType::ComPing),
            0x0f => Ok(PacketType::ComTime),
            0x10 => Ok(PacketType::ComDelayedInsert),
            0x11 => Ok(PacketType::ComChangeUser),
            0x12 => Ok(PacketType::ComBinlogDump),
            0x13 => Ok(PacketType::ComTableDump),
            0x14 => Ok(PacketType::ComConnectOut),
            0x15 => Ok(PacketType::ComRegisterSlave),
            0x16 => Ok(PacketType::ComStmtPrepare),
            0x17 => Ok(PacketType::ComStmtExecute),
            0x18 => Ok(PacketType::ComStmtSendLongData),
            0x19 => Ok(PacketType::ComStmtClose),
            0x1a => Ok(PacketType::ComStmtReset),
            0x1d => Ok(PacketType::ComDaemon),
            0x1e => Ok(PacketType::ComBinlogDumpGtid),
            0x1f => Ok(PacketType::ComResetConnection),
            _ => Err(MysqlError::new_global_error(
                1105,
                format!(
                    "Unknown error, The packet type is not supported, packet type: {:?}.",
                    a
                )
                .as_str(),
            )),
        }
    }
}
