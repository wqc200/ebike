use bstr::{ByteSlice};

use super::packet::PacketType;
use crate::mysql::error::{MysqlError, MysqlResult};

/// A payload is just a wrapper for a Vec<u8>
#[derive(Debug, PartialEq)]
pub struct RequestPayload {
    bytes: Vec<u8>,
}

impl RequestPayload {
    pub fn new(bytes: Vec<u8>) -> RequestPayload {
        RequestPayload { bytes }
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
            Ok(a) => {
                Ok(a)
            }
            Err(error) => {
                Err(MysqlError::new_global_error(1105, format!("Unknown error, Error reading SQL, error: {:?}.", error).as_str()))
            }
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
            _ => {
                Err(MysqlError::new_global_error(1105, format!("Unknown error, The packet type is not supported, packet type: {:?}.", a).as_str()))
            }
        }
    }
}
