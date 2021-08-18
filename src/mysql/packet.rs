use byteorder::{LittleEndian};
use byteorder::WriteBytesExt;
use bytes::Bytes;

use datafusion::error::{Result};

use super::response::ResponsePayload;

pub struct PacketMessage {
    sequence_id: u8,
}

impl PacketMessage {
    pub fn new() -> PacketMessage {
        PacketMessage { sequence_id: 0 }
    }

    pub fn sequence_increase(&mut self) {
        self.sequence_id = self.sequence_id + 1;
        log::debug!("sequence_id: {:?}", self.sequence_id);
    }

    pub fn sequence_init(&mut self) {
        self.sequence_id = 0;
    }

    pub fn create(&mut self, response_payload: ResponsePayload) -> Result<Bytes> {
        // create header with length and sequence id
        let mut header: Vec<u8> = Vec::with_capacity(4);
        header
            .write_u32::<LittleEndian>(response_payload.bytes.len() as u32)
            .unwrap();
        header.pop(); // we need 3 byte length, so discard last byte
        header.push(self.sequence_id); // sequence_id

        //self.sequence_increase();

        let mut payload: Vec<u8> = Vec::with_capacity(header.len() + response_payload.bytes.len());
        payload.extend_from_slice(&header);
        payload.extend_from_slice(&response_payload.bytes);

        let mem = Bytes::from(payload);

        return Ok(mem);
    }
}


#[derive(Copy, Clone, Debug)]
pub enum PacketType {
    ComSleep = 0x00,
    ComQuit = 0x01,
    ComInitDb = 0x02,
    ComQuery = 0x03,
    ComFieldList = 0x04,
    ComCreateDb = 0x05,
    ComDropDb = 0x06,
    ComRefresh = 0x07,
    ComShutdown = 0x08,
    ComStatistics = 0x09,
    ComProcessInfo = 0x0a,
    ComConnect = 0x0b,
    ComProcessKill = 0x0c,
    ComDebug = 0x0d,
    ComPing = 0x0e,
    ComTime = 0x0f,
    ComDelayedInsert = 0x10,
    ComChangeUser = 0x11,
    ComBinlogDump = 0x12,
    ComTableDump = 0x13,
    ComConnectOut = 0x14,
    ComRegisterSlave = 0x15,
    ComStmtPrepare = 0x16,
    ComStmtExecute = 0x17,
    ComStmtSendLongData = 0x18,
    ComStmtClose = 0x19,
    ComStmtReset = 0x1a,
    ComDaemon = 0x1d,
    ComBinlogDumpGtid = 0x1e,
    ComResetConnection = 0x1f,
}
