use byteorder::{LittleEndian};
use byteorder::WriteBytesExt;
use arrow::datatypes::{Field};

use arrow::array::ArrayBuilder;
use arrow::array::{
    StringArray,
};
use arrow::record_batch::RecordBatch;

use super::metadata::Column;
use super::response::ResponsePayload;
use super::{mysql_util, metadata};
use parquet::data_type::AsBytes;
use datafusion::scalar::ScalarValue;

pub enum MessageType {
    ResponsePayload(ResponsePayload),
    ResultSet(Vec<RecordBatch>),
    MultiResultSet(Vec<MessageType>),
}

pub fn row_message(columns: Vec<ScalarValue>) -> ResponsePayload {
    let mut payload = ResponsePayload::new(1024);
    payload.dumpTextRow(columns);
    return payload;
}

pub fn column_count_message(count: usize) -> ResponsePayload {
    let mut payload = ResponsePayload::new(1024);
    payload.dumpLengthEncodedInt(count as u64);
    return payload;
}

pub fn column_definition_message(field: &Field) -> ResponsePayload {
    let column = Column::from(field);
    let mut payload = column.into();
    return payload;
}

///
/// https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
///
pub fn ok_message(affect_rows: u64, last_insert_id: u64, statusFlags: metadata::StatusFlags, warning_count: u16, msg: String) -> ResponsePayload {
    let status = u16::from(statusFlags);
    let enclen = mysql_util::length_encoded_int_size(msg.len() as u64) + msg.len() as i32;

    let mut payload = ResponsePayload::new(32 + enclen as usize);
    payload.bytes.push(0x00);
    payload.dumpLengthEncodedInt(affect_rows);
    payload.dumpLengthEncodedInt(last_insert_id);
    payload.dumpUint16(status);
    payload.dumpUint16(warning_count);
    if enclen > 0 {
        payload.dumpLengthEncodedString(msg.as_bytes());
    }

    // let mut body: Vec<u8> = Vec::with_capacity(32);
    // body.push(0x00); //
    // body.extend_from_slice(&[0, 0]); //
    // // status
    // // autocommit
    // let e: u16 = 2;
    // let f: u16 = 0;
    // body.extend_from_slice(&[e as u8, f as u8]);
    // body.extend_from_slice(&[0, 0]); //
    //
    // payload.bytes = body;

    return payload;
}

pub fn error_message(code: u16, state: &str, msg: &str) -> ResponsePayload {
    let mut payload = ResponsePayload::new(9 + msg.len());
    payload.bytes.push(0xff); // packet type
    payload.bytes.write_u16::<LittleEndian>(code).unwrap();
    payload.bytes.extend_from_slice("#".as_bytes());
    payload.bytes.extend_from_slice(state.as_bytes());
    payload.bytes.extend_from_slice(msg.as_bytes());
    return payload;
}

///
/// https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
///
pub fn eof_message(warning_count: u16, status: u16) -> ResponsePayload {
    // start building payload
    let mut payload = ResponsePayload::new(5);
    payload.bytes.push(0xfe); // packet type
    payload.dumpUint16(warning_count);
    payload.dumpUint16(status);
    return payload;
}

pub fn handshark_auth_switch_request() -> ResponsePayload {
    let mut payload = ResponsePayload::new(128);
    // fe
    payload.bytes.push(0xfe);
    // plugin name
    payload.bytes.extend_from_slice("mysql_native_password".as_bytes());
    // auth plugin data
    payload.bytes.push(0x00);
    // salt
    payload.bytes.extend_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14]);
    // filler [00]
    payload.bytes.push(0);

    return payload;

}

pub fn handshark_message() -> ResponsePayload {
    let a = 181211;
    let b = 7078;
    let c = 27;
    let d = 0;
    // start building payload
    let mut payload = ResponsePayload::new(128);
    payload.bytes.push(10); //
    payload.bytes.extend_from_slice("8.0.25".as_bytes()); //
    // filler [00]
    payload.bytes.push(0); //
    // connection id
    payload.bytes.extend_from_slice(&[0x0b, 0x00, 0x00, 0x00]);
    // auth-plugin-data-part-1
    payload.bytes.extend_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    // filler [00]
    payload.bytes.push(0);
    // capability flag lower 2 bytes, using default capability here
    payload.bytes.extend_from_slice(&[a as u8, b as u8]);
    // charset
    payload.bytes.push(46);
    // status
    // autocommit
    let e: u16 = 2;
    let f: u16 = 0;
    payload.bytes.extend_from_slice(&[e as u8, f as u8]);
    // below 13 byte may not be used
    // capability flag upper 2 bytes, using default capability here
    payload.bytes.extend_from_slice(&[c as u8, d as u8]);
    // length of auth-plugin-data
    payload.bytes.push(0x15);
    // reserved 10 [00]
    payload.bytes.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    // auth-plugin-data-part-2
    payload.bytes.extend_from_slice(&[0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14]);
    // filler [00]
    payload.bytes.push(0);
    // auth-plugin name
    payload.bytes.extend_from_slice("mysql_native_password".as_bytes());
    // filler [00]
    payload.bytes.push(0);

    return payload;
}
