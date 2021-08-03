use std::io::{self, Error, ErrorKind, Read, Write};
use bitflags;

use byteorder::{ReadBytesExt, WriteBytesExt};
use byteorder::{ByteOrder, LittleEndian};
use bstr::{ByteSlice, ByteVec, Utf8Error};

use arrow::array::ArrayBuilder;
use arrow::array::{StringArray, Array};

use super::packet::PacketType;
use super::metadata;
use super::mysql_util;
use datafusion::scalar::ScalarValue;
use parquet::data_type::AsBytes;

#[derive(Debug, PartialEq)]
pub struct ResponsePayload {
    pub bytes: Vec<u8>,
}

impl ResponsePayload {
    pub fn new(length: usize)  -> Self {
        let mut body: Vec<u8> = Vec::with_capacity(length);
        ResponsePayload { bytes: body }
    }

    pub fn dumpTextRow(&mut self, columns: Vec<ScalarValue>) {
        for column_index in 0..columns.len() {
            let scalar_value = columns[column_index].clone();
            match scalar_value {
                ScalarValue::Utf8(item) => {
                    if let Some(value) = item {
                        self.dumpLengthEncodedString(value.as_ref());
                    } else {
                        self.dumpLengthEncodedNull()
                    }
                }
                ScalarValue::Int32(item) => {
                    if let Some(value) = item {
                        self.dumpLengthEncodedString(value.to_string().as_ref());
                    } else {
                        self.dumpLengthEncodedNull()
                    }
                }
                ScalarValue::Int64(item) => {
                    if let Some(value) = item {
                        self.dumpLengthEncodedString(value.to_string().as_ref());
                    } else {
                        self.dumpLengthEncodedNull()
                    }
                }
                ScalarValue::UInt64(item) => {
                    if let Some(value) = item {
                        self.dumpLengthEncodedString(value.to_string().as_ref());
                    } else {
                        self.dumpLengthEncodedNull()
                    }
                }
                _ => {
                    let message = format!("unsupported scalar value type: {}", scalar_value.get_datatype().to_string());
                    log::error!("{}", message);
                    panic!(message)
                }
            }
        }
    }

    pub fn dumpLengthEncodedString(&mut self, msg: &[u8]) {
        self.dumpLengthEncodedInt(msg.len() as u64);
        self.bytes.extend_from_slice(msg);
    }

    pub fn dumpLengthEncodedNull(&mut self) {
        self.bytes.extend_from_slice(&[0xfb]);
    }

    pub fn dumpLengthEncodedInt(&mut self, n: u64) {
        if n <= 250 {
            self.bytes.push(n as u8)
        } else if n <= 0xffff {
            let a = n >> 8;
            self.bytes.extend_from_slice(&[0xfc, n as u8, a as u8])
        } else if n <= 0xffffff {
            let a = n >> 8;
            let b = n >> 16;
            self.bytes.extend_from_slice(&[0xfd, n as u8, a as u8, b as u8])
        } else if n <= 0xffffffffffffffff {
            let a = n >> 8;
            let b = n >> 16;
            let c = n >> 24;
            let d = n >> 32;
            let e = n >> 40;
            let f = n >> 48;
            let g = n >> 56;
            self.bytes.extend_from_slice(&[0xfe, n as u8, a as u8, b as u8, c as u8, d as u8, e as u8, f as u8, g as u8])
        }
    }

    pub fn dumpUint16(&mut self, n: u16) {
        let a = n >> 8;
        self.bytes.extend_from_slice(&[n as u8, a as u8])
    }

    pub fn dumpUint32(&mut self, n: u32) {
        let a = n >> 8;
        let b = n >> 16;
        let c = n >> 24;
        self.bytes.extend_from_slice(&[n as u8, a as u8, b as u8, c as u8])
    }

    pub fn dumpUint64(&mut self, n: u64) {
        let a = n >> 8;
        let b = n >> 16;
        let c = n >> 24;
        let d = n >> 32;
        let e = n >> 40;
        let f = n >> 48;
        let g = n >> 56;
        self.bytes.extend_from_slice(&[n as u8, a as u8, b as u8, c as u8, d as u8, e as u8, f as u8, g as u8])
    }

    pub fn dumpBinaryTime(&mut self, mut nanoseconds: i64) {
        if nanoseconds == 0 {
            self.bytes.push(0);
            return;
        }

        let mut body: Vec<u8> = Vec::with_capacity(13);
        // size
        body.push(12);
        //
        if nanoseconds < 0 { // minus
            body.push(1);
            nanoseconds = 0 - nanoseconds
        } else { // plus
            body.push(0);
        }
        // days
        let days = nanoseconds / (24 * 60 * 60 * 1000 * 1000 * 1000);
        body.push(days as u8);
        nanoseconds = nanoseconds - days * 24 * 60 * 60 * 1000 * 1000 * 1000;
        // space
        body.extend_from_slice(&[0, 0, 0]);
        // hours
        let hours = nanoseconds / (60 * 60 * 1000 * 1000 * 1000);
        body.push(hours as u8);
        nanoseconds = nanoseconds - hours * 60 * 60 * 1000 * 1000 * 1000;
        // minutes
        let minutes = nanoseconds / (60 * 1000 * 1000 * 1000);
        body.push(minutes as u8);
        nanoseconds = nanoseconds - minutes * 60 * 1000 * 1000 * 1000;
        // senconds
        let seconds = nanoseconds / (1000 * 1000 * 1000);
        body.push(seconds as u8);
        nanoseconds = nanoseconds - seconds * 1000 * 1000 * 1000;

        if nanoseconds == 0 {
            body[0] = 8;
            body.drain(9..);
        } else {
            let microseconds = nanoseconds / 1000;
            let mut buf = [0; 4];
            LittleEndian::write_u32(&mut buf, microseconds as u32);
            body.extend_from_slice(buf.as_ref());
        }
        self.bytes.extend_from_slice(body.as_slice());
    }
}

impl AsRef<[u8]> for ResponsePayload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes[..]
    }
}

impl Into<Vec<u8>> for ResponsePayload {
    fn into(self) -> Vec<u8> {
        self.bytes
    }
}

