use bitflags;
use serde_derive::{Deserialize, Serialize};

use arrow::datatypes::{DataType, Field, Schema, ToByteSlice};
use arrow::array::{
    ArrayData,
    BinaryArray,
    Int8Array,
    Int16Array,
    Int32Array,
    Int64Array,
    UInt8Array,
    UInt16Array,
    UInt32Array,
    UInt64Array,
    Float32Array,
    Float64Array,
    StringArray,
};

use super::response::ResponsePayload;
use crate::mysql::metadata::MysqlType::MYSQL_TYPE_STRING;
use num::traits::AsPrimitive;
use crate::mysql::mysql_util;
use crate::meta::def::SparrowColumnDef;
use crate::meta::{meta_util, meta_const};
use sqlparser::ast::{ColumnOption, ObjectName};

pub enum ArrayCell<'a> {
    StringArray(&'a StringArray),
    Int8Array(&'a Int8Array),
    Int16Array(&'a Int16Array),
    Int32Array(&'a Int32Array),
    Int64Array(&'a Int64Array),
    UInt8Array(&'a UInt8Array),
    UInt16Array(&'a UInt16Array),
    UInt32Array(&'a UInt32Array),
    UInt64Array(&'a UInt64Array),
    Float32Array(&'a Float32Array),
    Float64Array(&'a Float64Array),
}

#[derive(Serialize, Deserialize)]
pub struct Column {
    schema: String,
    table: String,
    org_table: String,
    name: String,
    org_name: String,
    character_set: u8,
    column_length: u64,
    column_type: MysqlType,
    flags: ColumnFlags,
    decimals: u8,
    default_value: Option<String>,
}

impl Column {
    pub fn new(schema_name: ObjectName, table_name: ObjectName, column_def: &SparrowColumnDef) -> Column {
        //let schema_name = schema_name.to_string();
        //let table_name = table_name.to_string();
        let column_name = &column_def.sql_column.name.value.clone();

        let arrow_data_type = meta_util::convert_sql_data_type_to_arrow_data_type(&column_def.sql_column.data_type).unwrap();
        let mysql_type = mysql_util::convert_arrow_data_type_to_mysql_type(&arrow_data_type).unwrap();

        let nullable = column_def.sql_column.options
            .iter()
            .any(|x| x.option == ColumnOption::Null);
        let flags = if nullable {
            ColumnFlags::NO_DEFAULT_VALUE_FLAG
        } else {
            ColumnFlags::NOT_NULL_FLAG
        };

        let column_length = 100000;

        Column {
            schema: schema_name.to_string(),
            table: table_name.to_string(),
            org_table: table_name.to_string(),
            name: column_name.to_string(),
            org_name: column_name.to_string(),
            character_set: 46,
            column_length: column_length,
            column_type: mysql_type,
            flags,
            decimals: 8,
            default_value: None,
        }
    }
}

impl From<&Field> for Column {
    fn from(field: &Field) -> Self {
        let column_name = field.name();
        let mysql_type = mysql_util::convert_arrow_data_type_to_mysql_type(field.data_type()).unwrap();

        let flags = if field.is_nullable() {
            ColumnFlags::NO_DEFAULT_VALUE_FLAG
        } else {
            ColumnFlags::NOT_NULL_FLAG
        };

        Self {
            schema: "".to_string(),
            table: "SCHEMATA".to_string(),
            org_table: "schemata".to_string(),
            name: column_name.to_string(),
            org_name: column_name.to_string(),
            character_set: 33,
            column_length: 15,
            column_type: mysql_type,
            flags,
            decimals: 8,
            default_value: None,
        }
    }
}

impl Into<ResponsePayload> for Column {
    fn into(self) -> ResponsePayload {
        let mut payload = ResponsePayload::new(1024);
        payload.dumpLengthEncodedString(meta_const::CATALOG_NAME.as_bytes());
        payload.dumpLengthEncodedString(self.schema.as_ref());
        payload.dumpLengthEncodedString(self.table.as_ref());
        payload.dumpLengthEncodedString(self.org_table.as_ref());
        payload.dumpLengthEncodedString(self.name.as_ref());
        payload.dumpLengthEncodedString(self.org_name.as_ref());
        payload.bytes.push(0x0c);
        payload.dumpUint16(self.character_set as u16);
        payload.dumpUint32(self.column_length as u32);
        payload.bytes.push(dumpColumnType(self.column_type));
        payload.dumpUint16(dumpFlag(self.column_type, self.flags.bits as u16));
        payload.bytes.push(self.decimals);
        payload.bytes.extend_from_slice(&[0, 0]);

        /// https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition
        /// if command was COM_FIELD_LIST
        match self.default_value {
            Some(ref p) => {
                payload.dumpUint64(p.len() as u64);
                payload.bytes.extend_from_slice(p.as_ref());
            }
            None => {}
        }

        return payload;
    }
}

fn dumpFlag(tp: MysqlType, flag: u16) -> u16 {
    return match tp {
        MysqlType::MYSQL_TYPE_SET => {
            flag | u16::from(ColumnFlags::SET_FLAG)
        }
        MysqlType::MYSQL_TYPE_ENUM => {
            flag | u16::from(ColumnFlags::ENUM_FLAG)
        }
        _ => {
            if hasBinaryFlag(flag) {
                return flag | u16::from(ColumnFlags::NOT_NULL_FLAG);
            }
            flag
        }
    };
}

fn dumpColumnType(tp: MysqlType) -> u8 {
    return match tp {
        MysqlType::MYSQL_TYPE_SET => MysqlType::MYSQL_TYPE_STRING as u8,
        MysqlType::MYSQL_TYPE_ENUM => MysqlType::MYSQL_TYPE_STRING as u8,
        _ => {
            tp as u8
        }
    };
}

fn hasBinaryFlag(flag: u16) -> bool {
    return (flag & u16::from(ColumnFlags::BINARY_FLAG)) > 0;
}

bitflags! {
    /// MySql column flags
    #[derive(Serialize, Deserialize)]
    pub struct ColumnFlags: u16 {
        /// Field can't be NULL.
        const NOT_NULL_FLAG         = 1u16;

        /// Field is part of a primary key.
        const PRI_KEY_FLAG          = 2u16;

        /// Field is part of a unique key.
        const UNIQUE_KEY_FLAG       = 4u16;

        /// Field is part of a key.
        const MULTIPLE_KEY_FLAG     = 8u16;

        /// Field is a blob.
        const BLOB_FLAG             = 16u16;

        /// Field is unsigned.
        const UNSIGNED_FLAG         = 32u16;

        /// Field is zerofill.
        const ZEROFILL_FLAG         = 64u16;

        /// Field is binary.
        const BINARY_FLAG           = 128u16;

        /// Field is an enum.
        const ENUM_FLAG             = 256u16;

        /// Field is a autoincrement field.
        const AUTO_INCREMENT_FLAG   = 512u16;

        /// Field is a timestamp.
        const TIMESTAMP_FLAG        = 1024u16;

        /// Field is a set.
        const SET_FLAG              = 2048u16;

        /// Field doesn't have default value.
        const NO_DEFAULT_VALUE_FLAG = 4096u16;

        /// Field is set to NOW on UPDATE.
        const ON_UPDATE_NOW_FLAG    = 8192u16;

        /// Intern; Part of some key.
        const PART_KEY_FLAG         = 16384u16;

        /// Field is num (for clients).
        const NUM_FLAG              = 32768u16;
    }
}

impl From<ColumnFlags> for u16 {
    fn from(cf: ColumnFlags) -> u16 {
        cf.bits as u16
    }
}


/// Type of MySql column field
#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Debug)]
#[repr(u8)]
pub enum MysqlType {
    MYSQL_TYPE_DECIMAL = 0,
    MYSQL_TYPE_TINY,
    MYSQL_TYPE_SHORT,
    MYSQL_TYPE_LONG,
    MYSQL_TYPE_FLOAT,
    MYSQL_TYPE_DOUBLE,
    MYSQL_TYPE_NULL,
    MYSQL_TYPE_TIMESTAMP,
    MYSQL_TYPE_LONGLONG,
    MYSQL_TYPE_INT24,
    MYSQL_TYPE_DATE,
    MYSQL_TYPE_TIME,
    MYSQL_TYPE_DATETIME,
    MYSQL_TYPE_YEAR,
    MYSQL_TYPE_NEWDATE,
    // Internal to MySql
    MYSQL_TYPE_VARCHAR,
    MYSQL_TYPE_BIT,
    MYSQL_TYPE_TIMESTAMP2,
    MYSQL_TYPE_DATETIME2,
    MYSQL_TYPE_TIME2,
    MYSQL_TYPE_JSON = 245,
    MYSQL_TYPE_NEWDECIMAL = 246,
    MYSQL_TYPE_ENUM = 247,
    MYSQL_TYPE_SET = 248,
    MYSQL_TYPE_TINY_BLOB = 249,
    MYSQL_TYPE_MEDIUM_BLOB = 250,
    MYSQL_TYPE_LONG_BLOB = 251,
    MYSQL_TYPE_BLOB = 252,
    MYSQL_TYPE_VAR_STRING = 253,
    MYSQL_TYPE_STRING = 254,
    MYSQL_TYPE_GEOMETRY = 255,
}

impl From<u8> for MysqlType {
    fn from(x: u8) -> MysqlType {
        match x {
            0x00_u8 => MysqlType::MYSQL_TYPE_DECIMAL,
            0x01_u8 => MysqlType::MYSQL_TYPE_TINY,
            0x02_u8 => MysqlType::MYSQL_TYPE_SHORT,
            0x03_u8 => MysqlType::MYSQL_TYPE_LONG,
            0x04_u8 => MysqlType::MYSQL_TYPE_FLOAT,
            0x05_u8 => MysqlType::MYSQL_TYPE_DOUBLE,
            0x06_u8 => MysqlType::MYSQL_TYPE_NULL,
            0x07_u8 => MysqlType::MYSQL_TYPE_TIMESTAMP,
            0x08_u8 => MysqlType::MYSQL_TYPE_LONGLONG,
            0x09_u8 => MysqlType::MYSQL_TYPE_INT24,
            0x0a_u8 => MysqlType::MYSQL_TYPE_DATE,
            0x0b_u8 => MysqlType::MYSQL_TYPE_TIME,
            0x0c_u8 => MysqlType::MYSQL_TYPE_DATETIME,
            0x0d_u8 => MysqlType::MYSQL_TYPE_YEAR,
            0x0f_u8 => MysqlType::MYSQL_TYPE_VARCHAR,
            0x10_u8 => MysqlType::MYSQL_TYPE_BIT,
            0x11_u8 => MysqlType::MYSQL_TYPE_TIMESTAMP2,
            0x12_u8 => MysqlType::MYSQL_TYPE_DATETIME2,
            0x13_u8 => MysqlType::MYSQL_TYPE_TIME2,
            0xf5_u8 => MysqlType::MYSQL_TYPE_JSON,
            0xf6_u8 => MysqlType::MYSQL_TYPE_NEWDECIMAL,
            0xf7_u8 => MysqlType::MYSQL_TYPE_ENUM,
            0xf8_u8 => MysqlType::MYSQL_TYPE_SET,
            0xf9_u8 => MysqlType::MYSQL_TYPE_TINY_BLOB,
            0xfa_u8 => MysqlType::MYSQL_TYPE_MEDIUM_BLOB,
            0xfb_u8 => MysqlType::MYSQL_TYPE_LONG_BLOB,
            0xfc_u8 => MysqlType::MYSQL_TYPE_BLOB,
            0xfd_u8 => MysqlType::MYSQL_TYPE_VAR_STRING,
            0xfe_u8 => MysqlType::MYSQL_TYPE_STRING,
            0xff_u8 => MysqlType::MYSQL_TYPE_GEOMETRY,
            _ => panic!("Unknown column type {:?}", x),
        }
    }
}

bitflags! {
    /// MySql server status flags
    pub struct StatusFlags: u16 {
        /// Is raised when a multi-statement transaction has been started, either explicitly,
        /// by means of BEGIN or COMMIT AND CHAIN, or implicitly, by the first transactional
        /// statement, when autocommit=off.
        const SERVER_STATUS_IN_TRANS             = 0x0001;

        /// Server in auto_commit mode.
        const SERVER_STATUS_AUTOCOMMIT           = 0x0002;

        /// Multi query - next query exists.
        const SERVER_MORE_RESULTS_EXISTS         = 0x0008;

        const SERVER_STATUS_NO_GOOD_INDEX_USED   = 0x0010;

        const SERVER_STATUS_NO_INDEX_USED        = 0x0020;

        /// The server was able to fulfill the clients request and opened a read-only
        /// non-scrollable cursor for a query. This flag comes in reply to COM_STMT_EXECUTE
        /// and COM_STMT_FETCH commands. Used by Binary Protocol Resultset to signal that
        /// COM_STMT_FETCH must be used to fetch the row-data.
        const SERVER_STATUS_CURSOR_EXISTS        = 0x0040;

        /// This flag is sent when a read-only cursor is exhausted, in reply to
        /// COM_STMT_FETCH command.
        const SERVER_STATUS_LAST_ROW_SENT        = 0x0080;

        /// A database was dropped.
        const SERVER_STATUS_DB_DROPPED           = 0x0100;

        const SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200;

        /// Sent to the client if after a prepared statement reprepare we discovered
        /// that the new statement returns a different number of result set columns.
        const SERVER_STATUS_METADATA_CHANGED     = 0x0400;

        const SERVER_QUERY_WAS_SLOW              = 0x0800;

        /// To mark ResultSet containing output parameter values.
        const SERVER_PS_OUT_PARAMS               = 0x1000;

        /// Set at the same time as SERVER_STATUS_IN_TRANS if the started multi-statement
        /// transaction is a read-only transaction. Cleared when the transaction commits
        /// or aborts. Since this flag is sent to clients in OK and EOF packets, the flag
        /// indicates the transaction status at the end of command execution.
        const SERVER_STATUS_IN_TRANS_READONLY    = 0x2000;

        /// This status flag, when on, implies that one of the state information has
        /// changed on the server because of the execution of the last statement.
        const SERVER_SESSION_STATE_CHANGED       = 0x4000;
    }
}

impl From<StatusFlags> for u16 {
    fn from(cf: StatusFlags) -> u16 {
        cf.bits as u16
    }
}
