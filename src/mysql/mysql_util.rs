
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{Expr as SQLExpr, Value};

use crate::ArrowDataType;
use crate::MysqlType;
use crate::mysql::error::{MysqlResult, MysqlError};
use crate::core::output::OutputError::MysqlError;


pub fn length_encoded_int_size(n: u64) -> i32 {
    if n <= 250 {
        return 1;
    } else if n <= 0xffff {
        return 3;
    } else if n <= 0xffffff {
        return 4;
    }

    return 9;
}

/// convert arrow data type to mysql type
/// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
/// https://dev.mysql.com/doc/internals/en/binary-protocol-value.html#packet-ProtocolBinary::MYSQL_TYPE_LONG
///
pub fn convert_arrow_data_type_to_mysql_type(data_type: &ArrowDataType) -> MysqlResult<MysqlType> {
    match data_type {
        ArrowDataType::Int8 => Ok(MysqlType::MYSQL_TYPE_TINY),
        ArrowDataType::Int16 => Ok(MysqlType::MYSQL_TYPE_SHORT),
        ArrowDataType::Int32 => Ok(MysqlType::MYSQL_TYPE_LONG),
        ArrowDataType::Int64 => Ok(MysqlType::MYSQL_TYPE_LONGLONG),
        ArrowDataType::Utf8 => Ok(MysqlType::MYSQL_TYPE_STRING),
        _ => Ok(MysqlType::MYSQL_TYPE_STRING),
    }
}


pub fn parse_exec_args(sc *stmtctx.StatementContext, boundParams [][]byte,
                       nullBitmap: Vec<u8>, paramTypes, paramValues []byte, enc *inputDecoder) -> MysqlResult<()> {
    let mut args = vec![];
    for i in 0..2 {
        if nullBitmap[i>>3]&(1<<((i as i64)%8)) > 0 {
            args[i] = SQLExpr::Value(Value::Null);
            continue
        }


    }

    Ok(())
}

pub fn parse_length_encoded_bytes(bytes: Vec<u8>) -> Option<Vec<u8>> {
    let result = parse_length_encoded_int(bytes);
    match result {
        None => {}
        Some((len, start_pos)) => {
            let end_pos = start_pos + len;

            if bytes.len() <= (end_pos - 1) as usize {
                return None
            }

            return Some((bytes[start_pos..end_pos].to_owned()));
        }
    }

    return None
}

/// https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
pub fn parse_length_encoded_int(bytes: Vec<u8>) -> Option<(u64, i32)> {
    if bytes.len() < 1 {
        return None
    }

    let mut len = 0;
    let mut start_pos = 0;

    match bytes[0] {
        0xfc => {
            len = (bytes[1] as u64) | (bytes[2] as u64) << 8;
            start_pos = 3;
        }
        0xfd => {
            len = (bytes[1] as u64) | (bytes[2] as u64) << 8 | (bytes[3] as u64) << 16;
            start_pos = 4;
        }
        0xfe => {
            len = (bytes[1] as u64) | (bytes[2] as u64) << 8 | (bytes[3] as u64) << 16 |
                (bytes[4] as u64) << 24 | (bytes[5] as u64) << 32 | (bytes[6] as u64) << 40 |
                (bytes[7] as u64) << 48 | (bytes[8] as u64) << 56;
            start_pos = 9;
        }
        _ => {
            len = (bytes[0] as u64);
            start_pos = 1;
        }
    }

    Some((len, start_pos))
}
