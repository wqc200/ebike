use bstr::ByteSlice;
use byteorder::{ByteOrder, LittleEndian};

use datafusion::scalar::ScalarValue;
use sqlparser::ast::{Expr as SQLExpr, Value};

use crate::mysql::error::{MysqlError, MysqlResult};
use crate::mysql::mysql_error_code;
use crate::mysql::mysql_type_code;
use crate::ArrowDataType;
use crate::MysqlType;
use datafusion::logical_plan::Literal;

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

pub fn parse_stmt_execute_args(
    null_bitmap: Vec<u8>,
    param_types: Vec<u8>,
    param_values: Vec<u8>,
) -> MysqlResult<Vec<SQLExpr>> {
    let num_params = 2;

    let mut values = vec![];

    let mut param_type_pos = 0;
    let mut param_value_pos = 0;
    for i in 0..num_params {
        let mysql_type = param_types[param_type_pos] as u64;
        param_type_pos += 1;

        let type_flag = param_types[param_type_pos];
        param_type_pos += 1;

        if (null_bitmap[i / 8] & (1 << (i % 8) as u64)) > 0 {
            SQLExpr::Value(Value::Null);
            continue;
        }

        let is_unsigned = (type_flag & 0x80) > 0;

        let value = match mysql_type {
            mysql_type_code::TYPE_NULL => SQLExpr::Value(Value::Null),
            mysql_type_code::TYPE_INT32 => {
                let end = param_value_pos+4;
                let val = LittleEndian::read_u32(param_values[param_value_pos..end].as_bytes());
                param_value_pos = end;

                SQLExpr::Value(Value::Number(val.to_string(), false))
            }
            mysql_type_code::TYPE_INT64 => {
                let end = param_value_pos+8;
                let val = LittleEndian::read_u64(param_values[param_value_pos..end].as_bytes());
                param_value_pos = end;

                if is_unsigned {
                    SQLExpr::Value(Value::Number(val.to_string(), false))
                } else {
                    SQLExpr::Value(Value::Number((val as i64).to_string(), false))
                }
            }
            mysql_type_code::TYPE_VARCHAR2 => {
                let last_bytes = param_values[param_value_pos..].to_vec();
                let result = parse_length_encoded_bytes(last_bytes);
                match result {
                    None => {
                        return Err(MysqlError::new_global_error(
                            mysql_error_code::CR_MALFORMED_PACKET as u16,
                            format!("malformed packet error").as_str(),
                        ));
                    }
                    Some((start, content)) => {
                        let val = match content.to_str() {
                            Ok(val) => val.to_string(),
                            Err(err) => {
                                log::error!(
                                    "Unknown error, Error reading REQUEST, error: {:?}",
                                    err
                                );
                                break;
                            }
                        };

                        param_value_pos += start + content.len();

                        SQLExpr::Value(Value::SingleQuotedString(val))
                    }
                }
            }
            _ => {
                return Err(MysqlError::new_global_error(
                    mysql_error_code::CR_MALFORMED_PACKET as u16,
                    format!("unsupported mysql type: {:?}", mysql_type).as_str(),
                ));
            }
        };

        values.push(value);
    }

    Ok(values)
}

pub fn parse_length_encoded_bytes(bytes: Vec<u8>) -> Option<(usize, Vec<u8>)> {
    let result = parse_length_encoded_int(bytes.clone());
    match result {
        None => {}
        Some((start, len)) => {
            let end = start + len as usize;

            if bytes.len() <= (end - 1) as usize {
                return None;
            }

            let content = bytes[start..end].to_vec();

            return Some((start, content));
        }
    }

    return None;
}

/// https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
pub fn parse_length_encoded_int(bytes: Vec<u8>) -> Option<(usize, u64)> {
    if bytes.len() < 1 {
        return None;
    }

    let mut len = 0;
    let mut start = 0;

    match bytes[0] {
        0xfc => {
            len = (bytes[1] as u64) | (bytes[2] as u64) << 8;
            start = 3;
        }
        0xfd => {
            len = (bytes[1] as u64) | (bytes[2] as u64) << 8 | (bytes[3] as u64) << 16;
            start = 4;
        }
        0xfe => {
            len = (bytes[1] as u64)
                | (bytes[2] as u64) << 8
                | (bytes[3] as u64) << 16
                | (bytes[4] as u64) << 24
                | (bytes[5] as u64) << 32
                | (bytes[6] as u64) << 40
                | (bytes[7] as u64) << 48
                | (bytes[8] as u64) << 56;
            start = 9;
        }
        _ => {
            len = (bytes[0] as u64);
            start = 1;
        }
    }

    Some((start, len))
}
