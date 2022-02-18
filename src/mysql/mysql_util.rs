use bstr::ByteSlice;
use byteorder::{ByteOrder, LittleEndian};

use datafusion::scalar::ScalarValue;
use sqlparser::ast::{Expr as SQLExpr, Value};

use crate::mysql::error::{MysqlError, MysqlResult};
use crate::mysql::mysql_error_code;
use crate::mysql::mysql_type_code;
use crate::ArrowDataType;
use crate::MysqlType;

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
    param_types: &[u8],
    param_values: &[u8],
) -> MysqlResult<Vec<SQLExpr>> {
    let num_params = 2;

    let mut values = vec![];

    let mut param_type_pos = 0;
    let mut param_value_pos = 0;
    for i in 0..num_params {
        if (null_bitmap[i / 8] & (1 << (i % 8) as u64)) > 0 {
            SQLExpr::Value(Value::Null);
            continue;
        }

        let mysql_type = param_types[param_type_pos] as u64;
        param_type_pos += 1;

        let type_flag = param_types[param_type_pos];
        param_type_pos += 1;

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

                SQLExpr::Value(Value::Number(val.to_string(), false))
            }
            mysql_type_code::TYPE_VARCHAR2 => {
                let result = parse_length_encoded_bytes(&param_values[param_value_pos..]);
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
                                log::error!(
                                    "Unknown error, Error reading REQUEST, error: {:?}",
                                    err
                                );
                                break;
                            }
                        };

                         param_value_pos += bytes.len();

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

pub fn parse_length_encoded_bytes(bytes: &[u8]) -> Option<(&[u8])> {
    let result = parse_length_encoded_int(bytes);
    match result {
        None => {}
        Some((len, start_pos)) => {
            let end_pos = start_pos + len as usize;

            if bytes.len() <= (end_pos - 1) as usize {
                return None;
            }

            return Some((&bytes[start_pos..end_pos]));
        }
    }

    return None;
}

/// https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
pub fn parse_length_encoded_int(bytes: &[u8]) -> Option<(u64, usize)> {
    if bytes.len() < 1 {
        return None;
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
            len = (bytes[1] as u64)
                | (bytes[2] as u64) << 8
                | (bytes[3] as u64) << 16
                | (bytes[4] as u64) << 24
                | (bytes[5] as u64) << 32
                | (bytes[6] as u64) << 40
                | (bytes[7] as u64) << 48
                | (bytes[8] as u64) << 56;
            start_pos = 9;
        }
        _ => {
            len = (bytes[0] as u64);
            start_pos = 1;
        }
    }

    Some((len, start_pos))
}
