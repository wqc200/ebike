
use arrow::datatypes::{Int32Type, DataType, Field, Schema, SchemaRef};

use crate::ArrowDataType;
use crate::MysqlType;
use crate::SQLDataType;

use crate::mysql::error::{MysqlError, MysqlResult};

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
