use std::error;
use std::fmt::{Display, Formatter};
use std::result;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use sqlparser::ast::ObjectName;

use crate::meta::meta_def::TableDef;
use crate::mysql::error::MysqlError;
use crate::mysql::metadata::Column;

pub struct StmtPrepare {
    pub statement_id: u32,
    pub columns: Vec<Column>,
    pub params: Vec<Column>,
}

impl StmtPrepare {
    pub fn new(statement_id: u32, columns: Vec<Column>, params: Vec<Column>) -> Self {
        StmtPrepare::new_with_message(statement_id, columns, params)
    }

    pub fn new_with_message(statement_id: u32, columns: Vec<Column>, params: Vec<Column>) -> Self {
        Self {
            statement_id,
            columns,
            params,
        }
    }
}

pub struct FinalCount {
    pub affect_rows: u64,
    pub last_insert_id: u64,
    pub message: String,
}

impl FinalCount {
    pub fn new(affect_rows: u64, last_insert_id: u64) -> Self {
        FinalCount::new_with_message(affect_rows, last_insert_id, "")
    }

    pub fn new_with_message(affect_rows: u64, last_insert_id: u64, message: &str) -> Self {
        Self {
            affect_rows,
            last_insert_id,
            message: message.to_string(),
        }
    }
}

pub struct ResultSet {
    pub schema_ref: SchemaRef,
    pub record_batches: Vec<RecordBatch>,
}

impl ResultSet {
    pub fn new(schema_ref: SchemaRef, record_batches: Vec<RecordBatch>) -> Self {
        Self {
            schema_ref,
            record_batches,
        }
    }
}

pub enum CoreOutput {
    FinalCount(FinalCount),
    ResultSet(ResultSet),
    MultiResultSet(Vec<Vec<RecordBatch>>),
    ComFieldList(ObjectName, ObjectName, TableDef),
    ComStmtPrepare(StmtPrepare),
    ComStmtClose,
}

pub type Result<T> = result::Result<T, OutputError>;

#[derive(Debug)]
#[allow(missing_docs)]
pub enum OutputError {
    DataFusionError(DataFusionError),
    MysqlError(MysqlError),
}

impl From<DataFusionError> for OutputError {
    fn from(e: DataFusionError) -> Self {
        OutputError::DataFusionError(e)
    }
}

impl From<MysqlError> for OutputError {
    fn from(e: MysqlError) -> Self {
        OutputError::MysqlError(e)
    }
}

impl Display for OutputError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            OutputError::DataFusionError(ref desc) => write!(f, "DataFusion error: {}", desc),
            OutputError::MysqlError(ref desc) => {
                write!(f, "Mysql error: {}", desc)
            }
        }
    }
}

impl error::Error for OutputError {}
