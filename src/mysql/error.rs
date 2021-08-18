// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! DataFusion error types

use std::error;
use std::fmt::{Display, Formatter};
use std::io;
use std::result;

use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use parquet::errors::ParquetError;
use sqlparser::parser::ParserError;

pub type MysqlResult<T> = result::Result<T, MysqlError>;

#[derive(Debug)]
#[allow(missing_docs)]
pub struct MysqlError {
    error_number: u16,
    sql_state: String,
    message: String,
}

impl MysqlError {
    pub fn error_number(&self) -> u16{
        self.error_number
    }

    pub fn sql_state(&self) -> String{
        self.sql_state.to_string()
    }

    pub fn message(&self) -> String{
        self.message.to_string()
    }
}

impl MysqlError {
    /// Reference: https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
    pub fn new_server_error(error_number: u16, sql_state: &str, message: &str) -> Self {
        Self {
            error_number,
            sql_state: sql_state.to_string(),
            message: message.to_string(),
        }
    }

    /// Reference: https://dev.mysql.com/doc/mysql-errors/8.0/en/global-error-reference.html
    pub fn new_global_error(error_number: u16, message: &str) -> Self {
        MysqlError::new_server_error(error_number, "HY000", message)
    }
}

impl From<DataFusionError> for MysqlError {
    fn from(datafusion_error: DataFusionError) -> Self {
        MysqlError::new_global_error(1105, format!("Unknown error. Datafusion error: {:?}", datafusion_error).as_str())
    }
}

impl From<ArrowError> for MysqlError {
    fn from(arrow_error: ArrowError) -> Self {
        MysqlError::new_global_error(1105, format!("Unknown error. Arrow error: {:?}", arrow_error).as_str())
    }
}

impl Display for MysqlError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MySQL error, Error number: {:?}, SQLSTATE: {:?}, Message: {:?}", self.error_number, self.sql_state, self.message)
    }
}

impl error::Error for MysqlError {}
