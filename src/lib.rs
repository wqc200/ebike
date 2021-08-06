#![feature(min_specialization)]
#[macro_use]
extern crate bitflags;

pub mod core;
pub mod datafusion_impl;
pub mod meta;
pub mod mysql;
pub mod physical_plan;
pub mod logical_plan;
pub mod store;
pub mod test;
pub mod util;
pub mod variable;

use arrow::datatypes::DataType as ArrowDataType;
use sqlparser::ast::DataType as SQLDataType;

use crate::core::global_context::GlobalContext;
use crate::meta::meta_util;
use crate::mysql::handle;
use crate::mysql::metadata::MysqlType;
