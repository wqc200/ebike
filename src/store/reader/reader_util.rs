use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::compute::not;
use datafusion::logical_plan::Expr;
use datafusion::logical_plan::Operator;
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{ObjectName, TableConstraint};

use crate::core::core_util;
use crate::core::global_context::GlobalContext;
use crate::meta::def::ColumnDef;
use crate::meta::meta_util;
use crate::meta::def::TableDef;
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::store::engine::engine_util;
use crate::util::convert::{ToIdent, ToObjectName};
use crate::util::dbkey;
use crate::util::dbkey::CreateScanKey;

#[derive(Clone, Debug)]
pub struct TableIndex {
    index_name: String,
    level: usize,
    column_indexes: Vec<ColumnIndex>,
}

impl TableIndex {
    pub fn getIndexName(&self) -> String {
        self.index_name.clone()
    }

    pub fn getLevel(&self) -> usize {
        self.level
    }

    pub fn getColumns(&self) -> Vec<ColumnIndex> {
        self.column_indexes.clone()
    }

    pub fn setColumns(&mut self, columns: Vec<ColumnIndex>) {
        self.column_indexes = columns
    }
}

#[derive(Clone, Debug)]
pub struct ColumnIndex {
    column_name: String,
    compare_value: CompareValue,
}

impl ColumnIndex {
    pub fn getColumnName(&self) -> String {
        self.column_name.clone()
    }

    pub fn getCompareValue(&self) -> CompareValue {
        self.compare_value.clone()
    }
}

#[derive(Clone, Debug)]
pub struct NotNullValue {
    start: Option<(String, Interval)>,
    end: Option<(String, Interval)>,
}

impl NotNullValue {
    pub fn getStart(&self) -> Option<(String, Interval)> {
        self.start.clone()
    }

    pub fn getEnd(&self) -> Option<(String, Interval)> {
        self.end.clone()
    }
}

pub enum SeekType {
    NoRecord,
    FullTableScan {
        start: String,
        end: String,
    },
    UsingTheIndex {
        index_name: String,
        order: ScanOrder,
        start: CreateScanKey,
        end: CreateScanKey,
    },
}

#[derive(Clone, Debug)]
pub enum CompareResult {
    /// contain null and not null
    All,
    Empty,
    Null,
    NotNull(NotNullValue),
}

#[derive(Clone, Debug)]
pub enum CompareValue {
    Null,
    NotNull(NotNullValue),
}

#[derive(Clone, Debug)]
pub enum ScanOrder {
    Asc,
    Desc,
}

#[derive(Clone, Debug)]
pub enum Interval {
    Open,
    Closed,
}

pub fn compare_column(operators: Vec<Expr>) -> CompareResult {
    let result = match operators[0].clone() {
        Expr::BinaryExpr { op, right, .. } => {
            let input = match right.as_ref() {
                Expr::Literal(scalar_value) => {
                    let value = meta_util::convert_scalar_value_to_index_string(scalar_value.clone());
                    match value {
                        Ok(value) => {
                            match value {
                                None => return CompareResult::All,
                                Some(value) => value
                            }
                        }
                        Err(_) => return CompareResult::All,
                    }
                }
                _ => return CompareResult::All,
            };

            match op {
                Operator::GtEq => {
                    let compare_value = NotNullValue {
                        start: Some((input.clone(), Interval::Closed)),
                        end: None,
                    };
                    CompareResult::NotNull(compare_value)
                }
                Operator::Gt => {
                    let compare_value = NotNullValue {
                        start: Some((input.clone(), Interval::Open)),
                        end: None,
                    };
                    CompareResult::NotNull(compare_value)
                }
                Operator::Eq => {
                    let compare_value = NotNullValue {
                        start: Some((input.clone(), Interval::Closed)),
                        end: Some((input.clone(), Interval::Closed)),
                    };
                    CompareResult::NotNull(compare_value)
                }
                Operator::Lt => {
                    let compare_value = NotNullValue {
                        start: None,
                        end: Some((input.clone(), Interval::Open)),
                    };
                    CompareResult::NotNull(compare_value)
                }
                Operator::LtEq => {
                    let compare_value = NotNullValue {
                        start: None,
                        end: Some((input.clone(), Interval::Closed)),
                    };
                    CompareResult::NotNull(compare_value)
                }
                _ => return CompareResult::All,
            }
        }
        Expr::IsNotNull(_) => {
            let compare_value = NotNullValue {
                start: None,
                end: None,
            };
            CompareResult::NotNull(compare_value)
        }
        Expr::IsNull(_) => {
            CompareResult::Null
        }
        _ => return CompareResult::All,
    };

    let new_operators: Vec<_> = operators.clone().drain(1..).collect();

    for operator in new_operators {
        match operator {
            Expr::BinaryExpr { op, right, .. } => {
                let input = match right.as_ref() {
                    Expr::Literal(scalar_value) => {
                        let value = meta_util::convert_scalar_value_to_index_string(scalar_value.clone());
                        match value {
                            Ok(value) => {
                                match value {
                                    None => return CompareResult::All,
                                    Some(value) => value
                                }
                            }
                            Err(mysql_error) => return CompareResult::All,
                        }
                    }
                    _ => return CompareResult::All,
                };

                match op {
                    Operator::GtEq => {
                        match result.clone() {
                            CompareResult::Null => return CompareResult::Empty,
                            CompareResult::NotNull(mut compare_value) => {
                                match compare_value.start.clone() {
                                    None => {
                                        compare_value.start = Some((input.clone(), Interval::Closed));
                                    }
                                    Some((start_value, interval)) => {
                                        match input.as_str().partial_cmp(start_value.as_str()) {
                                            None => return CompareResult::All,
                                            Some(order) => {
                                                match order {
                                                    Ordering::Greater => {
                                                        compare_value.start = Some((input.clone(), Interval::Closed));
                                                    }
                                                    Ordering::Equal => {
                                                        match interval {
                                                            Interval::Open => {}
                                                            Interval::Closed => {}
                                                        }
                                                    }
                                                    Ordering::Less => return CompareResult::Empty,
                                                }
                                            }
                                        }
                                    }
                                }

                                match compare_value.end.clone() {
                                    None => {}
                                    Some((end_value, interval)) => {
                                        match input.as_str().partial_cmp(end_value.as_str()) {
                                            None => return CompareResult::All,
                                            Some(order) => {
                                                match order {
                                                    Ordering::Greater => return CompareResult::Empty,
                                                    Ordering::Equal => {
                                                        match interval {
                                                            Interval::Open => return CompareResult::Empty,
                                                            Interval::Closed => {}
                                                        }
                                                    }
                                                    Ordering::Less => {}
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Operator::Gt => {
                        match result.clone() {
                            CompareResult::Null => return CompareResult::Empty,
                            CompareResult::NotNull(mut compare_value) => {
                                match compare_value.start.clone() {
                                    None => {
                                        compare_value.start = Some((input.clone(), Interval::Open));
                                    }
                                    Some((start_value, interval)) => {
                                        match input.as_str().partial_cmp(start_value.as_str()) {
                                            None => return CompareResult::All,
                                            Some(order) => {
                                                match order {
                                                    Ordering::Greater => {
                                                        compare_value.start = Some((input.clone(), Interval::Open));
                                                    }
                                                    Ordering::Equal => {
                                                        match interval {
                                                            Interval::Open => {}
                                                            Interval::Closed => {
                                                                compare_value.start = Some((input.clone(), Interval::Open));
                                                            }
                                                        }
                                                    }
                                                    Ordering::Less => return CompareResult::Empty,
                                                }
                                            }
                                        }
                                    }
                                }

                                match compare_value.end.clone() {
                                    None => {}
                                    Some((end_value, interval)) => {
                                        match input.as_str().partial_cmp(end_value.as_str()) {
                                            None => return CompareResult::All,
                                            Some(order) => {
                                                match order {
                                                    Ordering::Greater => return CompareResult::Empty,
                                                    Ordering::Equal => return CompareResult::Empty,
                                                    Ordering::Less => {}
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Operator::Eq => {
                        match result.clone() {
                            CompareResult::Null => return CompareResult::Empty,
                            CompareResult::NotNull(mut compare_value) => {
                                match compare_value.start.clone() {
                                    None => {
                                        compare_value.start = Some((input.clone(), Interval::Closed));
                                    }
                                    Some((start_value, interval)) => {
                                        match input.as_str().partial_cmp(start_value.as_str()) {
                                            None => return CompareResult::All,
                                            Some(order) => {
                                                match order {
                                                    Ordering::Greater => {
                                                        compare_value.start = Some((input.clone(), Interval::Closed));
                                                    }
                                                    Ordering::Equal => {
                                                        match interval {
                                                            Interval::Open => return CompareResult::Empty,
                                                            Interval::Closed => {}
                                                        }
                                                    }
                                                    Ordering::Less => return CompareResult::Empty,
                                                }
                                            }
                                        }
                                    }
                                }

                                match compare_value.end.clone() {
                                    None => {
                                        compare_value.end = Some((input.clone(), Interval::Closed));
                                    }
                                    Some((end_value, interval)) => {
                                        match input.as_str().partial_cmp(end_value.as_str()) {
                                            None => return CompareResult::All,
                                            Some(order) => {
                                                match order {
                                                    Ordering::Greater => return CompareResult::Empty,
                                                    Ordering::Equal => {
                                                        match interval {
                                                            Interval::Open => return CompareResult::Empty,
                                                            Interval::Closed => {}
                                                        }
                                                    }
                                                    Ordering::Less => {
                                                        compare_value.end = Some((input.clone(), Interval::Closed));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Operator::Lt => {
                        match result.clone() {
                            CompareResult::Null => return CompareResult::Empty,
                            CompareResult::NotNull(mut compare_value) => {
                                match compare_value.start.clone() {
                                    None => {}
                                    Some((start_value, interval)) => {
                                        match input.as_str().partial_cmp(start_value.as_str()) {
                                            None => return CompareResult::All,
                                            Some(order) => {
                                                match order {
                                                    Ordering::Greater => {}
                                                    Ordering::Equal => return CompareResult::Empty,
                                                    Ordering::Less => return CompareResult::Empty,
                                                }
                                            }
                                        }
                                    }
                                }

                                match compare_value.end.clone() {
                                    None => {
                                        compare_value.end = Some((input.clone(), Interval::Open));
                                    }
                                    Some((end_value, interval)) => {
                                        match input.as_str().partial_cmp(end_value.as_str()) {
                                            None => return CompareResult::All,
                                            Some(order) => {
                                                match order {
                                                    Ordering::Greater => return CompareResult::Empty,
                                                    Ordering::Equal => {
                                                        match interval {
                                                            Interval::Open => {}
                                                            Interval::Closed => {
                                                                compare_value.end = Some((input.clone(), Interval::Open));
                                                            }
                                                        }
                                                    }
                                                    Ordering::Less => {
                                                        compare_value.end = Some((input.clone(), Interval::Open));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Operator::LtEq => {
                        match result.clone() {
                            CompareResult::Null => return CompareResult::Empty,
                            CompareResult::NotNull(mut compare_value) => {
                                match compare_value.start.clone() {
                                    None => {}
                                    Some((start_value, interval)) => {
                                        match input.as_str().partial_cmp(start_value.as_str()) {
                                            None => return CompareResult::All,
                                            Some(order) => {
                                                match order {
                                                    Ordering::Greater => {}
                                                    Ordering::Equal => {
                                                        match interval {
                                                            Interval::Open => return CompareResult::Empty,
                                                            Interval::Closed => {}
                                                        }
                                                    }
                                                    Ordering::Less => return CompareResult::Empty,
                                                }
                                            }
                                        }
                                    }
                                }

                                match compare_value.end.clone() {
                                    None => {
                                        compare_value.end = Some((input.clone(), Interval::Closed));
                                    }
                                    Some((end_value, interval)) => {
                                        match input.as_str().partial_cmp(end_value.as_str()) {
                                            None => return CompareResult::All,
                                            Some(order) => {
                                                match order {
                                                    Ordering::Greater => return CompareResult::Empty,
                                                    Ordering::Equal => {}
                                                    Ordering::Less => {
                                                        compare_value.end = Some((input.clone(), Interval::Closed));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => return CompareResult::All,
                }
            }
            Expr::IsNotNull(_) => {
                match result.clone() {
                    CompareResult::Null => return CompareResult::Empty,
                    CompareResult::NotNull(_) => {}
                    _ => {}
                }
            }
            Expr::IsNull(_) => {
                match result.clone() {
                    CompareResult::Null => {}
                    CompareResult::NotNull(_) => return CompareResult::Empty,
                    _ => {}
                }
            }
            _ => {}
        }
    }

    result
}

pub fn get_seek_prefix(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName, table_def: TableDef, filters: &[Expr]) -> MysqlResult<SeekType> {
    if filters.len() < 1 {
        return Ok(get_seek_prefix_default(full_table_name));
    }

    let column_filter_map = build_filter(filters).unwrap();

    let mut column_index_map = HashMap::new();
    for (column_name, exprs) in column_filter_map {
        let compare = compare_column(exprs);
        match compare {
            CompareResult::All => {
                break;
            }
            CompareResult::Empty => {
                return Ok(SeekType::NoRecord);
            }
            CompareResult::Null => {
                let compare_value = CompareValue::Null;
                let start_end = ColumnIndex {
                    column_name: column_name.clone(),
                    compare_value,
                };
                column_index_map.insert(column_name.clone(), start_end);
            }
            CompareResult::NotNull(not_null_value) => {
                let compare_value = CompareValue::NotNull(not_null_value.clone());
                let start_end = ColumnIndex {
                    column_name: column_name.clone(),
                    compare_value,
                };
                column_index_map.insert(column_name.clone(), start_end);

                /// interval scanning
                if not_null_value.clone().getStart().is_none() || not_null_value.clone().getEnd().is_none() {
                    break;
                }
            }
        }
    }
    if column_index_map.is_empty() {
        return Ok(get_seek_prefix_default(full_table_name));
    }

    let table_indexes = get_table_indexes(table_def, column_index_map);
    if table_indexes.is_empty() {
        return Ok(get_seek_prefix_default(full_table_name));
    }

    let result = get_seek_prefix_with_index(global_context, full_table_name, table_indexes);
    match result {
        Ok(seek_type) => Ok(seek_type),
        Err(mysql_error) => Err(mysql_error)
    }
}

pub fn get_seek_prefix_default(full_table_name: ObjectName) -> SeekType {
    let start = dbkey::scan_record_rowid(full_table_name.clone());
    let end = dbkey::scan_record_rowid(full_table_name.clone());
    SeekType::FullTableScan { start, end }
}

pub fn build_filter(filters: &[Expr]) -> MysqlResult<HashMap<String, Vec<Expr>>> {
    let mut filter_keys: HashMap<String, Vec<Expr>> = HashMap::new();
    for expr in filters {
        let column_name;
        match expr {
            Expr::IsNull(expr) => {
                match expr.as_ref() {
                    Expr::Column(value) => {
                        column_name = value.clone();
                    }
                    _ => break
                }
            }
            Expr::IsNotNull(expr) => {
                match expr.as_ref() {
                    Expr::Column(value) => {
                        column_name = value.clone();
                    }
                    _ => break
                }
            }
            Expr::BinaryExpr { left, op, right } => {
                match left.as_ref() {
                    Expr::Column(value) => {
                        column_name = value.clone();
                    }
                    _ => break
                }
            }
            _ => break
        }

        filter_keys.entry(column_name.to_string()).or_insert(vec![]).push(expr.clone());
    }

    Ok(filter_keys)
}

pub fn get_table_indexes(table_def: TableDef, column_start_end: HashMap<String, ColumnIndex>) -> Vec<TableIndex> {
    let mut table_indexes: Vec<TableIndex> = vec![];
    for table_constraint in table_def.get_constraints() {
        match table_constraint {
            TableConstraint::Unique { name, columns, is_primary } => {
                let index_name = name.clone().unwrap().value.to_string();

                let mut column_names = vec![];
                for column in columns {
                    let column_name = column.to_string();

                    if !column_start_end.contains_key(column_name.as_str()) {
                        break;
                    }
                    let column_start_end = column_start_end.get(column_name.as_str()).unwrap();

                    column_names.push(column_start_end.clone());
                }

                if column_names.is_empty() {
                    continue;
                }

                let mut level: usize = 0;
                if is_primary.clone() {
                    level = 1;
                } else {
                    level = 2;
                }

                let table_index = TableIndex {
                    index_name,
                    level,
                    column_indexes: column_names,
                };
                table_indexes.push(table_index);
            }
            _ => {}
        }
    }
    table_indexes
}

pub fn get_seek_prefix_with_index(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName, table_indexes: Vec<TableIndex>) -> MysqlResult<SeekType> {
    let catalog_name = meta_util::cut_out_catalog_name(full_table_name.clone());
    let schema_name = meta_util::cut_out_schema_name(full_table_name.clone());
    let table_name = meta_util::cut_out_table_name(full_table_name.clone());

    /// Find the index with the most matching fields
    let table_index = table_indexes.iter().fold(table_indexes[0].clone(), |accumulator, item| {
        if item.getColumns().len() > accumulator.getColumns().len() {
            item.clone()
        } else if item.getColumns().len() == accumulator.getColumns().len() && item.getLevel() > accumulator.getLevel() {
            item.clone()
        } else {
            accumulator
        }
    });

    let mut column_indexes: Vec<(usize, CompareValue)> = vec![];
    for column_index in table_index.getColumns() {
        let column_name = column_index.getColumnName().to_ident();
        let serial_number = global_context.lock().unwrap().meta_cache.get_serial_number(full_table_name.clone(), column_name.clone()).unwrap();

        column_indexes.push((serial_number, column_index.getCompareValue()));
    }

    let (start, end) = dbkey::create_scan_index(full_table_name.clone(), table_index.getIndexName().as_str(), column_indexes);
    let order = match start.key().as_str().partial_cmp(end.key().as_str()) {
        None => ScanOrder::Asc,
        Some(order) => {
            match order {
                Ordering::Greater => ScanOrder::Asc,
                Ordering::Equal => ScanOrder::Asc,
                Ordering::Less => ScanOrder::Desc,
            }
        }
    };

    Ok(SeekType::UsingTheIndex {
        index_name: table_index.getIndexName(),
        order,
        start,
        end,
    })
}
