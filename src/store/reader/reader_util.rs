use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::compute::not;
use datafusion::logical_plan::{Expr, Column};
use datafusion::logical_plan::Operator;
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{ObjectName, TableConstraint};

use crate::core::core_util;
use crate::core::global_context::GlobalContext;
use crate::meta::meta_def::SparrowColumnDef;
use crate::meta::meta_util;
use crate::meta::meta_def::TableDef;
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::store::engine::engine_util;
use crate::util::convert::{ToIdent, ToObjectName};
use crate::util::dbkey;
use crate::util::dbkey::CreateScanKey;

#[derive(Clone, Debug)]
pub struct TableIndex {
    index_name: String,
    level: usize,
    column_range_list: Vec<ColumnRange>,
}

impl TableIndex {
    pub fn getIndexName(&self) -> String {
        self.index_name.clone()
    }

    pub fn getLevel(&self) -> usize {
        self.level
    }

    pub fn getColumnRangeList(&self) -> Vec<ColumnRange> {
        self.column_range_list.clone()
    }

    pub fn setColumns(&mut self, columns: Vec<ColumnRange>) {
        self.column_range_list = columns
    }
}

#[derive(Clone, Debug)]
pub struct ColumnRange {
    column_name: String,
    range_value: RangeValue,
}

impl ColumnRange {
    pub fn getColumnName(&self) -> String {
        self.column_name.clone()
    }

    pub fn getCompareValue(&self) -> RangeValue {
        self.range_value.clone()
    }
}

#[derive(Clone, Debug)]
pub struct RangeNotNullValue {
    start: Option<(String, Interval)>,
    end: Option<(String, Interval)>,
}

impl RangeNotNullValue {
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
    NotNull(RangeNotNullValue),
}

#[derive(Clone, Debug)]
pub enum RangeValue {
    Null,
    NotNull(RangeNotNullValue),
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

pub fn compare_column_filter(operators: Vec<Expr>) -> CompareResult {
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
                    let compare_value = RangeNotNullValue {
                        start: Some((input.clone(), Interval::Closed)),
                        end: None,
                    };
                    CompareResult::NotNull(compare_value)
                }
                Operator::Gt => {
                    let compare_value = RangeNotNullValue {
                        start: Some((input.clone(), Interval::Open)),
                        end: None,
                    };
                    CompareResult::NotNull(compare_value)
                }
                Operator::Eq => {
                    let compare_value = RangeNotNullValue {
                        start: Some((input.clone(), Interval::Closed)),
                        end: Some((input.clone(), Interval::Closed)),
                    };
                    CompareResult::NotNull(compare_value)
                }
                Operator::Lt => {
                    let compare_value = RangeNotNullValue {
                        start: None,
                        end: Some((input.clone(), Interval::Open)),
                    };
                    CompareResult::NotNull(compare_value)
                }
                Operator::LtEq => {
                    let compare_value = RangeNotNullValue {
                        start: None,
                        end: Some((input.clone(), Interval::Closed)),
                    };
                    CompareResult::NotNull(compare_value)
                }
                _ => return CompareResult::All,
            }
        }
        Expr::IsNotNull(_) => {
            let compare_value = RangeNotNullValue {
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

    let column_filter_map = create_column_filter(filters).unwrap();

    let mut column_range_map = HashMap::new();
    for (column_name, expr_list) in column_filter_map {
        let compare_result = compare_column_filter(expr_list);
        match compare_result {
            CompareResult::All => {
                break;
            }
            CompareResult::Empty => {
                return Ok(SeekType::NoRecord);
            }
            CompareResult::Null => {
                let compare_value = RangeValue::Null;
                let column_range = ColumnRange {
                    column_name: column_name.clone(),
                    range_value: compare_value,
                };
                column_range_map.insert(column_name.clone(), column_range);
            }
            CompareResult::NotNull(not_null_value) => {
                let compare_value = RangeValue::NotNull(not_null_value.clone());
                let start_end = ColumnRange {
                    column_name: column_name.clone(),
                    range_value: compare_value,
                };
                column_range_map.insert(column_name.clone(), start_end);

                /// interval scanning
                if not_null_value.clone().getStart().is_none() || not_null_value.clone().getEnd().is_none() {
                    break;
                }
            }
        }
    }
    if column_range_map.is_empty() {
        return Ok(get_seek_prefix_default(full_table_name));
    }

    let table_index_list = get_table_indexes(table_def, column_range_map);
    if table_index_list.is_empty() {
        return Ok(get_seek_prefix_default(full_table_name));
    }

    let result = get_seek_prefix_with_index(global_context, full_table_name, table_index_list);
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

pub fn create_column_filter(filters: &[Expr]) -> MysqlResult<HashMap<String, Vec<Expr>>> {
    let mut column_filter_map: HashMap<String, Vec<Expr>> = HashMap::new();
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

        column_filter_map.entry(column_name.to_string()).or_insert(vec![]).push(expr.clone());
    }

    Ok(column_filter_map)
}

pub fn get_table_indexes(table_def: TableDef, column_range_map: HashMap<String, ColumnRange>) -> Vec<TableIndex> {
    let mut table_index_list: Vec<TableIndex> = vec![];
    for table_constraint in table_def.get_constraints() {
        match table_constraint {
            TableConstraint::Unique { name, columns, is_primary } => {
                let index_name = name.clone().unwrap().value.to_string();

                let mut column_range_list = vec![];
                for column in columns {
                    let column_name = column.to_string();

                    if !column_range_map.contains_key(column_name.as_str()) {
                        break;
                    }
                    let column_range = column_range_map.get(column_name.as_str()).unwrap();

                    column_range_list.push(column_range.clone());
                }

                if column_range_list.is_empty() {
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
                    column_range_list,
                };
                table_index_list.push(table_index);
            }
            _ => {}
        }
    }
    table_index_list
}

pub fn get_seek_prefix_with_index(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName, table_index_list: Vec<TableIndex>) -> MysqlResult<SeekType> {
    /// Find the index with the most matching fields
    let table_index = table_index_list.iter().fold(table_index_list[0].clone(), |accumulator, item| {
        if item.getColumnRangeList().len() > accumulator.getColumnRangeList().len() {
            item.clone()
        } else if item.getColumnRangeList().len() == accumulator.getColumnRangeList().len() && item.getLevel() > accumulator.getLevel() {
            item.clone()
        } else {
            accumulator
        }
    });

    let mut column_indexes: Vec<(usize, RangeValue)> = vec![];
    for column_index in table_index.getColumnRangeList() {
        let column_name = column_index.getColumnName().to_ident();
        let serial_number = global_context.lock().unwrap().meta_data.get_serial_number(full_table_name.clone(), column_name.clone()).unwrap();

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
