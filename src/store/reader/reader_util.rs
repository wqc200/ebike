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

const INDEX_LEVEL_PRIMARY: i32 = 0;
const INDEX_LEVEL_UNIQUE: i32 = 1;

#[derive(Clone, Debug)]
pub struct TableIndex {
    pub index_name: String,
    pub level: i32,
    pub column_range_list: Vec<ColumnRange>,
}

#[derive(Clone, Debug)]
pub struct ColumnRange {
    pub column_name: String,
    pub range_value: RangeValue,
}

#[derive(Clone, Debug)]
pub struct RangeNotNullValue {
    start: Option<(String, PointType)>,
    end: Option<(String, PointType)>,
}

impl RangeNotNullValue {
    pub fn getStart(&self) -> Option<(String, PointType)> {
        self.start.clone()
    }

    pub fn getEnd(&self) -> Option<(String, PointType)> {
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
pub struct Range {
    pub start: Option<RangePoint>,
    pub end: Option<RangePoint>,
}

#[derive(Clone, Debug)]
pub enum RangePoint {
    Null,
    NotNull,
    NotNullValue(ScalarValue, PointType),
}

#[derive(Clone, Debug)]
pub enum PointType {
    Open,
    Closed,
}

pub fn compare_column_filter(operators: Vec<Expr>) -> CompareResult {
    let mut range_list = vec![];
    for operator in operators {
        let range = match operator {
            Expr::BinaryExpr { op, right, .. } => {
                let scalar_value = match right.as_ref() {
                    Expr::Literal(scalar_value) => {
                        scalar_value.clone()
                    }
                    _ => continue,
                };

                match op {
                    Operator::GtEq => {
                        let range_point = RangePoint::NotNullValue(scalar_value, PointType::Closed);
                        Range {
                            start: Some(range_point.clone()),
                            end: None,
                        }
                    }
                    Operator::Gt => {
                        let range_point = RangePoint::NotNullValue(scalar_value, PointType::Open);
                        Range {
                            start: Some(range_point.clone()),
                            end: None,
                        }
                    }
                    Operator::Eq => {
                        let range_point = RangePoint::NotNullValue(scalar_value, PointType::Closed);
                        Range {
                            start: Some(range_point.clone()),
                            end: Some(range_point.clone()),
                        }
                    }
                    Operator::Lt => {
                        let range_point = RangePoint::NotNullValue(scalar_value, PointType::Open);
                        Range {
                            start: None,
                            end: Some(range_point.clone()),
                        }
                    }
                    Operator::LtEq => {
                        let range_point = RangePoint::NotNullValue(scalar_value, PointType::Closed);
                        Range {
                            start: None,
                            end: Some(range_point.clone()),
                        }
                    }
                    _ => continue,
                }
            }
            Expr::IsNotNull(_) => {
                let range_point = RangePoint::NotNull;
                Range {
                    start: Some(range_point.clone()),
                    end: Some(range_point.clone()),
                }
            }
            Expr::IsNull(_) => {
                let range_point = RangePoint::Null;
                Range {
                    start: Some(range_point.clone()),
                    end: Some(range_point.clone()),
                }
            }
            _ => continue,
        };

        range_list.push(range);
    };

    let mut accumulator_range = Range {
        start: None,
        end: None,
    };
    for range in range_list {
        match range.start {
            None => {}
            Some(range_point) => {
                match range_point {
                    RangePoint::Null => {
                        match accumulator_range.start.clone() {
                            None => accumulator_range.start = range.start.clone(),
                            Some(_) => {}
                        }
                    }
                    RangePoint::NotNull => {
                        match accumulator_range.start.clone() {
                            None => accumulator_range.start = range.start.clone(),
                            Some(accumulator_range_point) => {
                                match accumulator_range_point {
                                    RangePoint::Null => accumulator_range.start = range.start.clone(),
                                    RangePoint::NotNull => {}
                                    RangePoint::NotNullValue(_, _) => {}
                                }
                            }
                        }
                    }
                    RangePoint::NotNullValue(scalar_value, point_type) => {
                        match accumulator_range.start.clone() {
                            None => accumulator_range.start = range.start.clone(),
                            Some(accumulator_range_point) => {
                                match accumulator_range_point {
                                    RangePoint::Null => accumulator_range.start = range.start.clone(),
                                    RangePoint::NotNull => accumulator_range.start = range.start.clone(),
                                    RangePoint::NotNullValue(accumulator_scalar_value, _) => {
                                        match accumulator_scalar_value.partial_cmp(&scalar_value) {
                                            None => {},
                                            Some(ordering) => {
                                                match ordering {
                                                    Ordering::Greater => {}
                                                    Ordering::Equal => {
                                                        match point_type {
                                                            PointType::Open => accumulator_range.start = range.start.clone(),
                                                            PointType::Closed => {}
                                                        }
                                                    }
                                                    Ordering::Less => accumulator_range.start = range.start.clone(),
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        match range.end {
            None => {}
            Some(range_point) => {
                match range_point {
                    RangePoint::Null => {
                        match accumulator_range.end.clone() {
                            None => accumulator_range.end = range.end.clone(),
                            Some(accumulator_range_point) => {
                                match accumulator_range_point {
                                    RangePoint::Null => {},
                                    RangePoint::NotNull => accumulator_range.end = range.end.clone(),
                                    RangePoint::NotNullValue(_, _) => accumulator_range.end = range.end.clone(),
                                }
                            }
                        }
                    }
                    RangePoint::NotNull => {
                        match accumulator_range.end.clone() {
                            None => accumulator_range.end = range.end.clone(),
                            Some(accumulator_range_point) => {
                                match accumulator_range_point {
                                    RangePoint::Null => {},
                                    RangePoint::NotNull => {}
                                    RangePoint::NotNullValue(_, _) => accumulator_range.end = range.end.clone(),
                                }
                            }
                        }
                    }
                    RangePoint::NotNullValue(scalar_value, point_type) => {
                        match accumulator_range.end.clone() {
                            None => accumulator_range.end = range.end.clone(),
                            Some(accumulator_range_point) => {
                                match accumulator_range_point {
                                    RangePoint::Null => {},
                                    RangePoint::NotNull => {},
                                    RangePoint::NotNullValue(accumulator_scalar_value, _) => {
                                        match accumulator_scalar_value.partial_cmp(&scalar_value) {
                                            None => {},
                                            Some(ordering) => {
                                                match ordering {
                                                    Ordering::Greater => accumulator_range.end = range.end.clone(),
                                                    Ordering::Equal => {
                                                        match point_type {
                                                            PointType::Open => accumulator_range.end = range.end.clone(),
                                                            PointType::Closed => {}
                                                        }
                                                    }
                                                    Ordering::Less => {},
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }


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
                                        compare_value.start = Some((input.clone(), PointType::Closed));
                                    }
                                    Some((start_value, interval)) => {
                                        match input.as_str().partial_cmp(start_value.as_str()) {
                                            None => return CompareResult::All,
                                            Some(order) => {
                                                match order {
                                                    Ordering::Greater => {
                                                        compare_value.start = Some((input.clone(), PointType::Closed));
                                                    }
                                                    Ordering::Equal => {
                                                        match interval {
                                                            PointType::Open => {}
                                                            PointType::Closed => {}
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
                                                            PointType::Open => return CompareResult::Empty,
                                                            PointType::Closed => {}
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
                                        compare_value.start = Some((input.clone(), PointType::Open));
                                    }
                                    Some((start_value, interval)) => {
                                        match input.as_str().partial_cmp(start_value.as_str()) {
                                            None => return CompareResult::All,
                                            Some(order) => {
                                                match order {
                                                    Ordering::Greater => {
                                                        compare_value.start = Some((input.clone(), PointType::Open));
                                                    }
                                                    Ordering::Equal => {
                                                        match interval {
                                                            PointType::Open => {}
                                                            PointType::Closed => {
                                                                compare_value.start = Some((input.clone(), PointType::Open));
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
                                        compare_value.start = Some((input.clone(), PointType::Closed));
                                    }
                                    Some((start_value, interval)) => {
                                        match input.as_str().partial_cmp(start_value.as_str()) {
                                            None => return CompareResult::All,
                                            Some(order) => {
                                                match order {
                                                    Ordering::Greater => {
                                                        compare_value.start = Some((input.clone(), PointType::Closed));
                                                    }
                                                    Ordering::Equal => {
                                                        match interval {
                                                            PointType::Open => return CompareResult::Empty,
                                                            PointType::Closed => {}
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
                                        compare_value.end = Some((input.clone(), PointType::Closed));
                                    }
                                    Some((end_value, interval)) => {
                                        match input.as_str().partial_cmp(end_value.as_str()) {
                                            None => return CompareResult::All,
                                            Some(order) => {
                                                match order {
                                                    Ordering::Greater => return CompareResult::Empty,
                                                    Ordering::Equal => {
                                                        match interval {
                                                            PointType::Open => return CompareResult::Empty,
                                                            PointType::Closed => {}
                                                        }
                                                    }
                                                    Ordering::Less => {
                                                        compare_value.end = Some((input.clone(), PointType::Closed));
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
                                        compare_value.end = Some((input.clone(), PointType::Open));
                                    }
                                    Some((end_value, interval)) => {
                                        match input.as_str().partial_cmp(end_value.as_str()) {
                                            None => return CompareResult::All,
                                            Some(order) => {
                                                match order {
                                                    Ordering::Greater => return CompareResult::Empty,
                                                    Ordering::Equal => {
                                                        match interval {
                                                            PointType::Open => {}
                                                            PointType::Closed => {
                                                                compare_value.end = Some((input.clone(), PointType::Open));
                                                            }
                                                        }
                                                    }
                                                    Ordering::Less => {
                                                        compare_value.end = Some((input.clone(), PointType::Open));
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
                                                            PointType::Open => return CompareResult::Empty,
                                                            PointType::Closed => {}
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
                                        compare_value.end = Some((input.clone(), PointType::Closed));
                                    }
                                    Some((end_value, interval)) => {
                                        match input.as_str().partial_cmp(end_value.as_str()) {
                                            None => return CompareResult::All,
                                            Some(order) => {
                                                match order {
                                                    Ordering::Greater => return CompareResult::Empty,
                                                    Ordering::Equal => {}
                                                    Ordering::Less => {
                                                        compare_value.end = Some((input.clone(), PointType::Closed));
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

pub fn get_seek_prefix(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName, table: TableDef, filters: &[Expr]) -> MysqlResult<SeekType> {
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

    let table_index_list = get_table_index_list(table.clone(), column_range_map);

    let result = get_seek_prefix_with_index(global_context, table.clone(), table_index_list);
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

pub fn get_table_index_list(table_def: TableDef, column_range_map: HashMap<String, ColumnRange>) -> Vec<TableIndex> {
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

                let mut level = 0;
                if is_primary.clone() {
                    level = INDEX_LEVEL_PRIMARY;
                } else {
                    level = INDEX_LEVEL_UNIQUE;
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

pub fn get_seek_prefix_with_index(global_context: Arc<Mutex<GlobalContext>>, table: TableDef, table_index_list: Vec<TableIndex>) -> MysqlResult<SeekType> {
    if table_index_list.is_empty() {
        return Ok(get_seek_prefix_default(table.option.full_table_name));
    }

    /// Find the index with the most matching fields
    let table_index = table_index_list.iter().fold(table_index_list[0].clone(), |accumulator, item| {
        if item.column_range_list.len() > accumulator.column_range_list.len() {
            item.clone()
        } else if item.column_range_list.len() == accumulator.column_range_list.len() && item.level > accumulator.level {
            item.clone()
        } else {
            accumulator
        }
    });

    let (start, end) = dbkey::create_scan_index(table.clone(), table_index.clone());
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
        index_name: table_index.index_name,
        order,
        start,
        end,
    })
}
