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
    pub range: Range,
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
    FullTableScan {
        start: CreateScanKey,
        end: CreateScanKey,
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
    pub start: RangePoint,
    pub end: RangePoint,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RangePoint {
    Infinity,
    Null,
    NotNull,
    NotNullValue(ScalarValue, PointType),
}

#[derive(Clone, Debug, PartialEq)]
pub enum PointType {
    Open,
    Closed,
}

pub fn create_column_range(operators: Vec<Expr>) -> Range {
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
                        Range {
                            start: RangePoint::NotNullValue(scalar_value.clone(), PointType::Closed),
                            end: RangePoint::Infinity,
                        }
                    }
                    Operator::Gt => {
                        Range {
                            start: RangePoint::NotNullValue(scalar_value.clone(), PointType::Open),
                            end: RangePoint::Infinity,
                        }
                    }
                    Operator::Eq => {
                        Range {
                            start: RangePoint::NotNullValue(scalar_value.clone(), PointType::Closed),
                            end: RangePoint::NotNullValue(scalar_value.clone(), PointType::Closed),
                        }
                    }
                    Operator::Lt => {
                        Range {
                            start: RangePoint::Infinity,
                            end: RangePoint::NotNullValue(scalar_value.clone(), PointType::Open),
                        }
                    }
                    Operator::LtEq => {
                        Range {
                            start: RangePoint::Infinity,
                            end: RangePoint::NotNullValue(scalar_value.clone(), PointType::Closed),
                        }
                    }
                    _ => continue,
                }
            }
            Expr::IsNotNull(_) => {
                Range {
                    start: RangePoint::NotNull,
                    end: RangePoint::NotNull,
                }
            }
            Expr::IsNull(_) => {
                Range {
                    start: RangePoint::NotNull,
                    end: RangePoint::NotNull,
                }
            }
            _ => continue,
        };

        range_list.push(range);
    };

    let mut accumulator_range = Range {
        start: RangePoint::Infinity,
        end: RangePoint::Infinity,
    };
    for range in range_list {
        match range.start.clone() {
            RangePoint::Infinity => {}
            RangePoint::Null => {
                match accumulator_range.start.clone() {
                    RangePoint::Infinity => accumulator_range.start = range.start.clone(),
                    RangePoint::Null => {}
                    RangePoint::NotNull => {}
                    RangePoint::NotNullValue(_, _) => {}
                }
            }
            RangePoint::NotNull => {
                match accumulator_range.start.clone() {
                    RangePoint::Infinity => accumulator_range.start = range.start.clone(),
                    RangePoint::Null => accumulator_range.start = range.start.clone(),
                    RangePoint::NotNull => {}
                    RangePoint::NotNullValue(_, _) => {}
                }
            }
            RangePoint::NotNullValue(scalar_value, point_type) => {
                match accumulator_range.start.clone() {
                    RangePoint::Infinity => accumulator_range.start = range.start.clone(),
                    RangePoint::Null => accumulator_range.start = range.start.clone(),
                    RangePoint::NotNull => accumulator_range.start = range.start.clone(),
                    RangePoint::NotNullValue(accumulator_scalar_value, _) => {
                        match accumulator_scalar_value.partial_cmp(&scalar_value) {
                            None => {}
                            Some(ordering) => {
                                match ordering {
                                    Ordering::Greater => {}
                                    Ordering::Equal => {
                                        match point_type.clone() {
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

        match range.end.clone() {
            RangePoint::Infinity => {}
            RangePoint::Null => {
                match accumulator_range.end.clone() {
                    RangePoint::Infinity => accumulator_range.end = range.end.clone(),
                    RangePoint::Null => {}
                    RangePoint::NotNull => accumulator_range.end = range.end.clone(),
                    RangePoint::NotNullValue(_, _) => accumulator_range.end = range.end.clone(),
                }
            }
            RangePoint::NotNull => {
                match accumulator_range.end.clone() {
                    RangePoint::Infinity => accumulator_range.end = range.end.clone(),
                    RangePoint::Null => {}
                    RangePoint::NotNull => {}
                    RangePoint::NotNullValue(_, _) => accumulator_range.end = range.end.clone(),
                }
            }
            RangePoint::NotNullValue(scalar_value, point_type) => {
                match accumulator_range.end.clone() {
                    RangePoint::Infinity => accumulator_range.end = range.end.clone(),
                    RangePoint::Null => {}
                    RangePoint::NotNull => accumulator_range.end = range.end.clone(),
                    RangePoint::NotNullValue(accumulator_scalar_value, _) => {
                        match accumulator_scalar_value.partial_cmp(&scalar_value) {
                            None => {}
                            Some(ordering) => {
                                match ordering {
                                    Ordering::Greater => accumulator_range.end = range.end.clone(),
                                    Ordering::Equal => {
                                        match point_type.clone() {
                                            PointType::Open => accumulator_range.end = range.end.clone(),
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
        }
    }

    accumulator_range
}

pub fn get_seek_prefix(global_context: Arc<Mutex<GlobalContext>>, full_table_name: ObjectName, table: TableDef, filters: &[Expr]) -> MysqlResult<SeekType> {
    let column_filter_map = create_column_filter(filters).unwrap();

    let mut column_range_map = HashMap::new();
    for (column_name, expr_list) in column_filter_map {
        let range = create_column_range(expr_list);

        if range.start == RangePoint::Infinity && range.end == RangePoint::Infinity {
            continue;
        }

        column_range_map.insert(column_name.clone(), range);
    }

    let table_index_list = get_table_index_list(table.clone(), column_range_map);

    let result = get_seek_prefix_with_index(global_context.clone(), table.clone(), table_index_list);
    match result {
        Ok(seek_type) => Ok(seek_type),
        Err(mysql_error) => Err(mysql_error)
    }
}

pub fn get_seek_prefix_default(table: TableDef) -> SeekType {
    let scan_key = dbkey::create_scan_rowid(table.clone());
    SeekType::FullTableScan { start: scan_key.clone(), end: scan_key.clone() }
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
                    _ => continue
                }
            }
            Expr::IsNotNull(expr) => {
                match expr.as_ref() {
                    Expr::Column(value) => {
                        column_name = value.clone();
                    }
                    _ => continue
                }
            }
            Expr::BinaryExpr { left, op, right } => {
                match left.as_ref() {
                    Expr::Column(value) => {
                        column_name = value.clone();
                    }
                    _ => continue
                }
            }
            _ => continue
        }

        column_filter_map.entry(column_name.to_string()).or_insert(vec![]).push(expr.clone());
    }

    Ok(column_filter_map)
}

pub fn get_table_index_list(table_def: TableDef, column_range_map: HashMap<String, Range>) -> Vec<TableIndex> {
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
                    let range = column_range_map.get(column_name.as_str()).unwrap();

                    let column_range = ColumnRange {
                        column_name,
                        range: range.clone(),
                    };

                    column_range_list.push(column_range.clone());

                    if !range.start.eq(&range.end) {
                        break;
                    }
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
        return Ok(get_seek_prefix_default(table));
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
