use std::sync::Arc;
use std::string::String;
use std::ops::{Deref, DerefMut};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::fmt;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::{Result};
use datafusion::logical_plan::{LogicalPlanBuilder, LogicalPlan, Expr};
use datafusion::execution::context::ExecutionContext;
use sqlparser::ast::{AlterTableOperation, Assignment, ColumnDef, ObjectName, SqlOption, TableConstraint};

#[derive(Clone)]
pub enum Statement {
    CreateTable {
        table_name: String,
        if_not_exists: bool,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
    }
}

impl fmt::Display for Statement {
    // Clippy thinks this function is too complicated, but it is painful to
    // split up without extracting structs for each `Statement` variant.
    #[allow(clippy::cognitive_complexity)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Statement::CreateTable {
                table_name,
                if_not_exists,
                columns,
                constraints,
            } => {
                // We want to allow the following options
                // Empty column list, allowed by PostgreSQL:
                //   `CREATE TABLE t ()`
                // No columns provided for CREATE TABLE AS:
                //   `CREATE TABLE t AS SELECT a from t2`
                // Columns provided for CREATE TABLE AS:
                //   `CREATE TABLE t (a INT) AS SELECT a from t2`
                write!(
                    f,
                    "CREATE TABLE {if_not_exists}{table_name}",
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    table_name = table_name,
                )?;

                if !columns.is_empty() || !constraints.is_empty() {
                    write!(f, " ({}", display_comma_separated(columns))?;
                    if !columns.is_empty() && !constraints.is_empty() {
                        write!(f, ", ")?;
                    }
                    write!(f, "{})", display_comma_separated(constraints))?;
                }

                Ok(())
            }
        }
    }
}

fn display_comma_separated<T>(slice: &[T]) -> DisplaySeparated<'_, T>
    where
        T: fmt::Display,
{
    DisplaySeparated { slice, sep: ", " }
}

struct DisplaySeparated<'a, T>
    where
        T: fmt::Display,
{
    slice: &'a [T],
    sep: &'static str,
}

impl<'a, T> fmt::Display for DisplaySeparated<'a, T>
    where
        T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut delim = "";
        for t in self.slice {
            write!(f, "{}", delim)?;
            delim = self.sep;
            write!(f, "{}", t)?;
        }
        Ok(())
    }
}