
use std::string::String;

use datafusion::logical_plan::{LogicalPlan};
use sqlparser::ast::{AlterTableOperation, Assignment, ColumnDef, Ident, ObjectName, SetVariableValue, SqlOption, TableConstraint};

use std::collections::HashMap;
use datafusion::scalar::ScalarValue;
use crate::meta::meta_def::{TableDef, IndexDef};

#[derive(Clone)]
pub struct CoreSelectFrom {
    schema_name: String,
    logical_plan: LogicalPlan,
}

impl CoreSelectFrom {
    pub fn new(schema_name: &str, logical_plan: LogicalPlan) -> Self {
        Self {
            schema_name: schema_name.to_string(),
            logical_plan,
        }
    }
}

impl CoreSelectFrom {
    pub fn schema_name(&self) -> String {
        self.schema_name.clone()
    }

    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }
}

#[derive(Clone)]
pub struct CoreSelectFromWithAssignment {
    schema_name: String,
    assignments: Vec<Assignment>,
    logical_plan: LogicalPlan,
}

impl CoreSelectFromWithAssignment {
    pub fn new(schema_name: &str, assignments: Vec<Assignment>, logical_plan: LogicalPlan) -> Self {
        Self {
            schema_name: schema_name.to_string(),
            assignments,
            logical_plan,
        }
    }
}

impl CoreSelectFromWithAssignment {
    pub fn schema_name(&self) -> String {
        self.schema_name.clone()
    }

    pub fn assignments(&self) -> Vec<Assignment> {
        self.assignments.clone()
    }

    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }
}

#[derive(Clone)]
pub enum CoreLogicalPlan {
    AlterTableAddColumn {
        table: TableDef,
        operation: AlterTableOperation,
    },
    AlterTableDropColumn {
        table: TableDef,
        operation: AlterTableOperation,
        delete_columns: CoreSelectFrom,
        update_columns: CoreSelectFromWithAssignment,
    },
    SetDefaultDb(ObjectName),
    ComFieldList(ObjectName),
    DropSchema(ObjectName),
    DropTable {
        delete_columns: LogicalPlan,
        delete_statistics: LogicalPlan,
        delete_tables: LogicalPlan,
        table: TableDef,
    },
    Select(LogicalPlan),
    Explain(LogicalPlan),
    Delete {
        logical_plan: LogicalPlan,
        table: TableDef,
    },
    Update {
        logical_plan: LogicalPlan,
        table: TableDef,
        assignments: Vec<Assignment>,
    },
    Insert {
        table: TableDef,
        column_name_list: Vec<String>,
        index_keys_list: Vec<Vec<IndexDef>>,
        column_value_map_list: Vec<HashMap<Ident, ScalarValue>>,
    },
    CreateDb {
        db_name: ObjectName,
    },
    CreateTable {
        table_name: ObjectName,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
        with_options: Vec<SqlOption>,
    },
    ShowCreateTable {
        select_columns: CoreSelectFrom,
        select_statistics: CoreSelectFrom,
        select_tables: CoreSelectFrom,
        table: TableDef,
    },
    ShowColumnsFrom {
        select_from_columns: CoreSelectFrom,
        select_from_statistics: CoreSelectFrom,
    },
    ShowGrants {
        user: Ident,
    },
    ShowPrivileges,
    ShowEngines,
    ShowCharset,
    ShowCollation,
    SetVariable {
        variable: ObjectName,
        value: Vec<SetVariableValue>,
    },
    Commit,
}

