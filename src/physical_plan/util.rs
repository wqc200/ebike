use std::sync::Arc;

use datafusion::error::{Result};
use datafusion::physical_plan::ExecutionPlan;

use crate::core::logical_plan::CoreLogicalPlan;
use crate::core::output::CoreOutput;
use crate::physical_plan::alter_table::AlterTable;
use crate::physical_plan::select::Select;
use crate::physical_plan::delete::Delete;
use crate::physical_plan::update::Update;
use crate::physical_plan::insert::Insert;
use crate::physical_plan::create_db::CreateDb;
use crate::physical_plan::drop_db::DropDB;
use crate::physical_plan::drop_table::DropTable;
use crate::physical_plan::create_table::CreateTable;
use crate::physical_plan::set_default_schema::SetDefaultSchema;
use crate::physical_plan::show_create_table::ShowCreateTable;
use crate::physical_plan::show_columns_from::ShowColumnsFrom;
use crate::physical_plan::com_field_list::ComFieldList;
use crate::physical_plan::set_variable::SetVariable;
use crate::physical_plan::show_grants::ShowGrants;
use crate::physical_plan::show_privileges::ShowPrivileges;
use crate::physical_plan::show_engines::ShowEngines;
use crate::physical_plan::show_charset::ShowCharset;
use crate::physical_plan::show_collation::ShowCollation;

pub enum CorePhysicalPlan {
    AlterTableAddColumn(AlterTable),
    AlterTableDropColumn(AlterTable, Delete, Update),
    CreateDb(CreateDb),
    CreateTable(CreateTable),
    SetDefaultSchema(SetDefaultSchema),
    DropTable(DropTable, Delete, Delete, Delete),
    DropDB(DropDB),
    Delete(Delete),
    Insert(Insert),
    Select(Select),
    Update(Update),
    SetVariable(SetVariable),
    ShowCreateTable(ShowCreateTable, Select, Select, Select),
    ShowColumnsFrom(ShowColumnsFrom, Select, Select),
    ShowGrants(ShowGrants),
    ShowPrivileges(ShowPrivileges),
    ShowEngines(ShowEngines),
    ShowCharset(ShowCharset),
    ShowCollation(ShowCollation),
    ComFieldList(ComFieldList),
    Commit,
}

