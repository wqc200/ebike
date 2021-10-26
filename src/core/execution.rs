//! ExecutionContext contains methods for registering data sources and executing queries

use std::string::String;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use arrow::datatypes::{SchemaRef};
use arrow::record_batch::RecordBatch;

use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use datafusion::logical_plan::create_udf;
use datafusion::logical_plan::LogicalPlan;

use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::variable::VarType;
use sqlparser::ast::{AlterTableOperation, ObjectName, ObjectType, Statement as SQLStatement, Query};
use sqlparser::ast::{
    BinaryOperator, Expr as SQLExpr, JoinConstraint, JoinOperator, Select, SelectItem, SetExpr,
    TableFactor, Value,
};
use sqlparser::ast::{FunctionArg, Ident, OrderByExpr, ShowCreateObject, ShowStatementFilter};
use sqlparser::dialect::GenericDialect;
use uuid::Uuid;

use crate::core::core_util;
use crate::core::core_util as CoreUtil;
use crate::core::global_context::GlobalContext;
use crate::core::logical_plan::{CoreLogicalPlan, CoreSelectFrom, CoreSelectFromWithAssignment};
use crate::core::output::{CoreOutput, FinalCount};
use crate::core::session_context::SessionContext;
use crate::meta::{meta_const, meta_util};
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::physical_plan;
use crate::physical_plan::util::CorePhysicalPlan;

use crate::logical_plan::insert::LogicalPlanInsert;
use crate::physical_plan::delete::PhysicalPlanDelete;
use crate::physical_plan::drop_table::PhysicalPlanDropTable;
use crate::physical_plan::insert::PhysicalPlanInsert;
use crate::util::convert::{convert_ident_to_lowercase, ToLowercase, ToObjectName, ToIdent};
use crate::variable::system::SystemVar;
use crate::variable::user_defined::UserDefinedVar;

use crate::execute::drop_column::DropColumn;
use crate::execute::select_from::SelectFrom;
use crate::execute::delete_record::DeleteRecord;

/// Execution context for registering data sources and executing queries
pub struct Execution {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    datafusion_context: ExecutionContext,
    client_id: String,
}

impl Execution {
    pub fn new(global_context: Arc<Mutex<GlobalContext>>) -> Self {
        let datafusion_context = ExecutionContext::with_config(
            ExecutionConfig::new()
                .with_information_schema(true)
                .create_default_catalog_and_schema(true)
                .with_default_catalog_and_schema(
                    meta_const::CATALOG_NAME,
                    meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
                ),
        );

        let session_context = SessionContext::new_with_catalog(meta_const::CATALOG_NAME);

        let client_id = Uuid::new_v4()
            .to_simple()
            .encode_lower(&mut Uuid::encode_buffer())
            .to_string();

        Self {
            global_context,
            session_context,
            datafusion_context,
            client_id,
        }
    }
}

impl Execution {
    /// Create a new execution context for in-memory queries
    pub fn try_init(&mut self) -> MysqlResult<()> {
        let variable = UserDefinedVar::new(self.global_context.clone());
        self.datafusion_context
            .register_variable(VarType::UserDefined, Arc::new(variable));
        let variable = SystemVar::new(self.global_context.clone());
        self.datafusion_context
            .register_variable(VarType::System, Arc::new(variable));

        core_util::register_all_table(self.global_context.clone(), &mut self.datafusion_context)
            .unwrap();

        self.init_udf();

        Ok(())
    }

    pub fn init_udf(&mut self) {
        let captured_name = self.session_context.current_schema.clone();
        let database_function = move |_args: &[ArrayRef]| {
            // Lock the mutex, and read current db_name
            let captured_name = captured_name.lock().expect("mutex poisoned");
            let db_name = match captured_name.as_ref() {
                Some(s) => Some(s.as_str()),
                None => None,
            };
            let res = StringArray::from(vec![db_name]);
            Ok(Arc::new(res) as ArrayRef)
        };

        self.datafusion_context.register_udf(
            create_udf(
                "database",               // function name
                vec![],                   // input argument types
                Arc::new(DataType::Utf8), // output type
                make_scalar_function(database_function),
            ), // function implementation
        );
    }

    pub fn fix_statement(&mut self, statement: SQLStatement) -> SQLStatement {
        match statement {
            SQLStatement::Query(query) => {
                let mut new_query = query.clone();

                let mut table_alias_vec = vec![];

                match &query.body {
                    SetExpr::Select(select) => {
                        let mut new_select = select.clone();

                        // select database()
                        if new_select.from.len() == 0 {
                            let table_name = meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_DUAL
                                .to_object_name();
                            let table_with_joins = CoreUtil::build_table_with_joins(table_name);
                            new_select.from = vec![table_with_joins];
                        }

                        for i in 0..new_select.from.len() {
                            let result = self
                                .fix_table_factor(
                                    &mut table_alias_vec,
                                    new_select.from[i].relation.clone(),
                                )
                                .unwrap();
                            if let Some(new_table_factor) = result {
                                new_select.from[i].relation = new_table_factor;
                            }

                            for j in 0..new_select.from[i].joins.len() {
                                let result = self
                                    .fix_table_factor(
                                        &mut table_alias_vec,
                                        new_select.from[i].joins[j].relation.clone(),
                                    )
                                    .unwrap();
                                if let Some(new_table_factor) = result {
                                    new_select.from[i].joins[j].relation = new_table_factor;
                                }

                                let result = self
                                    .fix_join_operator(
                                        &mut table_alias_vec,
                                        &new_select.from[i].joins[j].join_operator,
                                    )
                                    .unwrap();
                                if let Some(new_join_operator) = result {
                                    new_select.from[i].joins[j].join_operator = new_join_operator;
                                }
                            }
                        }

                        for i in 0..new_select.from.len() {
                            for j in 0..new_select.from[i].joins.len() {
                                match new_select.from[i].joins[j].join_operator.clone() {
                                    JoinOperator::Inner(join_constraint) => {
                                        let result = self
                                            .fix_join_column_name(
                                                table_alias_vec.clone(),
                                                &join_constraint,
                                            )
                                            .unwrap();
                                        if let Some(new_join_constraint) = result {
                                            new_select.from[i].joins[j].join_operator =
                                                JoinOperator::Inner(new_join_constraint);
                                        }
                                    }
                                    JoinOperator::LeftOuter(join_constraint) => {
                                        let result = self
                                            .fix_join_column_name(
                                                table_alias_vec.clone(),
                                                &join_constraint,
                                            )
                                            .unwrap();
                                        if let Some(new_join_constraint) = result {
                                            new_select.from[i].joins[j].join_operator =
                                                JoinOperator::LeftOuter(new_join_constraint);
                                        }
                                    }
                                    JoinOperator::RightOuter(join_constraint) => {
                                        let result = self
                                            .fix_join_column_name(
                                                table_alias_vec.clone(),
                                                &join_constraint,
                                            )
                                            .unwrap();
                                        if let Some(new_join_constraint) = result {
                                            new_select.from[i].joins[j].join_operator =
                                                JoinOperator::RightOuter(new_join_constraint);
                                        }
                                    }
                                    JoinOperator::FullOuter(join_constraint) => {
                                        let result = self
                                            .fix_join_column_name(
                                                table_alias_vec.clone(),
                                                &join_constraint,
                                            )
                                            .unwrap();
                                        if let Some(new_join_constraint) = result {
                                            new_select.from[i].joins[j].join_operator =
                                                JoinOperator::FullOuter(new_join_constraint);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }

                        for i in 0..new_select.projection.len() {
                            match new_select.projection[i].clone() {
                                SelectItem::UnnamedExpr(sql_expr) => {
                                    let result = self
                                        .fix_column_name(table_alias_vec.clone(), &sql_expr)
                                        .unwrap();
                                    if let Some(new_sql_expr) = result {
                                        new_select.projection[i] =
                                            SelectItem::UnnamedExpr(new_sql_expr);
                                    }
                                }
                                SelectItem::ExprWithAlias { expr, alias } => {
                                    let result = self
                                        .fix_column_name(table_alias_vec.clone(), &expr)
                                        .unwrap();
                                    if let Some(new_sql_expr) = result {
                                        new_select.projection[i] = SelectItem::ExprWithAlias {
                                            expr: new_sql_expr,
                                            alias,
                                        };
                                    }
                                }
                                _ => {}
                            }
                        }

                        for i in 0..new_select.group_by.len() {
                            let sql_expr = new_select.group_by[i].clone();
                            let result = self
                                .fix_column_name(table_alias_vec.clone(), &sql_expr)
                                .unwrap();
                            if let Some(new_sql_expr) = result {
                                new_select.group_by[i] = new_sql_expr;
                            }
                        }

                        match new_select.selection.clone() {
                            None => {}
                            Some(sql_expr) => {
                                let result = self
                                    .fix_column_name(table_alias_vec.clone(), &sql_expr)
                                    .unwrap();
                                if let Some(new_sql_expr) = result {
                                    new_select.selection = Some(new_sql_expr)
                                }
                            }
                        }

                        new_query.body = SetExpr::Select(new_select.clone());
                    }
                    _ => {}
                };

                for i in 0..new_query.order_by.len() {
                    let sql_expr = new_query.order_by[i].expr.clone();
                    let result = self
                        .fix_column_name(table_alias_vec.clone(), &sql_expr)
                        .unwrap();
                    if let Some(new_sql_expr) = result {
                        new_query.order_by[i].expr = new_sql_expr;
                    }
                }

                SQLStatement::Query(new_query)
            }
            _ => statement.clone(),
        }
    }

    pub fn fix_idents(
        &mut self,
        table_alias_vec: Vec<Ident>,
        ids: Vec<Ident>,
    ) -> MysqlResult<Option<Vec<Ident>>> {
        if &ids[0].value[0..1] == "@" { // @version.comment
        } else if ids.len() == 2 && table_alias_vec.contains(&ids[0]) {
            // with alias, such as: select t.id from table1 as t;
            let mut new_ids = ids.clone();
            new_ids[1] = convert_ident_to_lowercase(&ids[1]);
            return Ok(Some(new_ids));
        } else {
            let original_column_name = ObjectName(ids.clone());
            let full_column_name =
                meta_util::fill_up_column_name(&mut self.session_context, original_column_name)
                    .unwrap();
            let full_column_name = full_column_name.to_lowercase();
            let new_ids = full_column_name.0;
            return Ok(Some(new_ids));
        }

        Ok(None)
    }

    pub fn fix_join_operator(
        &mut self,
        table_alias_vec: &mut Vec<Ident>,
        join_operator: &JoinOperator,
    ) -> MysqlResult<Option<JoinOperator>> {
        match join_operator {
            JoinOperator::Inner(join_constraint) => {
                let result = self
                    .fix_join_constraint(table_alias_vec, join_constraint)
                    .unwrap();
                if let Some(new_join_constraint) = result {
                    return Ok(Some(JoinOperator::Inner(new_join_constraint)));
                }
            }
            JoinOperator::LeftOuter(join_constraint) => {
                let result = self
                    .fix_join_constraint(table_alias_vec, join_constraint)
                    .unwrap();
                if let Some(new_join_constraint) = result {
                    return Ok(Some(JoinOperator::LeftOuter(new_join_constraint)));
                }
            }
            JoinOperator::RightOuter(join_constraint) => {
                let result = self
                    .fix_join_constraint(table_alias_vec, join_constraint)
                    .unwrap();
                if let Some(new_join_constraint) = result {
                    return Ok(Some(JoinOperator::RightOuter(new_join_constraint)));
                }
            }
            JoinOperator::FullOuter(join_constraint) => {
                let result = self
                    .fix_join_constraint(table_alias_vec, join_constraint)
                    .unwrap();
                if let Some(new_join_constraint) = result {
                    return Ok(Some(JoinOperator::FullOuter(new_join_constraint)));
                }
            }
            _ => {}
        }

        Ok(None)
    }

    pub fn fix_join_constraint(
        &mut self,
        _: &mut Vec<Ident>,
        join_constraint: &JoinConstraint,
    ) -> MysqlResult<Option<JoinConstraint>> {
        match join_constraint {
            JoinConstraint::On(_) => {}
            JoinConstraint::Using(idents) => {
                let mut new_idents = vec![];
                for ident in idents {
                    let new_ident = convert_ident_to_lowercase(ident);
                    new_idents.push(new_ident);
                }
                return Ok(Some(JoinConstraint::Using(new_idents)));
            }
            JoinConstraint::Natural => {}
            JoinConstraint::None => {}
        }

        Ok(None)
    }

    pub fn fix_table_factor(
        &mut self,
        table_alias_vec: &mut Vec<Ident>,
        table_factor: TableFactor,
    ) -> MysqlResult<Option<TableFactor>> {
        match table_factor {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
            } => {
                // select database() from dual;
                let full_table_name = if name
                    .to_string()
                    .eq(meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_DUAL)
                {
                    meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_DUAL.to_object_name()
                } else {
                    meta_util::fill_up_table_name(&mut self.session_context, name.clone()).unwrap()
                };
                let full_table_name = full_table_name.to_lowercase();

                if let Some(table_alias) = alias.clone() {
                    table_alias_vec.push(table_alias.name);
                }

                let new_table_factor = TableFactor::Table {
                    name: full_table_name,
                    alias: alias.clone(),
                    args: args.clone(),
                    with_hints: with_hints.clone(),
                };
                return Ok(Some(new_table_factor));
            }
            _ => {}
        }

        Ok(None)
    }

    pub fn fix_join_column_name(
        &mut self,
        table_alias_vec: Vec<Ident>,
        join_constraint: &JoinConstraint,
    ) -> MysqlResult<Option<JoinConstraint>> {
        match join_constraint {
            JoinConstraint::On(sql_expr) => {
                let result = self
                    .fix_column_name(table_alias_vec.clone(), sql_expr)
                    .unwrap();
                if let Some(new_sql_expr) = result {
                    let new_join_constraint = JoinConstraint::On(new_sql_expr);
                    return Ok(Some(new_join_constraint));
                }
            }
            _ => {}
        }

        Ok(None)
    }

    pub fn fix_function_column_name(
        &mut self,
        table_alias_vec: Vec<Ident>,
        function_arg: &FunctionArg,
    ) -> MysqlResult<Option<FunctionArg>> {
        match function_arg.clone() {
            FunctionArg::Unnamed(sql_expr) => {
                let result = self
                    .fix_column_name(table_alias_vec.clone(), &sql_expr)
                    .unwrap();
                if let Some(new_sql_expr) = result {
                    return Ok(Some(FunctionArg::Unnamed(new_sql_expr)));
                }
            }
            _ => {}
        }

        Ok(None)
    }

    /// make full path column name if can do
    /// make column name to lowercase
    pub fn fix_column_name(
        &mut self,
        table_alias_vec: Vec<Ident>,
        sql_expr: &SQLExpr,
    ) -> MysqlResult<Option<SQLExpr>> {
        match sql_expr.clone() {
            SQLExpr::Identifier(id) => {
                if &id.value[0..1] == "@" { // @version
                } else {
                    let new_id = convert_ident_to_lowercase(&id);
                    return Ok(Some(SQLExpr::Identifier(new_id)));
                }
            }
            SQLExpr::CompoundIdentifier(ids) => {
                let result = self.fix_idents(table_alias_vec, ids).unwrap();
                if let Some(new_ids) = result {
                    return Ok(Some(SQLExpr::CompoundIdentifier(new_ids)));
                }
            }
            SQLExpr::Function(function) => {
                let mut new_function = function.clone();
                let mut count = 0;
                for i in 0..function.args.len() {
                    let result = self
                        .fix_function_column_name(table_alias_vec.clone(), &function.args[i])
                        .unwrap();
                    if let Some(new_function_arg) = result {
                        count += 1;
                        new_function.args[i] = new_function_arg;
                    }
                }
                if count > 0 {
                    return Ok(Some(SQLExpr::Function(new_function)));
                }
            }
            SQLExpr::BinaryOp { left, op, right } => {
                let result_left = self
                    .fix_column_name(table_alias_vec.clone(), &*left)
                    .unwrap();
                let result_right = self
                    .fix_column_name(table_alias_vec.clone(), &*right)
                    .unwrap();

                if result_left.is_some() || result_right.is_some() {
                    let mut new_left = left;
                    let mut new_right = right;
                    if let Some(expr) = result_left {
                        new_left = Box::new(expr);
                    }
                    if let Some(expr) = result_right {
                        new_right = Box::new(expr);
                    }

                    let new_sql_expr = SQLExpr::BinaryOp {
                        left: new_left,
                        op,
                        right: new_right,
                    };
                    return Ok(Some(new_sql_expr));
                }
            }
            SQLExpr::Nested(box_expr) => {
                let result = self
                    .fix_column_name(table_alias_vec.clone(), &*box_expr)
                    .unwrap();
                if let Some(expr) = result {
                    let new_box_expr = Box::new(expr);
                    return Ok(Some(SQLExpr::Nested(new_box_expr)));
                }
            }
            SQLExpr::IsNull(box_expr) => {
                let result = self
                    .fix_column_name(table_alias_vec.clone(), &*box_expr)
                    .unwrap();
                if let Some(expr) = result {
                    let new_box_expr = Box::new(expr);
                    return Ok(Some(SQLExpr::IsNull(new_box_expr)));
                }
            }
            SQLExpr::IsNotNull(box_expr) => {
                let result = self
                    .fix_column_name(table_alias_vec.clone(), &*box_expr)
                    .unwrap();
                if let Some(expr) = result {
                    let new_box_expr = Box::new(expr);
                    return Ok(Some(SQLExpr::IsNotNull(new_box_expr)));
                }
            }
            SQLExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let mut change_count = 0;

                let mut new_operand = operand.clone();
                if let Some(box_sql_expr) = operand.clone() {
                    let result = self
                        .fix_box_sql_expr(table_alias_vec.clone(), box_sql_expr.clone())
                        .unwrap();
                    if let Some(new_box_sql_expr) = result {
                        change_count += 1;
                        new_operand = Some(new_box_sql_expr);
                    }
                }

                let mut new_conditions = conditions.clone();
                let result = self
                    .fix_sql_exprs(table_alias_vec.clone(), conditions.clone())
                    .unwrap();
                if let Some(new_exprs) = result {
                    change_count += 1;
                    new_conditions = new_exprs;
                }

                let mut new_results = results.clone();
                let result = self
                    .fix_sql_exprs(table_alias_vec.clone(), results.clone())
                    .unwrap();
                if let Some(new_exprs) = result {
                    change_count += 1;
                    new_results = new_exprs;
                }

                let mut new_else_result = else_result.clone();
                if let Some(box_sql_expr) = else_result.clone() {
                    let result = self
                        .fix_box_sql_expr(table_alias_vec.clone(), box_sql_expr.clone())
                        .unwrap();
                    if let Some(new_box_sql_expr) = result {
                        change_count += 1;
                        new_else_result = Some(new_box_sql_expr);
                    }
                }

                if change_count > 0 {
                    return Ok(Some(SQLExpr::Case {
                        operand: new_operand,
                        conditions: new_conditions,
                        results: new_results,
                        else_result: new_else_result,
                    }));
                }
            }
            _ => {}
        }

        Ok(None)
    }

    pub fn fix_box_sql_expr(
        &mut self,
        table_alias_vec: Vec<Ident>,
        box_sql_expr: Box<SQLExpr>,
    ) -> MysqlResult<Option<Box<SQLExpr>>> {
        let result = self
            .fix_column_name(table_alias_vec.clone(), &*box_sql_expr)
            .unwrap();
        if let Some(sql_expr) = result {
            let new_box_sql_expr = Box::new(sql_expr);
            return Ok(Some(new_box_sql_expr));
        }

        Ok(None)
    }

    pub fn fix_sql_exprs(
        &mut self,
        table_alias_vec: Vec<Ident>,
        sql_exprs: Vec<SQLExpr>,
    ) -> MysqlResult<Option<Vec<SQLExpr>>> {
        let mut new_sql_exprs = sql_exprs.clone();

        let mut change_count = 0;
        for i in 0..sql_exprs.len() {
            let expr = &sql_exprs[i];
            let result = self.fix_column_name(table_alias_vec.clone(), expr).unwrap();
            if let Some(new_expr) = result {
                change_count += 1;
                new_sql_exprs[i] = new_expr;
            }
        }

        if change_count > 0 {
            return Ok(Some(new_sql_exprs));
        }

        Ok(None)
    }

    pub fn query_projection_has_rowid(&self, query: &Query) -> bool {
        let mut has_rowid = false;

        match query.body {
            SetExpr::Select(ref select) => {
                has_rowid = core_util::projection_has_rowid(select.projection.clone());
            }
            _ => {}
        }

        has_rowid
    }

    pub fn projection_has_rowid(&self, statement: &SQLStatement) -> bool {
        let mut has_rowid = false;

        match statement {
            SQLStatement::Query(query) => {
                match &query.body {
                    SetExpr::Select(select) => {
                        has_rowid = core_util::projection_has_rowid(select.projection.clone());
                    }
                    _ => {}
                };
            }
            _ => {}
        }

        has_rowid
    }

    pub fn check_table_exists_with_name(&mut self, table_name: &ObjectName) -> MysqlResult<()> {
        let result = meta_util::resolve_table_name(&mut self.session_context, table_name);
        let full_table_name = match result {
            Ok(full_table_name) => full_table_name,
            Err(mysql_error) => return Err(mysql_error),
        };

        let schema_name = meta_util::cut_out_schema_name(full_table_name.clone());
        let table_name = meta_util::cut_out_table_name(full_table_name.clone());

        if schema_name
            .to_string()
            .eq(meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA)
        {
            let schema_provider = core_util::get_schema_provider(
                &mut self.datafusion_context,
                meta_const::CATALOG_NAME,
                meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
            );
            let table_names = schema_provider.table_names();
            if table_names.contains(&table_name.to_string()) {
                return Ok(());
            }
        }

        let result = meta_util::get_table(self.global_context.clone(), full_table_name.clone());
        if let Err(mysql_error) = result {
            return Err(mysql_error);
        }

        Ok(())
    }

    pub fn check_table_exists_in_statement(&mut self, statement: &SQLStatement) -> MysqlResult<()> {
        match statement {
            SQLStatement::Query(query) => match &query.body {
                SetExpr::Select(select) => {
                    for from in &select.from {
                        match from.relation.clone() {
                            TableFactor::Table { name, .. } => {
                                return self.check_table_exists_with_name(&name);
                            }
                            _ => {}
                        }

                        for i in 0..from.joins.clone().len() {
                            match from.joins[i].relation.clone() {
                                TableFactor::Table { name, .. } => {
                                    return self.check_table_exists_with_name(&name);
                                }
                                _ => {}
                            }
                        }
                    }
                }
                _ => {}
            },
            _ => {}
        }

        Ok(())
    }

    pub fn build_logical_plan<S: ContextProvider>(
        &mut self,
        statement: SQLStatement,
        query_planner: &SqlToRel<S>,
    ) -> MysqlResult<CoreLogicalPlan> {
        let statement = self.fix_statement(statement);
        match statement {
            SQLStatement::AlterTable { name, operation } => {
                let full_table_name =
                    meta_util::fill_up_table_name(&mut self.session_context, name.clone()).unwrap();

                let table_map = self
                    .global_context
                    .lock()
                    .unwrap()
                    .meta_data
                    .get_table_map();
                let table = match table_map.get(&full_table_name) {
                    None => {
                        return Err(meta_util::error_of_table_doesnt_exists(
                            full_table_name.clone(),
                        ))
                    }
                    Some(table) => table.clone(),
                };

                match operation.clone() {
                    AlterTableOperation::DropColumn { column_name, .. } => {
                        let sparrow_column = table
                            .column
                            .get_sparrow_column(column_name.clone())
                            .unwrap();

                        // delete column from information_schema.columns
                        let selection = core_util::build_find_column_sqlwhere(
                            table.option.catalog_name.as_ref(),
                            table.option.schema_name.as_str(),
                            table.option.table_name.as_str(),
                            column_name.to_string().as_str(),
                        );
                        let select = core_util::build_select_rowid_sqlselect(
                            meta_util::convert_to_object_name(
                                meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS,
                            ),
                            Some(selection),
                        );
                        let logical_plan =
                            query_planner.select_to_plan(&select, &mut Default::default())?;
                        let select_from_columns_for_delete = CoreSelectFrom::new(
                            meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS,
                            logical_plan,
                        );

                        // update ordinal_position from information_schema.columns
                        let ordinal_position = sparrow_column.ordinal_position;
                        let assignments = core_util::build_update_column_assignments();
                        let selection = core_util::build_find_column_ordinal_position_sqlwhere(
                            table.option.catalog_name.as_ref(),
                            table.option.schema_name.as_str(),
                            table.option.table_name.as_str(),
                            ordinal_position,
                        );
                        let select = core_util::build_update_sqlselect(
                            meta_util::convert_to_object_name(
                                meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS,
                            ),
                            assignments.clone(),
                            Some(selection),
                        );
                        let logical_plan =
                            query_planner.select_to_plan(&select, &mut Default::default())?;
                        let select_from_columns_for_update = CoreSelectFromWithAssignment::new(
                            meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS,
                            assignments,
                            logical_plan,
                        );

                        Ok(CoreLogicalPlan::AlterTableDropColumn {
                            table,
                            operation,
                            delete_columns: select_from_columns_for_delete,
                            update_columns: select_from_columns_for_update,
                        })
                    }
                    AlterTableOperation::AddColumn { .. } => {
                        Ok(CoreLogicalPlan::AlterTableAddColumn { table, operation })
                    }
                    _ => {
                        return Err(MysqlError::new_global_error(
                            1105,
                            format!(
                            "Unknown error. The drop command is not allowed with this MySQL version"
                        )
                            .as_str(),
                        ));
                    }
                }
            }
            SQLStatement::Drop {
                object_type, names, ..
            } => {
                match object_type {
                    ObjectType::Table => {
                        let name = names[0].clone();

                        let full_table_name =
                            meta_util::fill_up_table_name(&mut self.session_context, name.clone())
                                .unwrap();

                        let result = meta_util::get_table(
                            self.global_context.clone(),
                            full_table_name.clone(),
                        );
                        let table = match result {
                            Ok(table) => table.clone(),
                            Err(mysql_error) => return Err(mysql_error),
                        };

                        let catalog_name = table.option.catalog_name.to_string();
                        let schema_name = table.option.schema_name.to_string();
                        let table_name = table.option.table_name.to_string();

                        // delete table from the information_schema.columns
                        let selection = core_util::build_find_table_sqlwhere(
                            catalog_name.as_str(),
                            schema_name.as_str(),
                            table_name.as_str(),
                        );
                        let select = core_util::build_select_rowid_sqlselect(
                            meta_util::convert_to_object_name(
                                meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS,
                            ),
                            Some(selection),
                        );
                        let logical_plan_of_delete_columns =
                            query_planner.select_to_plan(&select, &mut Default::default())?;

                        // delete table from the information_schema.statistics
                        let selection = core_util::build_find_table_sqlwhere(
                            catalog_name.as_str(),
                            schema_name.as_str(),
                            table_name.as_str(),
                        );
                        let select = core_util::build_select_rowid_sqlselect(
                            meta_util::convert_to_object_name(
                                meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS,
                            ),
                            Some(selection),
                        );
                        let logical_plan_of_statistics =
                            query_planner.select_to_plan(&select, &mut Default::default())?;

                        // delete table from the information_schema.tables
                        let selection = core_util::build_find_table_sqlwhere(
                            catalog_name.as_str(),
                            schema_name.as_str(),
                            table_name.as_str(),
                        );
                        let select = core_util::build_select_rowid_sqlselect(
                            meta_util::convert_to_object_name(
                                meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES,
                            ),
                            Some(selection),
                        );
                        let logical_plan_of_tables =
                            query_planner.select_to_plan(&select, &mut Default::default())?;

                        Ok(CoreLogicalPlan::DropTable {
                            delete_columns: logical_plan_of_delete_columns,
                            delete_statistics: logical_plan_of_statistics,
                            delete_tables: logical_plan_of_tables,
                            table,
                        })
                    }
                    ObjectType::Schema => {
                        let original_schema_name = names[0].clone();
                        let full_schema_name = meta_util::fill_up_schema_name(
                            &mut self.session_context,
                            original_schema_name.clone(),
                        )
                        .unwrap();

                        let map_table_schema =
                            meta_util::read_all_schema(self.global_context.clone()).unwrap();
                        if !map_table_schema.contains_key(&full_schema_name) {
                            return Err(MysqlError::new_global_error(
                                1008,
                                format!(
                                    "Can't drop database '{}'; database doesn't exist",
                                    original_schema_name
                                )
                                .as_str(),
                            ));
                        }

                        Ok(CoreLogicalPlan::DropSchema(original_schema_name))
                    }
                    _ => {
                        return Err(MysqlError::new_global_error(
                            1105,
                            format!(
                            "Unknown error. The drop command is not allowed with this MySQL version"
                        )
                            .as_str(),
                        ));
                    }
                }
            }
            SQLStatement::ShowColumns { table_name, .. } => {
                let result = meta_util::resolve_table_name(&mut self.session_context, &table_name);
                let full_table_name = match result {
                    Ok(full_table_name) => full_table_name,
                    Err(mysql_error) => return Err(mysql_error),
                };

                let result =
                    meta_util::get_table(self.global_context.clone(), full_table_name.clone());
                let table = match result {
                    Ok(table) => table.clone(),
                    Err(mysql_error) => return Err(mysql_error),
                };

                let catalog_name = table.option.catalog_name.to_string();
                let schema_name = table.option.schema_name.to_string();
                let table_name = table.option.table_name.to_string();

                let selection = core_util::build_find_table_sqlwhere(
                    catalog_name.as_str(),
                    schema_name.as_str(),
                    table_name.as_str(),
                );

                let select = core_util::build_select_wildcard_sqlselect(
                    meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS.to_object_name(),
                    Some(selection.clone()),
                );
                let logical_plan =
                    query_planner.select_to_plan(&select, &mut Default::default())?;
                let select_from_columns = CoreSelectFrom::new(
                    meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS,
                    logical_plan,
                );

                let select = core_util::build_select_wildcard_sqlselect(
                    meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS
                        .to_object_name(),
                    Some(selection.clone()),
                );
                let logical_plan =
                    query_planner.select_to_plan(&select, &mut Default::default())?;
                let select_from_statistics = CoreSelectFrom::new(
                    meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS,
                    logical_plan,
                );

                return Ok(CoreLogicalPlan::ShowColumnsFrom {
                    select_from_columns,
                    select_from_statistics,
                });
            }
            SQLStatement::SetVariable {
                variable, value, ..
            } => {
                let idents = vec![variable];
                let variable = ObjectName(idents);
                return Ok(CoreLogicalPlan::SetVariable { variable, value });
            }
            SQLStatement::ShowCreate { obj_type, obj_name } => match obj_type {
                ShowCreateObject::Table => {
                    let table_name = obj_name;

                    let full_table_name = meta_util::fill_up_table_name(
                        &mut self.session_context,
                        table_name.clone(),
                    )
                    .unwrap();

                    let result =
                        meta_util::get_table(self.global_context.clone(), full_table_name.clone());
                    let table = match result {
                        Ok(table) => table.clone(),
                        Err(mysql_error) => return Err(mysql_error),
                    };

                    let catalog_name = table.option.catalog_name.to_string();
                    let schema_name = table.option.schema_name.to_string();
                    let table_name = table.option.table_name.to_string();

                    let selection = core_util::build_find_table_sqlwhere(
                        catalog_name.as_str(),
                        schema_name.as_str(),
                        table_name.as_str(),
                    );

                    let select = core_util::build_select_wildcard_sqlselect(
                        meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS
                            .to_object_name(),
                        Some(selection.clone()),
                    );
                    // order by
                    let order_by = OrderByExpr {
                        expr: SQLExpr::Identifier(Ident::new(meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS_ORDINAL_POSITION)),
                        asc: None,
                        nulls_first: None,
                    };
                    // create logical plan
                    let logical_plan =
                        query_planner.select_to_plan(&select, &mut Default::default())?;
                    let logical_plan = query_planner.order_by(logical_plan, &[order_by])?;
                    let select_from_columns = CoreSelectFrom::new(
                        meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS,
                        logical_plan,
                    );

                    let select = core_util::build_select_wildcard_sqlselect(
                        meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS
                            .to_object_name(),
                        Some(selection.clone()),
                    );
                    let logical_plan =
                        query_planner.select_to_plan(&select, &mut Default::default())?;
                    let select_from_statistics = CoreSelectFrom::new(
                        meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS,
                        logical_plan,
                    );

                    let select = core_util::build_select_wildcard_sqlselect(
                        meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES
                            .to_object_name(),
                        Some(selection.clone()),
                    );
                    let logical_plan =
                        query_planner.select_to_plan(&select, &mut Default::default())?;
                    let select_from_tables = CoreSelectFrom::new(
                        meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES,
                        logical_plan,
                    );

                    return Ok(CoreLogicalPlan::ShowCreateTable {
                        select_columns: select_from_columns,
                        select_statistics: select_from_statistics,
                        select_tables: select_from_tables,
                        table,
                    });
                }
                _ => {
                    let message = format!(
                        "Unsupported show create statement, show type: {:?}, show name: {:?}",
                        obj_type.clone(),
                        obj_name.clone()
                    );
                    log::error!("{}", message);
                    return Err(MysqlError::new_global_error(67, message.as_str()));
                }
            },
            SQLStatement::ShowVariable { variable } => {
                let first_variable = variable.get(0).unwrap();
                if first_variable.to_string().to_uppercase()
                    == meta_const::SHOW_VARIABLE_DATABASES.to_uppercase()
                {
                    let table_name = meta_util::convert_to_object_name(
                        meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA,
                    );
                    let full_table_name = meta_util::fill_up_table_name(
                        &mut self.session_context,
                        table_name.clone(),
                    )
                    .unwrap();

                    let projection_column = SQLExpr::Identifier(Ident {
                        value:
                            meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_SCHEMATA_SCHEMA_NAME
                                .to_string(),
                        quote_style: None,
                    });
                    let projection_column_alias = Ident::new("Database");
                    // from
                    let table_with_joins =
                        core_util::build_table_with_joins(full_table_name.clone());
                    // projection
                    let projection = vec![SelectItem::ExprWithAlias {
                        expr: projection_column,
                        alias: projection_column_alias.clone(),
                    }];
                    // order by
                    let order_by_column = SQLExpr::Identifier(projection_column_alias);
                    let order_by = OrderByExpr {
                        expr: order_by_column,
                        asc: None,
                        nulls_first: None,
                    };

                    // select
                    let select = Select {
                        distinct: false,
                        top: None,
                        projection,
                        from: vec![table_with_joins],
                        lateral_views: vec![],
                        selection: None,
                        group_by: vec![],
                        cluster_by: vec![],
                        distribute_by: vec![],
                        sort_by: vec![],
                        having: None,
                    };
                    // create logical plan
                    let logical_plan =
                        query_planner.select_to_plan(&select, &mut Default::default())?;
                    let logical_plan = query_planner.order_by(logical_plan, &[order_by])?;

                    return Ok(CoreLogicalPlan::Select(logical_plan));
                } else if first_variable.to_string().to_uppercase()
                    == meta_const::SHOW_VARIABLE_GRANTS.to_uppercase()
                {
                    let user = variable.get(2).unwrap();
                    return Ok(CoreLogicalPlan::ShowGrants { user: user.clone() });
                } else if first_variable.to_string().to_uppercase()
                    == meta_const::SHOW_VARIABLE_PRIVILEGES.to_uppercase()
                {
                    return Ok(CoreLogicalPlan::ShowPrivileges);
                } else if first_variable.to_string().to_uppercase()
                    == meta_const::SHOW_VARIABLE_ENGINES.to_uppercase()
                {
                    return Ok(CoreLogicalPlan::ShowEngines);
                } else if first_variable.to_string().to_uppercase()
                    == meta_const::SHOW_VARIABLE_CHARSET.to_uppercase()
                {
                    return Ok(CoreLogicalPlan::ShowCharset);
                } else if first_variable.to_string().to_uppercase()
                    == meta_const::SHOW_VARIABLE_COLLATION.to_uppercase()
                {
                    return Ok(CoreLogicalPlan::ShowCollation);
                }

                let message = format!("Unsupported show statement, show variable: {:?}", variable);
                log::error!("{}", message);
                Err(MysqlError::new_global_error(67, message.as_str()))
            }
            SQLStatement::ShowTableStatus { db_name, .. } => {
                // table
                let full_table_name = meta_util::convert_to_object_name(
                    meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES,
                );
                let table_with_joins = core_util::build_table_with_joins(full_table_name.clone());
                // projection
                let projection = vec![SelectItem::Wildcard];
                // selection
                let selection = SQLExpr::BinaryOp {
                    left: Box::new(SQLExpr::Identifier(Ident::new(
                        meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_SCHEMA,
                    ))),
                    op: BinaryOperator::Eq,
                    right: Box::new(SQLExpr::Value(Value::SingleQuotedString(
                        db_name.to_string(),
                    ))),
                };
                // select
                let select = Select {
                    distinct: false,
                    top: None,
                    projection,
                    from: vec![table_with_joins],
                    lateral_views: vec![],
                    selection: Some(selection),
                    group_by: vec![],
                    cluster_by: vec![],
                    distribute_by: vec![],
                    sort_by: vec![],
                    having: None,
                };
                // logical plan
                let mut logical_plan =
                    query_planner.select_to_plan(&select, &mut Default::default())?;
                logical_plan = core_util::remove_rowid_from_projection(&logical_plan);

                return Ok(CoreLogicalPlan::Select(logical_plan));
            }
            SQLStatement::ShowTables { full, db_name, .. } => {
                let db_name = match db_name {
                    None => {
                        let captured_name =
                            core_util::captured_name(self.session_context.current_schema.clone());
                        let schema_name = match captured_name {
                            Some(schema_name) => schema_name.clone(),
                            None => {
                                return Err(MysqlError::new_server_error(
                                    1046,
                                    "3D000",
                                    "No database selected",
                                ));
                            }
                        };
                        schema_name
                    }
                    Some(db_name) => {
                        let full_schema_name = meta_util::fill_up_schema_name(
                            &mut self.session_context,
                            db_name.clone(),
                        )
                        .unwrap();

                        let db_map =
                            meta_util::read_all_schema(self.global_context.clone()).unwrap();
                        if !db_map.contains_key(&full_schema_name) {
                            return Err(MysqlError::new_server_error(
                                1049,
                                "42000",
                                format!("Unknown database '{}'", db_name.to_string()).as_str(),
                            ));
                        }

                        db_name.to_string()
                    }
                };

                let column_expr = SQLExpr::Identifier(Ident {
                    value: meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_NAME
                        .to_string(),
                    quote_style: None,
                });
                let column_alias = Ident::new(format!("Tables_in_{}", db_name.as_str()));
                // table
                let full_table_name = meta_util::convert_to_object_name(
                    meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES,
                );
                let table_with_joins = core_util::build_table_with_joins(full_table_name.clone());
                // projection
                let mut projection = vec![SelectItem::ExprWithAlias {
                    expr: column_expr,
                    alias: column_alias.clone(),
                }];
                if full {
                    let column_expr = SQLExpr::Identifier(Ident {
                        value: meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_TYPE
                            .to_string(),
                        quote_style: None,
                    });
                    let column_alias = Ident::new("Table_type");
                    projection.push(SelectItem::ExprWithAlias {
                        expr: column_expr,
                        alias: column_alias.clone(),
                    });
                }
                // selection
                let selection = SQLExpr::BinaryOp {
                    left: Box::new(SQLExpr::Identifier(Ident::new(
                        meta_const::COLUMN_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES_TABLE_SCHEMA,
                    ))),
                    op: BinaryOperator::Eq,
                    right: Box::new(SQLExpr::Value(Value::SingleQuotedString(
                        db_name.to_string(),
                    ))),
                };
                // order by
                let order_by_column = SQLExpr::Identifier(column_alias);
                let order_by = OrderByExpr {
                    expr: order_by_column,
                    asc: None,
                    nulls_first: None,
                };

                // select
                let select = Select {
                    distinct: false,
                    top: None,
                    projection,
                    from: vec![table_with_joins],
                    lateral_views: vec![],
                    selection: Some(selection),
                    group_by: vec![],
                    cluster_by: vec![],
                    distribute_by: vec![],
                    sort_by: vec![],
                    having: None,
                };
                // create logical plan
                let logical_plan =
                    query_planner.select_to_plan(&select, &mut Default::default())?;
                let logical_plan = query_planner.order_by(logical_plan, &[order_by])?;

                return Ok(CoreLogicalPlan::Select(logical_plan));
            }
            SQLStatement::ShowVariables { filter } => {
                let full_table_name = meta_util::convert_to_object_name(
                    meta_const::FULL_TABLE_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES,
                );
                let table_with_joins = core_util::build_table_with_joins(full_table_name.clone());

                let mut projection = vec![];
                let sql_expr = SQLExpr::Identifier(Ident { value: meta_const::COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_NAME.to_string(), quote_style: None });
                let select_item = SelectItem::UnnamedExpr(sql_expr);
                projection.push(select_item);
                let sql_expr = SQLExpr::Identifier(Ident { value: meta_const::COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_VALUE.to_string(), quote_style: None });
                let select_item = SelectItem::UnnamedExpr(sql_expr);
                projection.push(select_item);

                let selection = match filter {
                    None => None,
                    Some(filter) => match filter {
                        ShowStatementFilter::Like(value) => {
                            let sql_expr = SQLExpr::BinaryOp {
                                    left: Box::new(SQLExpr::Identifier(Ident::new(meta_const::COLUMN_NAME_OF_DEF_PERFORMANCE_SCHEMA_GLOBAL_VARIABLES_VARIABLE_VALUE))),
                                    op: BinaryOperator::Like,
                                    right: Box::new(SQLExpr::Value(Value::SingleQuotedString(value.clone()))),
                                };
                            Some(sql_expr)
                        }
                        ShowStatementFilter::ILike(_) => None,
                        ShowStatementFilter::Where(sql_expr) => Some(sql_expr),
                    },
                };

                let select = Select {
                    distinct: false,
                    top: None,
                    projection,
                    from: vec![table_with_joins],
                    lateral_views: vec![],
                    selection,
                    group_by: vec![],
                    cluster_by: vec![],
                    distribute_by: vec![],
                    sort_by: vec![],
                    having: None,
                };
                let logical_plan =
                    query_planner.select_to_plan(&select, &mut Default::default())?;
                return Ok(CoreLogicalPlan::Select(logical_plan));
            }
            SQLStatement::Explain {
                analyze,
                verbose,
                statement,
                ..
            } => {
                let mut logical_plan =
                    query_planner.explain_statement_to_plan(verbose, analyze, &statement)?;

                let has_rowid = self.projection_has_rowid(&statement);
                if !has_rowid {
                    logical_plan = core_util::remove_rowid_from_projection(&logical_plan);
                }
                //logical_plan = core_util::explain_reset_ified_plan(&logical_plan);

                Ok(CoreLogicalPlan::Select(logical_plan))
            }
            SQLStatement::Query(ref query) => {
                let result = self.check_table_exists_in_statement(&statement);
                if let Err(mysql_error) = result {
                    return Err(mysql_error);
                }

                let result = query_planner.query_to_plan(&query);
                let mut logical_plan = match result {
                    Ok(logical_plan) => logical_plan,
                    Err(datafusion_error) => {
                        return Err(MysqlError::from(datafusion_error));
                    }
                };

                let has_rowid = self.projection_has_rowid(&statement);
                if !has_rowid {
                    logical_plan = core_util::remove_rowid_from_projection(&logical_plan);
                }

                Ok(CoreLogicalPlan::Select(logical_plan))
            }
            SQLStatement::CreateSchema { schema_name, .. } => {
                let db_name = meta_util::object_name_remove_quote(schema_name.clone());

                let full_db_name =
                    meta_util::fill_up_schema_name(&mut self.session_context, db_name.clone())
                        .unwrap();

                let db_map = meta_util::read_all_schema(self.global_context.clone()).unwrap();
                if db_map.contains_key(&full_db_name) {
                    return Err(MysqlError::new_global_error(
                        1007,
                        format!(
                            "Can't create database '{}'; database exists",
                            schema_name.to_string()
                        )
                        .as_str(),
                    ));
                }

                Ok(CoreLogicalPlan::CreateDb { db_name })
            }
            SQLStatement::CreateTable {
                name,
                columns,
                constraints,
                with_options,
                ..
            } => {
                let table_name = name.clone();

                let result = meta_util::schema_name_not_allow_exist(
                    self.global_context.clone(),
                    &mut self.session_context,
                    table_name.clone(),
                );
                if let Err(mysql_error) = result {
                    return Err(mysql_error);
                }

                Ok(CoreLogicalPlan::CreateTable {
                    table_name,
                    columns: columns.clone(),
                    constraints: constraints.clone(),
                    with_options: with_options.clone(),
                })
            }
            SQLStatement::Insert {
                table_name,
                columns,
                overwrite,
                source,
                ..
            } => {
                let logical_plan_insert = LogicalPlanInsert::new(
                    self.global_context.clone(),
                    table_name.clone(),
                    columns.clone(),
                    overwrite,
                    source.clone(),
                );
                logical_plan_insert.create_logical_plan(
                    &mut self.datafusion_context,
                    &mut self.session_context,
                    query_planner,
                )
            }
            SQLStatement::Update {
                table_name,
                assignments,
                selection,
            } => {
                let gc = self.global_context.lock().unwrap();
                let full_table_name =
                    meta_util::fill_up_table_name(&mut self.session_context, table_name.clone())
                        .unwrap();

                let result = gc.meta_data.get_table(full_table_name.clone());
                let table = match result {
                    None => {
                        let message = format!("Table '{}' doesn't exist", table_name.to_string());
                        log::error!("{}", message);
                        return Err(MysqlError::new_server_error(
                            1146,
                            "42S02",
                            message.as_str(),
                        ));
                    }
                    Some(table) => table.clone(),
                };

                let select = core_util::build_update_sqlselect(
                    table_name.clone(),
                    assignments.clone(),
                    selection,
                );
                let logical_plan =
                    query_planner.select_to_plan(&select, &mut Default::default())?;
                Ok(CoreLogicalPlan::Update {
                    logical_plan,
                    table,
                    assignments,
                })
            }
            SQLStatement::Delete {
                table_name,
                selection,
            } => {
                let full_table_name =
                    meta_util::fill_up_table_name(&mut self.session_context, table_name.clone())
                        .unwrap();

                let table_map = self
                    .global_context
                    .lock()
                    .unwrap()
                    .meta_data
                    .get_table_map();
                let table = match table_map.get(&full_table_name) {
                    None => {
                        let message = format!("Table '{}' doesn't exist", table_name.to_string());
                        log::error!("{}", message);
                        return Err(MysqlError::new_server_error(
                            1146,
                            "42S02",
                            message.as_str(),
                        ));
                    }
                    Some(table) => table.clone(),
                };

                let select =
                    core_util::build_select_rowid_sqlselect(full_table_name.clone(), selection);
                let logical_plan =
                    query_planner.select_to_plan(&select, &mut Default::default())?;

                Ok(CoreLogicalPlan::Delete {
                    logical_plan,
                    table,
                })
            }
            SQLStatement::Commit { .. } => Ok(CoreLogicalPlan::Commit),
            _ => Err(MysqlError::new_global_error(
                1105,
                format!("Unknown error. The used command is not allowed with this MySQL version")
                    .as_str(),
            )),
        }
    }

    pub fn drop_column(&mut self, table_name: ObjectName, column_name: Ident) -> MysqlResult<u64> {
        let full_table_name =
            meta_util::fill_up_table_name(&mut self.session_context, table_name.clone()).unwrap();

        let table_map = self
            .global_context
            .lock()
            .unwrap()
            .meta_data
            .get_table_map();
        let table = match table_map.get(&full_table_name) {
            None => {
                return Err(meta_util::error_of_table_doesnt_exists(
                    full_table_name.clone(),
                ))
            }
            Some(table) => table.clone(),
        };

        let sparrow_column = table
            .column
            .get_sparrow_column(column_name.clone())
            .unwrap();

        // delete column from information_schema.columns
        let selection = core_util::build_find_column_sqlwhere(
            table.option.catalog_name.as_ref(),
            table.option.schema_name.as_str(),
            table.option.table_name.as_str(),
            column_name.to_string().as_str(),
        );
        let result = self.delete_from(meta_util::create_full_table_name(
            meta_const::CATALOG_NAME,
            meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
            meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS,
        ), Some(selection)).unwrap();

        // update ordinal_position from information_schema.columns
        let ordinal_position = sparrow_column.ordinal_position;
        let assignments = core_util::build_update_column_assignments();
        let selection = core_util::build_find_column_ordinal_position_sqlwhere(
            table.option.catalog_name.as_ref(),
            table.option.schema_name.as_str(),
            table.option.table_name.as_str(),
            ordinal_position,
        );
        let select = core_util::build_update_sqlselect(
            meta_util::convert_to_object_name(
                meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS,
            ),
            assignments.clone(),
            Some(selection),
        );
        let logical_plan =
            query_planner.select_to_plan(&select, &mut Default::default())?;
        let select_from_columns_for_update = CoreSelectFromWithAssignment::new(
            meta_const::FULL_TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS,
            assignments,
            logical_plan,
        );

        Ok(CoreLogicalPlan::AlterTableDropColumn {
            table,
            operation,
            delete_columns: select_from_columns_for_delete,
            update_columns: select_from_columns_for_update,
        })
    }

    pub fn delete_from(&mut self, table_name: ObjectName, selection: Option<SQLExpr>) -> MysqlResult<u64> {
        let full_table_name =
            meta_util::fill_up_table_name(&mut self.session_context, table_name.clone())
                .unwrap();

        let table_map = self
            .global_context
            .lock()
            .unwrap()
            .meta_data
            .get_table_map();
        let table = match table_map.get(&full_table_name) {
            None => {
                let message = format!("Table '{}' doesn't exist", table_name.to_string());
                log::error!("{}", message);
                return Err(MysqlError::new_server_error(
                    1146,
                    "42S02",
                    message.as_str(),
                ));
            }
            Some(table) => table.clone(),
        };

        let select =
            core_util::build_select_rowid_sqlselect(full_table_name.clone(), selection);
        let query = Box::new(Query {
            with: None,
            body: SetExpr::Select(select.clone()),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None
        });
        let result = self.select_from(&query);
        match result {
            Ok((schema_ref, records)) => {
                let delete_record = DeleteRecord::new(self.global_context.clone(), table);
                let result = delete_record.execute(&mut self.session_context, records);
                return result;
            },
            Err(mysql_error) => Err(mysql_error),
        }
    }

    pub fn select_from(&mut self, query: &Query) -> MysqlResult<(SchemaRef, Vec<RecordBatch>)> {
        let result = self.check_table_exists_in_statement(&statement);
        if let Err(mysql_error) = result {
            return Err(mysql_error);
        }

        let state = self.datafusion_context.state.lock().unwrap().clone();
        let query_planner = SqlToRel::new(&state);

        let result = query_planner.query_to_plan(query);
        let mut logical_plan = match result {
            Ok(logical_plan) => logical_plan,
            Err(datafusion_error) => {
                return Err(MysqlError::from(datafusion_error));
            }
        };

        let has_rowid = self.query_projection_has_rowid(query);
        if !has_rowid {
            logical_plan = core_util::remove_rowid_from_projection(&logical_plan);
        }

        let result = self.datafusion_context.create_physical_plan(&logical_plan);
        let execution_plan = match result {
            Ok(execution_plan) => execution_plan,
            Err(datafusion_error) => {
                return Err(MysqlError::from(datafusion_error));
            }
        };

        let physical_plan_select = physical_plan::select::PhysicalPlanSelect::new(self.global_context.clone(), execution_plan);
        let result = physical_plan_select.execute().await;

        result
    }

    pub async fn execute_query(&mut self, sql: &str) -> MysqlResult<CoreOutput> {
        let mut new_sql = sql;
        if sql.starts_with("SET NAMES") {
            new_sql = "SET NAMES = utf8mb4"
        }
        let core_logical_plan = self.create_logical_plan(new_sql)?;
        let core_logical_plan = self.optimize(&core_logical_plan)?;
        let core_physical_plan = self.create_physical_plan(&core_logical_plan)?;
        let core_output = self.execution(&core_physical_plan).await;
        core_output
    }

    pub async fn execute_query2(&mut self, sql: &str) -> MysqlResult<CoreOutput> {
        let mut new_sql = sql;
        if sql.starts_with("SET NAMES") {
            new_sql = "SET NAMES = utf8mb4"
        }

        let dialect = &GenericDialect {};
        let statements = DFParser::parse_sql_with_dialect(sql, dialect).unwrap();

        match &statements[0] {
            Statement::Statement(statement) => {
                let statement = self.fix_statement(statement.clone());
                match statement {
                    SQLStatement::AlterTable { name, operation } => {
                        match operation.clone() {
                            AlterTableOperation::DropColumn { column_name, .. } => {
                                let drop_column = DropColumn::new(self.global_context.clone(), table_name.clone(), column_name.clone());
                                let result = drop_column.execute(&mut self.datafusion_context, &mut self.session_context);
                                match result {
                                    Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                                    Err(mysql_error) => Err(mysql_error),
                                }
                            }
                            AlterTableOperation::AddColumn { .. } => {
                                Ok(CoreLogicalPlan::AlterTableAddColumn { table, operation })
                            }
                            _ => {
                                return Err(MysqlError::new_global_error(
                                    1105,
                                    format!(
                                        "Unknown error. The drop command is not allowed with this MySQL version"
                                    )
                                        .as_str(),
                                ));
                            }
                        }
                    }
                    SQLStatement::Query(ref query) => {
                        let result = self.select_from(query);
                        match result {
                            Ok((schema_ref, records)) => Ok(CoreOutput::ResultSet(schema_ref, records)),
                            Err(mysql_error) => Err(mysql_error),
                        }
                    }
                    _ => {}
                }
            }
            _ => Err(MysqlError::new_global_error(
                1105,
                "Unknown error. The statement is not supported",
            )),
        }


        let core_logical_plan = self.create_logical_plan(new_sql)?;
        let core_logical_plan = self.optimize(&core_logical_plan)?;
        let core_physical_plan = self.create_physical_plan(&core_logical_plan)?;
        let core_output = self.execution(&core_physical_plan).await;
        core_output
    }

    pub async fn set_default_schema(&mut self, db_name: &str) -> MysqlResult<CoreOutput> {
        let schema_name = meta_util::convert_to_object_name(db_name);
        let core_logical_plan = CoreLogicalPlan::SetDefaultDb(schema_name);
        let core_physical_plan = self.create_physical_plan(&core_logical_plan)?;
        let core_output = self.execution(&core_physical_plan).await?;
        Ok(core_output)
    }

    pub async fn field_list(&mut self, table_name: &str) -> MysqlResult<CoreOutput> {
        let table_name = table_name.to_object_name();
        log::debug!("com field list table name: {}", table_name);
        let core_logical_plan = CoreLogicalPlan::ComFieldList(table_name);
        let core_physical_plan = self.create_physical_plan(&core_logical_plan)?;
        let core_output = self.execution(&core_physical_plan).await?;
        Ok(core_output)
    }

    pub fn create_logical_plan(&mut self, sql: &str) -> MysqlResult<CoreLogicalPlan> {
        let dialect = &GenericDialect {};
        let statements = DFParser::parse_sql_with_dialect(sql, dialect).unwrap();
        let state = self.datafusion_context.state.lock().unwrap().clone();
        let query_planner = SqlToRel::new(&state);

        match &statements[0] {
            Statement::Statement(statement) => {
                self.build_logical_plan(statement.clone(), &query_planner)
            }
            _ => Err(MysqlError::new_global_error(
                1105,
                "Unknown error. The statement is not supported",
            )),
        }
    }

    pub fn optimize_from_datafusion(&self, logical_plan: &LogicalPlan) -> MysqlResult<LogicalPlan> {
        let result = self.datafusion_context.optimize(logical_plan);
        match result {
            Ok(logical_plan) => Ok(logical_plan),
            Err(datafusion_error) => Err(MysqlError::from(datafusion_error)),
        }
    }

    pub fn optimize(&self, core_logical_plan: &CoreLogicalPlan) -> MysqlResult<CoreLogicalPlan> {
        match core_logical_plan {
            CoreLogicalPlan::Select(logical_plan) => {
                let logical_plan = self.optimize_from_datafusion(logical_plan)?;
                Ok(CoreLogicalPlan::Select(logical_plan))
            }
            _ => Ok(core_logical_plan.clone()),
        }
    }

    pub fn create_physical_plan(
        &mut self,
        core_logical_plan: &CoreLogicalPlan,
    ) -> MysqlResult<CorePhysicalPlan> {
        match core_logical_plan {
            CoreLogicalPlan::AlterTableDropColumn {
                table,
                operation,
                delete_columns: for_delete,
                update_columns: for_update,
            } => {
                let alter_table = physical_plan::alter_table::AlterTable::new(
                    self.global_context.clone(),
                    table.clone(),
                    operation.clone(),
                );

                let full_table_name_of_def_information_schema_columns =
                    meta_util::create_full_table_name(
                        meta_const::CATALOG_NAME,
                        meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
                        meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS,
                    );
                let table_of_def_information_schema_columns = meta_util::get_table(
                    self.global_context.clone(),
                    full_table_name_of_def_information_schema_columns,
                )
                .unwrap();

                let execution_plan = self
                    .datafusion_context
                    .create_physical_plan(for_delete.logical_plan())
                    .unwrap();
                let physical_plan_delete_from_columns =
                    physical_plan::delete::PhysicalPlanDelete::new(
                        self.global_context.clone(),
                        table_of_def_information_schema_columns.clone(),
                        execution_plan,
                    );

                let execution_plan = self
                    .datafusion_context
                    .create_physical_plan(for_update.logical_plan())
                    .unwrap();
                let physical_plan_update_from_columns = physical_plan::update::Update::new(
                    self.global_context.clone(),
                    table_of_def_information_schema_columns.clone(),
                    for_update.assignments(),
                    execution_plan,
                );

                Ok(CorePhysicalPlan::AlterTableDropColumn(
                    alter_table,
                    physical_plan_delete_from_columns,
                    physical_plan_update_from_columns,
                ))
            }
            CoreLogicalPlan::AlterTableAddColumn { table, operation } => {
                let alter_table = physical_plan::alter_table::AlterTable::new(
                    self.global_context.clone(),
                    table.clone(),
                    operation.clone(),
                );
                Ok(CorePhysicalPlan::AlterTableAddColumn(alter_table))
            }
            CoreLogicalPlan::Select(logical_plan) => {
                let result = self.datafusion_context.create_physical_plan(logical_plan);
                let execution_plan = match result {
                    Ok(execution_plan) => execution_plan,
                    Err(datafusion_error) => {
                        return Err(MysqlError::from(datafusion_error));
                    }
                };

                let select =
                    physical_plan::select::PhysicalPlanSelect::new(self.global_context.clone(), execution_plan);
                Ok(CorePhysicalPlan::Select(select))
            }
            CoreLogicalPlan::Explain(logical_plan) => {
                let execution_plan = self.datafusion_context.create_physical_plan(logical_plan)?;
                let select =
                    physical_plan::select::PhysicalPlanSelect::new(self.global_context.clone(), execution_plan);
                Ok(CorePhysicalPlan::Select(select))
            }
            CoreLogicalPlan::SetDefaultDb(object_name) => {
                let set_default_db =
                    physical_plan::set_default_schema::SetDefaultSchema::new(object_name.clone());
                Ok(CorePhysicalPlan::SetDefaultSchema(set_default_db))
            }
            CoreLogicalPlan::DropTable {
                delete_columns: logical_plan_of_columns,
                delete_statistics: logical_plan_of_statistics,
                delete_tables: logical_plan_of_tables,
                table,
            } => {
                let physical_plan_drop_table =
                    PhysicalPlanDropTable::new(self.global_context.clone(), table.clone());

                let full_table_name = meta_util::create_full_table_name(
                    meta_const::CATALOG_NAME,
                    meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
                    meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_COLUMNS,
                );
                let table_of_columns =
                    meta_util::get_table(self.global_context.clone(), full_table_name).unwrap();
                let execution_plan = self
                    .datafusion_context
                    .create_physical_plan(logical_plan_of_columns)
                    .unwrap();
                let physical_plan_delete_columns = PhysicalPlanDelete::new(
                    self.global_context.clone(),
                    table_of_columns,
                    execution_plan,
                );

                let full_table_name = meta_util::create_full_table_name(
                    meta_const::CATALOG_NAME,
                    meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
                    meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_STATISTICS,
                );
                let table_of_statistics =
                    meta_util::get_table(self.global_context.clone(), full_table_name).unwrap();
                let execution_plan = self
                    .datafusion_context
                    .create_physical_plan(logical_plan_of_statistics)
                    .unwrap();
                let physical_plan_delete_statistics = PhysicalPlanDelete::new(
                    self.global_context.clone(),
                    table_of_statistics,
                    execution_plan,
                );

                let full_table_name = meta_util::create_full_table_name(
                    meta_const::CATALOG_NAME,
                    meta_const::SCHEMA_NAME_OF_DEF_INFORMATION_SCHEMA,
                    meta_const::TABLE_NAME_OF_DEF_INFORMATION_SCHEMA_TABLES,
                );
                let table_of_tables =
                    meta_util::get_table(self.global_context.clone(), full_table_name).unwrap();
                let execution_plan = self
                    .datafusion_context
                    .create_physical_plan(logical_plan_of_tables)
                    .unwrap();
                let physical_plan_delete_tables = PhysicalPlanDelete::new(
                    self.global_context.clone(),
                    table_of_tables,
                    execution_plan,
                );

                Ok(CorePhysicalPlan::DropTable(
                    physical_plan_drop_table,
                    physical_plan_delete_columns,
                    physical_plan_delete_statistics,
                    physical_plan_delete_tables,
                ))
            }
            CoreLogicalPlan::DropSchema(db_name) => {
                let drop_db = physical_plan::drop_db::DropDB::new(
                    self.global_context.clone(),
                    db_name.clone(),
                );
                Ok(CorePhysicalPlan::DropDB(drop_db))
            }
            CoreLogicalPlan::CreateDb {
                db_name: schema_name,
            } => {
                let create_schema = physical_plan::create_db::CreateDb::new(schema_name.clone());
                Ok(CorePhysicalPlan::CreateDb(create_schema))
            }
            CoreLogicalPlan::CreateTable {
                table_name,
                columns,
                constraints,
                with_options,
            } => {
                let create_table = physical_plan::create_table::CreateTable::new(
                    self.global_context.clone(),
                    table_name.clone(),
                    columns.clone(),
                    constraints.clone(),
                    with_options.clone(),
                );
                Ok(CorePhysicalPlan::CreateTable(create_table))
            }
            CoreLogicalPlan::Insert {
                table,
                column_name_list,
                index_keys_list,
                column_value_map_list,
            } => {
                let cd = PhysicalPlanInsert::new(
                    self.global_context.clone(),
                    table.clone(),
                    column_name_list.clone(),
                    index_keys_list.clone(),
                    column_value_map_list.clone(),
                );
                Ok(CorePhysicalPlan::Insert(cd))
            }
            CoreLogicalPlan::Update {
                logical_plan,
                table,
                assignments,
            } => {
                let execution_plan = self.datafusion_context.create_physical_plan(logical_plan)?;
                let update = physical_plan::update::Update::new(
                    self.global_context.clone(),
                    table.clone(),
                    assignments.clone(),
                    execution_plan,
                );
                Ok(CorePhysicalPlan::Update(update))
            }
            CoreLogicalPlan::Delete {
                logical_plan,
                table,
            } => {
                let execution_plan = self.datafusion_context.create_physical_plan(logical_plan)?;
                let delete = physical_plan::delete::PhysicalPlanDelete::new(
                    self.global_context.clone(),
                    table.clone(),
                    execution_plan,
                );
                Ok(CorePhysicalPlan::Delete(delete))
            }
            CoreLogicalPlan::SetVariable { variable, value } => {
                let set_variable =
                    physical_plan::set_variable::SetVariable::new(variable.clone(), value.clone());
                Ok(CorePhysicalPlan::SetVariable(set_variable))
            }
            CoreLogicalPlan::ShowCreateTable {
                select_columns,
                select_statistics,
                select_tables,
                table,
            } => {
                let show_create_table = physical_plan::show_create_table::ShowCreateTable::new(
                    self.global_context.clone(),
                    table.clone(),
                );

                let execution_plan = self
                    .datafusion_context
                    .create_physical_plan(select_columns.logical_plan())
                    .unwrap();
                let select_columns =
                    physical_plan::select::PhysicalPlanSelect::new(self.global_context.clone(), execution_plan);

                let execution_plan = self
                    .datafusion_context
                    .create_physical_plan(select_statistics.logical_plan())
                    .unwrap();
                let select_statistics =
                    physical_plan::select::PhysicalPlanSelect::new(self.global_context.clone(), execution_plan);

                let execution_plan = self
                    .datafusion_context
                    .create_physical_plan(select_tables.logical_plan())
                    .unwrap();
                let select_tables =
                    physical_plan::select::PhysicalPlanSelect::new(self.global_context.clone(), execution_plan);

                Ok(CorePhysicalPlan::ShowCreateTable(
                    show_create_table,
                    select_columns,
                    select_statistics,
                    select_tables,
                ))
            }
            CoreLogicalPlan::ShowColumnsFrom {
                select_from_columns,
                select_from_statistics,
            } => {
                let show_columns_from = physical_plan::show_columns_from::ShowColumnsFrom::new(
                    self.global_context.clone(),
                );

                let execution_plan = self
                    .datafusion_context
                    .create_physical_plan(select_from_columns.logical_plan())
                    .unwrap();
                let select_columns =
                    physical_plan::select::PhysicalPlanSelect::new(self.global_context.clone(), execution_plan);

                let execution_plan = self
                    .datafusion_context
                    .create_physical_plan(select_from_statistics.logical_plan())
                    .unwrap();
                let select_statistics =
                    physical_plan::select::PhysicalPlanSelect::new(self.global_context.clone(), execution_plan);

                Ok(CorePhysicalPlan::ShowColumnsFrom(
                    show_columns_from,
                    select_columns,
                    select_statistics,
                ))
            }
            CoreLogicalPlan::ShowGrants { user } => {
                let show_grants = physical_plan::show_grants::ShowGrants::new(
                    self.global_context.clone(),
                    user.clone(),
                );
                Ok(CorePhysicalPlan::ShowGrants(show_grants))
            }
            CoreLogicalPlan::ShowPrivileges => {
                let show_privileges = physical_plan::show_privileges::ShowPrivileges::new(
                    self.global_context.clone(),
                );
                Ok(CorePhysicalPlan::ShowPrivileges(show_privileges))
            }
            CoreLogicalPlan::ShowEngines => {
                let show_engines =
                    physical_plan::show_engines::ShowEngines::new(self.global_context.clone());
                Ok(CorePhysicalPlan::ShowEngines(show_engines))
            }
            CoreLogicalPlan::ShowCharset => {
                let show_charset =
                    physical_plan::show_charset::ShowCharset::new(self.global_context.clone());
                Ok(CorePhysicalPlan::ShowCharset(show_charset))
            }
            CoreLogicalPlan::ShowCollation => {
                let show_collation =
                    physical_plan::show_collation::ShowCollation::new(self.global_context.clone());
                Ok(CorePhysicalPlan::ShowCollation(show_collation))
            }
            CoreLogicalPlan::ComFieldList(table_name) => {
                let com_field_list = physical_plan::com_field_list::ComFieldList::new(
                    self.global_context.clone(),
                    table_name.clone(),
                );
                Ok(CorePhysicalPlan::ComFieldList(com_field_list))
            }
            CoreLogicalPlan::Commit => Ok(CorePhysicalPlan::Commit),
        }
    }

    pub async fn execution(
        &mut self,
        core_physical_plan: &CorePhysicalPlan,
    ) -> MysqlResult<CoreOutput> {
        match core_physical_plan {
            CorePhysicalPlan::AlterTableAddColumn(physical_plan_alter_table) => {
                let result = physical_plan_alter_table
                    .execute(&mut self.datafusion_context, &mut self.session_context);
                match result {
                    Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::AlterTableDropColumn(
                physical_plan_alter_table,
                physical_plan_delete_from_columns,
                physical_plan_update_from_columns,
            ) => {
                let result = physical_plan_delete_from_columns
                    .execute(&mut self.session_context)
                    .await;
                if let Err(mysql_error) = result {
                    return Err(mysql_error);
                }

                let result = physical_plan_update_from_columns
                    .execute(&mut self.session_context)
                    .await;
                if let Err(mysql_error) = result {
                    return Err(mysql_error);
                }

                let result = physical_plan_alter_table
                    .execute(&mut self.datafusion_context, &mut self.session_context);
                match result {
                    Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::SetDefaultSchema(set_default_schema) => {
                let result = set_default_schema.execute(
                    &mut self.datafusion_context,
                    self.global_context.clone(),
                    &mut self.session_context,
                );
                match result {
                    Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new_with_message(
                        count,
                        0,
                        "Database changed",
                    ))),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::Select(select) => {
                let result = select.execute().await;
                match result {
                    Ok((schema_ref, records)) => Ok(CoreOutput::ResultSet(schema_ref, records)),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::Delete(delete) => {
                let result = delete.execute(&mut self.session_context).await;
                match result {
                    Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::Update(update) => {
                let result = update.execute(&mut self.session_context).await;
                match result {
                    Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::CreateDb(create_db) => {
                let result = create_db.execute(
                    self.global_context.clone(),
                    &mut self.datafusion_context,
                    &mut self.session_context,
                );
                match result {
                    Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::CreateTable(create_table) => {
                let result =
                    create_table.execute(&mut self.datafusion_context, &mut self.session_context);
                match result {
                    Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::DropDB(drop_db) => {
                let result = drop_db.execute(&mut self.session_context);
                match result {
                    Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::DropTable(
                physical_plan_drop_table,
                physical_plan_delete_columns,
                physical_plan_delete_statistics,
                physical_plan_delete_tables,
            ) => {
                let result = physical_plan_delete_columns
                    .execute(&mut self.session_context)
                    .await;
                if let Err(mysql_error) = result {
                    return Err(mysql_error);
                }

                let result = physical_plan_delete_statistics
                    .execute(&mut self.session_context)
                    .await;
                if let Err(mysql_error) = result {
                    return Err(mysql_error);
                }

                let result = physical_plan_delete_tables
                    .execute(&mut self.session_context)
                    .await;
                if let Err(mysql_error) = result {
                    return Err(mysql_error);
                }

                let result = physical_plan_drop_table
                    .execute(&mut self.datafusion_context, &mut self.session_context);
                match result {
                    Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::Insert(physical_plan_insert) => {
                let result = physical_plan_insert.execute();
                match result {
                    Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::SetVariable(set_variable) => {
                let result = set_variable.execute(
                    &mut self.datafusion_context,
                    self.global_context.clone(),
                    &mut self.session_context,
                );
                match result {
                    Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::ShowCreateTable(
                show_create_table,
                select_columns,
                select_statistics,
                select_tables,
            ) => {
                let result = select_columns.execute().await;
                let columns_record = match result {
                    Ok((_, records)) => records,
                    Err(mysql_error) => return Err(mysql_error),
                };

                let result = select_statistics.execute().await;
                let statistics_record = match result {
                    Ok((_, records)) => records,
                    Err(mysql_error) => return Err(mysql_error),
                };

                let result = select_tables.execute().await;
                let tables_record = match result {
                    Ok((_, records)) => records,
                    Err(mysql_error) => return Err(mysql_error),
                };

                let result =
                    show_create_table.execute(columns_record, statistics_record, tables_record);
                match result {
                    Ok((schema, records)) => Ok(CoreOutput::ResultSet(schema, records)),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::ShowColumnsFrom(
                show_columns_from,
                select_columns,
                select_statistics,
            ) => {
                let result = select_columns.execute().await;
                let columns_record = match result {
                    Ok((_, records)) => records,
                    Err(mysql_error) => return Err(mysql_error),
                };

                let result = select_statistics.execute().await;
                let statistics_record = match result {
                    Ok((_, records)) => records,
                    Err(mysql_error) => return Err(mysql_error),
                };

                let result = show_columns_from.execute(columns_record, statistics_record);
                match result {
                    Ok((schema, records)) => Ok(CoreOutput::ResultSet(schema, records)),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::ShowGrants(show_grants) => {
                let result = show_grants.execute();
                match result {
                    Ok((schema, records)) => Ok(CoreOutput::ResultSet(schema, records)),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::ShowPrivileges(show_privileges) => {
                let result = show_privileges.execute();
                match result {
                    Ok((schema, records)) => Ok(CoreOutput::ResultSet(schema, records)),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::ShowEngines(show_engines) => {
                let result = show_engines.execute();
                match result {
                    Ok((schema, records)) => Ok(CoreOutput::ResultSet(schema, records)),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::ShowCharset(show_charset) => {
                let result = show_charset.execute();
                match result {
                    Ok((schema, records)) => Ok(CoreOutput::ResultSet(schema, records)),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::ShowCollation(show_collation) => {
                let result = show_collation.execute();
                match result {
                    Ok((schema, records)) => Ok(CoreOutput::ResultSet(schema, records)),
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::ComFieldList(com_field_list) => {
                let result = com_field_list.execute(&mut self.session_context);
                match result {
                    Ok((schema_name, table_name, table_def)) => {
                        Ok(CoreOutput::ComFieldList(schema_name, table_name, table_def))
                    }
                    Err(mysql_error) => Err(mysql_error),
                }
            }
            CorePhysicalPlan::Commit => Ok(CoreOutput::FinalCount(FinalCount::new(0, 0))),
        }
    }
}
