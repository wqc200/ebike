//! ExecutionContext contains methods for registering data sources and executing queries

use std::string::String;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use datafusion::logical_plan::create_udf;
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::variable::VarType;
use sqlparser::ast::{
    AlterTableOperation, Assignment, BinaryOperator, ColumnDef, Expr as SQLExpr, JoinConstraint,
    JoinOperator, ObjectName, ObjectType, Query, Select, SelectItem, SetExpr,
    Statement as SQLStatement, TableFactor, Value,
};
use sqlparser::ast::{FunctionArg, Ident, OrderByExpr, ShowCreateObject, ShowStatementFilter};
use sqlparser::dialect::GenericDialect;
use uuid::Uuid;

use crate::core::core_util;
use crate::core::core_util as CoreUtil;
use crate::core::core_util::register_all_table;
use crate::core::global_context::GlobalContext;
use crate::core::logical_plan::{CoreLogicalPlan, CoreSelectFrom, CoreSelectFromWithAssignment};
use crate::core::output::{CoreOutput, FinalCount, ResultSet};
use crate::core::session_context::SessionContext;
use crate::execute_impl::add_column::AddColumn;
use crate::execute_impl::com_field_list::ComFieldList;
use crate::execute_impl::create_db::CreateDb;
use crate::execute_impl::create_table::CreateTable;
use crate::execute_impl::delete::DeleteFrom;
use crate::execute_impl::drop_column::DropColumn;
use crate::execute_impl::drop_schema::DropSchema;
use crate::execute_impl::drop_table::DropTable;
use crate::execute_impl::explain::Explain;
use crate::execute_impl::insert::Insert;
use crate::execute_impl::select::SelectFrom;
use crate::execute_impl::set_default_schema::SetDefaultSchema;
use crate::execute_impl::set_variable::SetVariable;
use crate::execute_impl::show_charset::ShowCharset;
use crate::execute_impl::show_collation::ShowCollation;
use crate::execute_impl::show_columns_from_table::ShowColumns;
use crate::execute_impl::show_databases::ShowDatabases;
use crate::execute_impl::show_engines::ShowEngines;
use crate::execute_impl::show_grants::ShowGrants;
use crate::execute_impl::show_privileges::ShowPrivileges;
use crate::execute_impl::show_tables::ShowTables;
use crate::execute_impl::show_variables::ShowVariables;
use crate::execute_impl::update::Update;
use crate::meta::meta_util::load_all_table;
use crate::meta::{meta_const, meta_util};
use crate::mysql::error::{MysqlError, MysqlResult};
use crate::util::convert::{convert_ident_to_lowercase, ToIdent, ToLowercase, ToObjectName};
use crate::variable::system::SystemVar;
use crate::variable::user_defined::UserDefinedVar;

/// Execution context for registering data sources and executing queries
pub struct Execution {
    global_context: Arc<Mutex<GlobalContext>>,
    session_context: SessionContext,
    execution_context: ExecutionContext,
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
            execution_context: datafusion_context,
            client_id,
        }
    }
}

impl Execution {
    /// Create a new execution context for in-memory queries
    pub fn try_init(&mut self) -> MysqlResult<()> {
        let variable = UserDefinedVar::new(self.global_context.clone());
        self.execution_context
            .register_variable(VarType::UserDefined, Arc::new(variable));
        let variable = SystemVar::new(self.global_context.clone());
        self.execution_context
            .register_variable(VarType::System, Arc::new(variable));

        core_util::register_all_table(self.global_context.clone(), &mut self.execution_context)
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

        self.execution_context.register_udf(
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
                &mut self.execution_context,
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

    pub fn check_table_exists(&mut self, query: &Query) -> MysqlResult<()> {
        match &query.body {
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
        }
        Ok(())
    }

    pub async fn execute_query(&mut self, sql: &str) -> MysqlResult<CoreOutput> {
        let mut new_sql = sql;
        if sql.starts_with("SET NAMES") {
            new_sql = "SET NAMES = utf8mb4"
        }

        let dialect = &GenericDialect {};
        let statements = DFParser::parse_sql_with_dialect(new_sql, dialect).unwrap();

        match &statements[0] {
            Statement::Statement(statement) => {
                let statement = self.fix_statement(statement.clone());
                match statement {
                    SQLStatement::AlterTable { name, operation } => {
                        let table_name = name;
                        match operation.clone() {
                            AlterTableOperation::DropColumn { column_name, .. } => {
                                let mut drop_column = DropColumn::new(
                                    self.global_context.clone(),
                                    self.session_context.clone(),
                                    self.execution_context.clone(),
                                );
                                let result = drop_column.execute(table_name, column_name).await;
                                match result {
                                    Ok(count) => {
                                        Ok(CoreOutput::FinalCount(FinalCount::new(count, 0)))
                                    }
                                    Err(mysql_error) => Err(mysql_error),
                                }
                            }
                            AlterTableOperation::AddColumn { column_def } => {
                                let mut add_column = AddColumn::new(
                                    self.global_context.clone(),
                                    self.session_context.clone(),
                                    self.execution_context.clone(),
                                );
                                let result = add_column.execute(table_name, column_def);
                                match result {
                                    Ok(count) => {
                                        Ok(CoreOutput::FinalCount(FinalCount::new(count, 0)))
                                    }
                                    Err(mysql_error) => Err(mysql_error),
                                }
                            }
                            _ => {
                                return Err(MysqlError::new_global_error(
                                    1105,
                                    format!(
                                        "Unknown error. The drop command is not allowed with this MySQL version"
                                    ).as_str(),
                                ));
                            }
                        }
                    }
                    SQLStatement::CreateSchema { schema_name, .. } => {
                        let db_name = meta_util::object_name_remove_quote(schema_name.clone());

                        let mut create_db = CreateDb::new(
                            self.global_context.clone(),
                            self.session_context.clone(),
                            self.execution_context.clone(),
                        );
                        let result = create_db.execute(db_name);
                        match result {
                            Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                            Err(mysql_error) => Err(mysql_error),
                        }
                    }
                    SQLStatement::CreateTable {
                        name,
                        columns,
                        constraints,
                        with_options,
                        ..
                    } => {
                        let table_name = name.clone();

                        let mut create_table = CreateTable::new(
                            self.global_context.clone(),
                            self.session_context.clone(),
                            self.execution_context.clone(),
                        );
                        let result = create_table
                            .execute(table_name, columns, constraints, with_options);
                        match result {
                            Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                            Err(mysql_error) => Err(mysql_error),
                        }
                    }
                    SQLStatement::Insert {
                        table_name,
                        columns,
                        overwrite,
                        source,
                        ..
                    } => {
                        let mut insert = Insert::new(
                            self.global_context.clone(),
                            self.session_context.clone(),
                            self.execution_context.clone(),
                        );
                        let result = insert.execute(table_name, columns, overwrite, source);
                        match result {
                            Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                            Err(mysql_error) => Err(mysql_error),
                        }
                    }
                    SQLStatement::Query(ref query) => {
                        let mut select_from = SelectFrom::new(
                            self.global_context.clone(),
                            self.session_context.clone(),
                            self.execution_context.clone(),
                        );
                        let result = select_from.execute(query).await;
                        match result {
                            Ok(result_set) => Ok(CoreOutput::ResultSet(result_set)),
                            Err(mysql_error) => Err(mysql_error),
                        }
                    }
                    SQLStatement::Explain {
                        analyze,
                        verbose,
                        statement,
                        ..
                    } => {
                        let mut explain = Explain::new(
                            self.global_context.clone(),
                            self.session_context.clone(),
                            self.execution_context.clone(),
                        );
                        let result = explain.execute(verbose, analyze, &statement).await;
                        match result {
                            Ok(result_set) => Ok(CoreOutput::ResultSet(result_set)),
                            Err(mysql_error) => Err(mysql_error),
                        }
                    }
                    SQLStatement::Drop {
                        object_type, names, ..
                    } => match object_type {
                        ObjectType::Table => {
                            let table_name = names[0].clone();

                            let mut drop_table = DropTable::new(
                                self.global_context.clone(),
                                self.session_context.clone(),
                                self.execution_context.clone(),
                            );
                            let result = drop_table.execute(table_name).await;
                            match result {
                                Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                                Err(mysql_error) => Err(mysql_error),
                            }
                        }
                        ObjectType::Schema => {
                            let schema_name = names[0].clone();

                            let mut drop_schema = DropSchema::new(
                                self.global_context.clone(),
                                self.session_context.clone(),
                                self.execution_context.clone(),
                            );
                            let result = drop_schema.execute(schema_name).await;
                            match result {
                                Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                                Err(mysql_error) => Err(mysql_error),
                            }
                        }
                        _ => {
                            return Err(MysqlError::new_global_error(
                                1105,
                                format!(
                                    "Unknown error. The drop command is not allowed with this MySQL version"
                                ).as_str(),
                            ));
                        }
                    },
                    SQLStatement::ShowColumns { table_name, .. } => {
                        let mut show_columns = ShowColumns::new(
                            self.global_context.clone(),
                            self.session_context.clone(),
                            self.execution_context.clone(),
                        );
                        let result = show_columns.execute(&table_name).await;
                        match result {
                            Ok(result_set) => Ok(CoreOutput::ResultSet(result_set)),
                            Err(mysql_error) => Err(mysql_error),
                        }
                    }
                    SQLStatement::SetVariable {
                        variable, value, ..
                    } => {
                        let idents = vec![variable];
                        let variable = ObjectName(idents);

                        let mut drop_schema = SetVariable::new(
                            self.global_context.clone(),
                            self.session_context.clone(),
                            self.execution_context.clone(),
                        );
                        let result = drop_schema.execute();
                        match result {
                            Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new(count, 0))),
                            Err(mysql_error) => Err(mysql_error),
                        }
                    }
                    SQLStatement::ShowVariable { variable } => {
                        let first_variable = variable.get(0).unwrap();
                        if first_variable.to_string().to_uppercase()
                            == meta_const::SHOW_VARIABLE_DATABASES.to_uppercase()
                        {
                            let mut show_databases = ShowDatabases::new(
                                self.global_context.clone(),
                                self.session_context.clone(),
                                self.execution_context.clone(),
                            );
                            let result = show_databases.execute().await;
                            match result {
                                Ok(result_set) => Ok(CoreOutput::ResultSet(result_set)),
                                Err(mysql_error) => Err(mysql_error),
                            }
                        } else if first_variable.to_string().to_uppercase()
                            == meta_const::SHOW_VARIABLE_GRANTS.to_uppercase()
                        {
                            let mut show_grants = ShowGrants::new(
                                self.global_context.clone(),
                                self.session_context.clone(),
                                self.execution_context.clone(),
                            );
                            let result = show_grants.execute();
                            match result {
                                Ok(result_set) => Ok(CoreOutput::ResultSet(result_set)),
                                Err(mysql_error) => Err(mysql_error),
                            }
                        } else if first_variable.to_string().to_uppercase()
                            == meta_const::SHOW_VARIABLE_PRIVILEGES.to_uppercase()
                        {
                            let mut show_privileges = ShowPrivileges::new(
                                self.global_context.clone(),
                                self.session_context.clone(),
                                self.execution_context.clone(),
                            );
                            let result = show_privileges.execute();
                            match result {
                                Ok(result_set) => Ok(CoreOutput::ResultSet(result_set)),
                                Err(mysql_error) => Err(mysql_error),
                            }
                        } else if first_variable.to_string().to_uppercase()
                            == meta_const::SHOW_VARIABLE_ENGINES.to_uppercase()
                        {
                            let mut show_engines = ShowEngines::new(
                                self.global_context.clone(),
                                self.session_context.clone(),
                                self.execution_context.clone(),
                            );
                            let result = show_engines.execute();
                            match result {
                                Ok(result_set) => Ok(CoreOutput::ResultSet(result_set)),
                                Err(mysql_error) => Err(mysql_error),
                            }
                        } else if first_variable.to_string().to_uppercase()
                            == meta_const::SHOW_VARIABLE_CHARSET.to_uppercase()
                        {
                            let mut show_charset = ShowCharset::new(
                                self.global_context.clone(),
                                self.session_context.clone(),
                                self.execution_context.clone(),
                            );
                            let result = show_charset.execute();
                            match result {
                                Ok(result_set) => Ok(CoreOutput::ResultSet(result_set)),
                                Err(mysql_error) => Err(mysql_error),
                            }
                        } else if first_variable.to_string().to_uppercase()
                            == meta_const::SHOW_VARIABLE_COLLATION.to_uppercase()
                        {
                            let mut show_collation = ShowCollation::new(
                                self.global_context.clone(),
                                self.session_context.clone(),
                                self.execution_context.clone(),
                            );
                            let result = show_collation.execute();
                            match result {
                                Ok(result_set) => Ok(CoreOutput::ResultSet(result_set)),
                                Err(mysql_error) => Err(mysql_error),
                            }
                        }

                        let message =
                            format!("Unsupported show statement, show variable: {:?}", variable);
                        log::error!("{}", message);
                        Err(MysqlError::new_global_error(67, message.as_str()))
                    }
                    SQLStatement::ShowTables { full, db_name, .. } => {
                        let mut show_tables = ShowTables::new(
                            self.global_context.clone(),
                            self.session_context.clone(),
                            self.execution_context.clone(),
                        );
                        let result = show_tables.execute(full, db_name).await;
                        match result {
                            Ok(result_set) => Ok(CoreOutput::ResultSet(result_set)),
                            Err(mysql_error) => Err(mysql_error),
                        }
                    }
                    SQLStatement::ShowVariables { filter } => {
                        let mut show_variables = ShowVariables::new(
                            self.global_context.clone(),
                            self.session_context.clone(),
                            self.execution_context.clone(),
                        );
                        let result = show_variables.execute(filter).await;
                        match result {
                            Ok(result_set) => Ok(CoreOutput::ResultSet(result_set)),
                            Err(mysql_error) => Err(mysql_error),
                        }
                    }
                    SQLStatement::Commit { .. } => {
                        Ok(CoreOutput::FinalCount(FinalCount::new(0, 0)))
                    }
                    _ => Err(MysqlError::new_global_error(
                        1105,
                        "Unknown error. The statement is not supported",
                    )),
                }
            }
            _ => Err(MysqlError::new_global_error(
                1105,
                "Unknown error. The statement is not supported",
            )),
        }
    }

    pub async fn set_default_schema(&mut self, db_name: &str) -> MysqlResult<CoreOutput> {
        let schema_name = meta_util::convert_to_object_name(db_name);

        let mut set_default_schema = SetDefaultSchema::new(
            self.global_context.clone(),
            self.session_context.clone(),
            self.execution_context.clone(),
        );
        let result = set_default_schema.execute(schema_name);
        match result {
            Ok(count) => Ok(CoreOutput::FinalCount(FinalCount::new_with_message(
                count,
                0,
                "Database changed",
            ))),
            Err(mysql_error) => Err(mysql_error),
        }
    }

    pub async fn com_field_list(&mut self, table_name: &str) -> MysqlResult<CoreOutput> {
        let table_name = table_name.to_object_name();
        log::debug!("com field list table name: {}", table_name);

        let mut com_field_list = ComFieldList::new(
            self.global_context.clone(),
            self.session_context.clone(),
            self.execution_context.clone(),
        );
        let result = com_field_list.execute(table_name.clone());
        match result {
            Ok((schema_name, table_name, table_def)) => Ok(CoreOutput::ComFieldList(schema_name, table_name, table_def)),
            Err(mysql_error) => Err(mysql_error),
        }
    }

    pub fn optimize_from_datafusion(&self, logical_plan: &LogicalPlan) -> MysqlResult<LogicalPlan> {
        let result = self.execution_context.optimize(logical_plan);
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
}
