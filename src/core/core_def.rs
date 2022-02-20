use sqlparser::ast::Statement as SQLStatement;
use datafusion::sql::parser::Statement;
use std::ops::{DerefMut, Deref};

#[derive(Clone, Debug)]
pub struct StmtCacheDef {
    df_statements: Vec<Statement>,
    param_types: Vec<u8>,
}

impl StmtCacheDef {
    pub fn new(df_statements: Vec<Statement>) -> Self {
        let param_types = vec![];

        Self {
            df_statements,
            param_types,
        }
    }
}

impl StmtCacheDef {
    pub fn set_param_types(&mut self, param_types: &[u8]) {
        self.param_types = param_types.to_vec();
    }

    pub fn get_param_types(&self) -> Vec<u8> {
        self.param_types.clone()
    }

    pub fn get_statements(&self) -> Vec<Statement> {
        self.df_statements.clone()
    }
}
