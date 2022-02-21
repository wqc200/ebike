use sqlparser::ast::Statement as SQLStatement;
use datafusion::sql::parser::Statement;
use std::ops::{DerefMut, Deref};

#[derive(Clone, Debug)]
pub struct StmtCacheDef {
    num_params: usize,
    df_statements: Vec<Statement>,
    param_types: Vec<u8>,
}

impl StmtCacheDef {
    pub fn new(df_statements: Vec<Statement>) -> Self {
        let num_params = 0;
        let param_types = vec![];

        Self {
            num_params,
            df_statements,
            param_types,
        }
    }
}

impl StmtCacheDef {
    pub fn set_num_params(&mut self, num_params: usize) {
        self.num_params = num_params;
    }

    pub fn set_param_types(&mut self, param_types: &[u8]) {
        self.param_types = param_types.to_vec();
    }

    pub fn get_num_params(&self) -> usize {
        self.num_params
    }

    pub fn get_param_types(&self) -> Vec<u8> {
        self.param_types.clone()
    }

    pub fn get_statements(&self) -> Vec<Statement> {
        self.df_statements.clone()
    }
}
