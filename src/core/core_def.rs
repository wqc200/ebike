
use datafusion::sql::parser::Statement;

pub struct StmtCacheDef {
    statements: Vec<Statement>,
    param_types: Vec<u8>,
}

impl StmtCacheDef {
    pub fn new(statements: Vec<Statement>) -> Self {
        let param_types = vec![];

        Self {
            statements,
            param_types
        }
    }
}

impl StmtCacheDef {
    pub fn set_param_types(&mut self, param_types: Vec<u8>) {
        self.param_types = param_types;
    }
}