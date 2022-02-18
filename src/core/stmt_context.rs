use std::sync::Arc;
use std::sync::Mutex;

use crate::meta::variable::Variable;
use std::collections::HashMap;

use crate::core::core_def::StmtCacheDef;

#[derive(Clone, Debug)]
pub struct StmtContext {
    stmt_id: u64,
    stmts: HashMap<u64, StmtCacheDef>,
}

impl StmtContext {
    pub fn new() -> Self {
        let stmt_id = 0;
        let stmts = HashMap::new();

        Self {
            stmt_id,
            stmts,
        }
    }

    pub fn get_stmt(&self, stmt_id: u64) -> Option<&StmtCacheDef> {
        self.stmts.get(&stmt_id)
    }

    pub fn get_stmt_id(&self) -> u64 {
        self.stmt_id
    }

    pub fn add_stmt(&mut self, stmt_cache: StmtCacheDef){
        let stmt_id = self.stmt_id+1;
        self.stmt_id = stmt_id;

        self.stmts.insert(stmt_id, stmt_cache);
    }
}

