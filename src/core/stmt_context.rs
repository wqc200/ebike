use std::sync::Arc;
use std::sync::Mutex;

use crate::meta::variable::Variable;
use std::collections::HashMap;

use crate::core::core_def::StmtCacheDef;
use std::collections::hash_map::RandomState;

#[derive(Clone, Debug)]
pub struct StmtContext {
    pub stmt_id: u32,
    pub stmts: HashMap<u32, StmtCacheDef>,
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

    pub fn remove_stmt(&mut self, stmt_id: u32) {
        self.stmts.remove(&stmt_id);
    }

    pub fn add_stmt(&mut self, stmt_cache: StmtCacheDef){
        let stmt_id = self.stmt_id+1;
        self.stmt_id = stmt_id;

        self.stmts.insert(stmt_id, stmt_cache);
    }
}

