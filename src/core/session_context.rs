use std::sync::Arc;
use std::sync::Mutex;

use crate::meta::variable::Variable;

#[derive(Clone, Debug)]
pub struct SessionContext {
    pub current_catalog: Arc<Mutex<Option<String>>>,
    pub current_schema: Arc<Mutex<Option<String>>>,
    pub variable: Variable,
}

impl SessionContext {
    pub fn new() -> Self {
        let variable = Variable::new();

        Self {
            current_catalog: Arc::new(Mutex::new(None)),
            current_schema: Arc::new(Mutex::new(None)),
            variable,
        }
    }

    pub fn new_with_catalog(catalog_name: &str) -> Self {
        let variable = Variable::new();

        Self {
            current_catalog: Arc::new(Mutex::new(Some(catalog_name.to_string()))),
            current_schema: Arc::new(Mutex::new(None)),
            variable,
        }
    }

    pub fn new_with_catalog_schema(catalog_name: &str, schema_name: &str) -> Self {
        let variable = Variable::new();

        Self {
            current_catalog: Arc::new(Mutex::new(Some(catalog_name.to_string()))),
            current_schema: Arc::new(Mutex::new(Some(schema_name.to_string()))),
            variable,
        }
    }
}

