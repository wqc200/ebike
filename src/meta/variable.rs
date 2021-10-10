use std::collections::{HashMap};

#[derive(Debug, Clone)]
pub struct Variable {
    variable_map: HashMap<String, String>,
}

impl Variable {
    pub fn new() -> Self {
        let variable_map: HashMap<String, String> = HashMap::new();
        Self {
            variable_map
        }
    }

    pub fn get_variable(&self, name: &str) -> Option<&String> {
        return self.variable_map.get(name)
    }

    pub fn add_variable_map(&mut self, variable_map: HashMap<String, String>) {
        for (variable_name, variable_value) in variable_map {
            self.variable_map.insert(variable_name, variable_value);
        }
    }
}
