use std::collections::{HashMap};
use datafusion::scalar::ScalarValue;

#[derive(Debug, Clone)]
pub struct Variable {
    variable_map: HashMap<String, ScalarValue>,
}

impl Variable {
    pub fn new() -> Self {
        let variable_map: HashMap<String, ScalarValue> = HashMap::new();
        Self {
            variable_map
        }
    }

    pub fn get_variable(&self, name: &str) -> Option<&ScalarValue> {
        return self.variable_map.get(name)
    }

    pub fn add_variable_map(&mut self, variable_map: HashMap<String, String>) {
        for (variable_name, variable_value) in variable_map {
            let value = match variable_name.as_str() {
                "transaction_read_only" => {
                    let mut value = 0;
                    if variable_value.to_lowercase().eq("on") {
                        value = 1;
                    }
                    ScalarValue::Int64(Some(value))
                }
                _ => {
                    ScalarValue::Utf8(Some(variable_value))
                }
            };
            self.variable_map.insert(variable_name, value);
        }
    }
}
