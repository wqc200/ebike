// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
use std::sync::{Mutex, Arc};

/// System variable provider
use datafusion::error::{Result};
use datafusion::variable::VarProvider;
use datafusion::scalar::ScalarValue;

use crate::core::global_context::GlobalContext;

/// System variable
pub struct SystemVar {
    global_context: Arc<Mutex<GlobalContext>>,
}

impl SystemVar {
    /// new system variable
    pub fn new(global_context: Arc<Mutex<GlobalContext>>) -> Self {
        Self {
            global_context
        }
    }
}

impl VarProvider for SystemVar {
    /// get system variable value
    fn get_value(&self, mut var_names: Vec<String>) -> Result<ScalarValue> {
        let mut value;
        let mut is_session = false;
        if var_names.len() > 1 {
            if var_names[0].eq("@@session") {
                is_session = true;
            }
            let avar_names: Vec<_> = var_names.drain(1..).collect();
            value = avar_names.join(".");
        } else {
            let a: String = var_names.join(".");
            value = a.trim_start_matches("@@").to_string();
        }

        let b = self.global_context.lock().unwrap();
        let result = b.variable.get_variable(value.as_str());
        match result {
            None => {
                let s = format!("{}-{}", env!("CARGO_PKG_VERSION").to_string(), var_names.concat());
                Ok(ScalarValue::Utf8(Option::from(s)))
            }
            Some(value) => {
                Ok(ScalarValue::Utf8(Option::from(value.clone())))
            }
        }
    }
}
