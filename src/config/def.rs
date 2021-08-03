use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct MyConfig {
    pub server: ConfigServer,
    pub engine: ConfigEngine,
}

/// `MyConfig` implements `Default`
impl ::std::default::Default for MyConfig {
    fn default() -> Self {
        Self {
            server: ConfigServer::default(),
            engine: ConfigEngine::default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfigServer {
    pub log_file: String,
    pub bind_host: String,
    pub schema_engine: String,
}

/// `ConfigServer` implements `Default`
impl ::std::default::Default for ConfigServer {
    fn default() -> Self {
        Self {
            log_file: "./log4rs.yaml".into(),
            bind_host: "0.0.0.0:3307".into(),
            schema_engine: "sled".into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfigEngine {
    pub sled: EngineSled,
}

impl ::std::default::Default for ConfigEngine {
    fn default() -> Self {
        Self {
            sled: EngineSled::default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EngineSled {
    pub data_path: String,
}

impl ::std::default::Default for EngineSled {
    fn default() -> Self {
        Self {
            data_path: "F:/Data/sparrow".into(),
        }
    }
}