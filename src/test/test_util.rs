use crate::config::def::MyConfig;
use crate::core::execution::Execution;
use crate::core::global_context::GlobalContext;
use crate::meta::{initial, meta_util};
use crate::mysql::error::MysqlResult;
use log::LevelFilter;
use log4rs::append::console::{ConsoleAppender, Target};
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::filter::threshold::ThresholdFilter;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use uuid::Uuid;

pub async fn create_execution() -> MysqlResult<Execution> {
    let mut my_config = MyConfig::default();
    let test_id = Uuid::new_v4()
        .to_simple()
        .encode_lower(&mut Uuid::encode_buffer())
        .to_string();
    my_config.engine.sled.data_path = format!("./data/test/{}/sled", test_id);

    let global_context = Arc::new(Mutex::new(GlobalContext::new_with_config(
        my_config.clone(),
    )));

    let level = log::LevelFilter::Info;
    let file_path = format!("./data/test/{}/log/log4rs.log", test_id);

    // Build a stderr logger.
    let stderr = ConsoleAppender::builder().target(Target::Stderr).build();
    
    // Log Trace level output to file where trace is the default level
    // and the programmatically specified level to stderr.
    let config = Config::builder()
        .appender(
            Appender::builder()
                .filter(Box::new(ThresholdFilter::new(level)))
                .build("stderr", Box::new(stderr)),
        )
        .build(
            Root::builder()
                .appender("stderr")
                .build(LevelFilter::Trace),
        )
        .unwrap();

    log4rs::init_config(config).unwrap();

    let result = meta_util::init_meta(global_context.clone()).await;
    if let Err(mysql_error) = result {
        log::error!("init meta error: {}", mysql_error);
        return Err(mysql_error);
    }

    let result = meta_util::load_global_variable(global_context.clone());
    if let Err(mysql_error) = result {
        log::error!("load global variable error: {}", mysql_error);
        return Err(mysql_error);
    }

    let result = meta_util::read_all_schema(global_context.clone());
    match result {
        Ok(schema_map) => {
            global_context
                .lock()
                .unwrap()
                .meta_data
                .add_all_schema(schema_map);
        }
        Err(mysql_error) => {
            log::error!("init meta schema error: {}", mysql_error);
            return Err(mysql_error);
        }
    }

    // table def
    let result = initial::read_all_table(global_context.clone());
    match result {
        Ok(table_def_map) => {
            global_context
                .lock()
                .unwrap()
                .meta_data
                .add_all_table(table_def_map);
        }
        Err(mysql_error) => {
            log::error!("init meta table error: {}", mysql_error);
            return Err(mysql_error);
        }
    }

    let mut core_execution = Execution::new(global_context.clone());

    let result = core_execution.try_init();
    if let Err(mysql_error) = result {
        return Err(mysql_error);
    }

    Ok(core_execution)
}
