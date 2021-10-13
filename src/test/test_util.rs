use std::sync::{Arc, Mutex};
use crate::core::global_context::GlobalContext;
use tokio::net::TcpListener;
use crate::meta::{meta_util, initial};
use crate::core::execution::Execution;
use crate::config::def::MyConfig;
use crate::mysql::error::MysqlResult;

pub async fn create_execution() -> MysqlResult<Execution> {
    let global_context = Arc::new(Mutex::new(GlobalContext::new_with_config(MyConfig::default())));

    log4rs::init_file(global_context.lock().unwrap().my_config.server.log_file.to_string(), Default::default()).unwrap();

    let addr = global_context.lock().unwrap().my_config.server.bind_host.to_string();
    let listener = TcpListener::bind(&addr).await.unwrap();
    log::info!("Listening on: {}", addr.clone());

    let result = meta_util::init_meta(global_context.clone()).await;
    if let Err(e) = result {
        log::error!("init meta error: {}", e);
        return Err(e);
    }

    let result = meta_util::load_global_variable(global_context.clone());
    if let Err(e) = result {
        log::error!("load global variable error: {}", e);
        return Err(e);
    }

    let result = meta_util::read_all_schema(global_context.clone());
    match result {
        Ok(schema_map) => {
            global_context.lock().unwrap().meta_data.add_all_schema(schema_map);
        }
        Err(e) => {
            log::error!("init meta schema error: {}", e);
            return Err(e);
        }
    }

    // table def
    let result = initial::read_all_table(global_context.clone());
    match result {
        Ok(table_def_map) => {
            global_context.lock().unwrap().meta_data.add_all_table(table_def_map);
        }
        Err(e) => {
            log::error!("init meta table error: {}", e);
            return Err(e);
        }
    }

    let core_execution = Execution::new(global_context.clone());

    Ok(core_execution)
}

#[cfg(test)]
mod tests {
    use crate::test::test_util::create_execution;
    use crate::mysql::error::MysqlResult;

    #[tokio::test]
    async fn parallel_projection() -> MysqlResult<()> {
        let mut core_execution = create_execution().await?;

        let a = core_execution.execute_query("show databases").await?;

        Ok(())
    }
}