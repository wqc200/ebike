use crate::config::def::MyConfig;
use crate::core::execution::Execution;
use crate::core::global_context::GlobalContext;
use crate::meta::{initial, meta_util};
use crate::mysql::error::MysqlResult;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use uuid::Uuid;

pub async fn create_execution() -> MysqlResult<Execution> {
    let mut my_config = MyConfig::default();
    let test_id = Uuid::new_v4().to_simple().encode_lower(&mut Uuid::encode_buffer()).to_string();
    my_config.engine.sled.data_path = format!("./data/ebike/sled/{}", test_id);

    let global_context = Arc::new(Mutex::new(GlobalContext::new_with_config(
        my_config.clone(),
    )));

    log4rs::init_file(
        global_context
            .lock()
            .unwrap()
            .my_config
            .server
            .log_file
            .to_string(),
        Default::default(),
    )
    .unwrap();

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

#[cfg(test)]
mod tests {
    use crate::core::output::CoreOutput;
    use crate::core::output::CoreOutput::FinalCount;
    use crate::mysql::error::MysqlResult;
    use crate::mysql::{message, metadata};
    use crate::test::test_util::create_execution;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
    use std::sync::Arc;

    #[tokio::test]
    async fn show_databases() -> MysqlResult<()> {
        let mut core_execution = create_execution().await?;

        let result = core_execution.execute_query("show databases").await?;

        let mut results: Vec<RecordBatch> = vec![];
        match result {
            CoreOutput::ResultSet(_, r) => results = r,
            _ => {}
        }

        let expected = vec![
            "+--------------------+",
            "| Database           |",
            "+--------------------+",
            "| mysql              |",
            "| performance_schema |",
            "+--------------------+",
        ];

        assert_batches_eq!(expected, &results);

        Ok(())
    }
}
