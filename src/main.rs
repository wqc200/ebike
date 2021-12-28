#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate clap;

use std::io;
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType as ArrowDataType;
use log4rs;
use tokio::net::TcpListener;
use tokio::signal::unix::{signal, SignalKind};

use meta::initial;

use crate::config::util::get_config_path;
use crate::config::util::read_config;
use crate::core::global_context::GlobalContext;
use crate::meta::meta_util;
use crate::mysql::handle;
use crate::mysql::metadata::MysqlType;

pub mod core;
pub mod config;
pub mod datafusion_impl;
pub mod meta;
pub mod mysql;
pub mod store;
pub mod util;
pub mod test;
pub mod variable;
pub mod execute_impl;
pub mod physical_plan;

#[tokio::main]
async fn main() {
    let config_path = get_config_path();
    println!("The config path: {}", config_path);

    let my_config = read_config(config_path.as_str());

    let global_context = Arc::new(Mutex::new(GlobalContext::new_with_config(my_config)));

    log4rs::init_file(global_context.lock().unwrap().my_config.server.log_file.to_string(), Default::default()).unwrap();

    let addr = global_context.lock().unwrap().my_config.server.bind_host.to_string();
    let listener = TcpListener::bind(&addr).await.unwrap();
    log::info!("Listening on: {}", addr.clone());

    let result = meta_util::init_meta(global_context.clone()).await;
    if let Err(e) = result {
        log::error!("init meta error: {}", e);
        return;
    }

    let result = meta_util::load_global_variable(global_context.clone());
    if let Err(e) = result {
        log::error!("load global variable error: {}", e);
        return;
    }

    let result = meta_util::read_all_schema(global_context.clone());
    match result {
        Ok(schema_map) => {
            global_context.lock().unwrap().meta_data.add_all_schema(schema_map);
        }
        Err(e) => {
            log::error!("init meta schema error: {}", e);
            return;
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
            return;
        }
    }

    let mut stream = signal(SignalKind::interrupt()).unwrap();

    tokio::select! {
        _ = async {
            loop {
                match listener.accept().await {
                    Ok((socket, _)) => {
                        let mut handler = handle::Handle::new(socket, global_context.clone()).await.unwrap();
                        tokio::spawn(async move {
                            handler.run().await;
                            log::info!("client closed");
                        });
                    }
                    Err(e) => log::error!("error accepting socket; error = {:?}", e),
                }
            }
        } => {}
        _ = async {
            stream.recv().await;
            log::info!("got signal interrupt");

            Ok::<_, io::Error>(())
        } => {}
    }
}
