use std::fs::File;
use std::io::Read;

use clap::{Arg, App};

use crate::config::def::MyConfig;

pub fn get_config_path() -> String {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .author(crate_authors!())
        .about(crate_description!())
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .help("Sets a custom config file")
                .default_value("./config.toml")
                .takes_value(true),
        )
        .get_matches();
    let config_path = matches.value_of("config").expect("invalid config value");
    config_path.to_string()
}

pub fn read_config(config_path: &str) -> MyConfig {
    let mut file = File::open(config_path).expect("Cannot open config file.");
    let mut buf = String::new();
    file.read_to_string(&mut buf).expect("Cannot read config file to buf.");
    let my_config = toml::from_str(&buf).expect("Occur syntax error in the config file.");
    my_config
}
