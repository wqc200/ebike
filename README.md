# Ebike
Ebike is an OLAP open source database, written is Rust, that uses Apache Datafusion as its query execution framework.

## Why Ebike
1. High Performance 
   - Leveraging Rust and Arrow's memory model, Ebike achieves very high performance
2. Easy to Use
   - MySQL protocol support, The ecosystem around mysql is rich.
3. Simple
   - Rust Sled engine, can run on a single machine

## Installation
### Install from source
```shell
git clone --recurse-submodules git@github.com:wqc200/ebike.git
cd ebike
cargo build --release
./target/release/ebike-server -c ./config.toml
```

## Example Usage
The default MySQL port is 3307, Which we can use it with MySQL command:
```shell
mysql -uroot -h127.0.0.1 -P3307 -p
```