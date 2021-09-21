# <center>Sparrow</center>
Sparrow is an OLAP open source database, written is Rust, that uses Apache Datafusion as its query execution framework.

## Why sparrow
1. High Performance 
   - Leveraging Rust and Arrow's memory model, Sparrow achieves very high performance
2. Easy to Use
   - MySQL protocol support
3. Simple
   - Rust Sled engine, can run on a single machine

## Installation
### Install from source
```shell
git clone --recurse-submodules git@github.com:wqc200/sparrow.git
cd sparrow
cargo build --release
```

## Example Usage
The default MySQL port is 3307, Which we can use it with MySQL command:
```shell
mysql -uroot -h127.0.0.1 -P3307 -p
```