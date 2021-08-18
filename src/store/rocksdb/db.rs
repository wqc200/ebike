// Copyright 2014 Tyler Neely
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use super::ffi;
use libc::{self, c_char, c_int, c_uchar, c_void, size_t};
use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::fmt;
use std::fs;
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::Path;
use std::ptr;
use std::slice;
use std::str;
use std::error;
use std::path::PathBuf;

use super::ffi_util::opt_bytes_to_ptr;
use super::option::{Options, FlushOptions, WriteBatch, WriteOptions, ReadOptions};
use super::column_family::{ColumnFamilyDescriptor, ColumnFamily};
use super::iterator::DBRawIterator;

///
/// See crate level documentation for a simple usage example.
#[derive(Clone)]
pub struct DB {
    pub(crate) inner: *mut ffi::rocksdb_t,
    cfs: BTreeMap<String, ColumnFamily>,
    path: PathBuf,
}

/// A simple wrapper round a string, used for errors reported from
/// ffi calls.
#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    message: String,
}

impl Error {
    fn new(message: String) -> Error {
        Error { message }
    }

    pub fn into_string(self) -> String {
        self.into()
    }
}

impl AsRef<str> for Error {
    fn as_ref(&self) -> &str {
        &self.message
    }
}

impl From<Error> for String {
    fn from(e: Error) -> String {
        e.message
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.message.fmt(formatter)
    }
}

unsafe impl Send for DB {}
unsafe impl Sync for DB {}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum DBCompressionType {
    None = ffi::rocksdb_no_compression as isize,
    Snappy = ffi::rocksdb_snappy_compression as isize,
    Zlib = ffi::rocksdb_zlib_compression as isize,
    Bz2 = ffi::rocksdb_bz2_compression as isize,
    Lz4 = ffi::rocksdb_lz4_compression as isize,
    Lz4hc = ffi::rocksdb_lz4hc_compression as isize,
    Zstd = ffi::rocksdb_zstd_compression as isize,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum DBCompactionStyle {
    Level = ffi::rocksdb_level_compaction as isize,
    Universal = ffi::rocksdb_universal_compaction as isize,
    Fifo = ffi::rocksdb_fifo_compaction as isize,
}

impl DB {
    /// Open a database with default options.
    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<DB, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        DB::open(&opts, path)
    }

    /// Open the database with the specified options.
    pub fn open<P: AsRef<Path>>(opts: &Options, path: P) -> Result<DB, Error> {
        DB::open_cf(opts, path, None::<&str>)
    }

    /// Open a database with the given database options and column family names.
    ///
    /// Column families opened using this function will be created with default `Options`.
    pub fn open_cf<P, I, N>(opts: &Options, path: P, cfs: I) -> Result<DB, Error>
        where
            P: AsRef<Path>,
            I: IntoIterator<Item = N>,
            N: AsRef<str>,
    {
        let cfs = cfs
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name.as_ref(), Options::default()));

        DB::open_cf_descriptors(opts, path, cfs)
    }

    /// Open a database with the given database options and column family descriptors.
    pub fn open_cf_descriptors<P, I>(opts: &Options, path: P, cfs: I) -> Result<DB, Error>
        where
            P: AsRef<Path>,
            I: IntoIterator<Item = ColumnFamilyDescriptor>,
    {
        let cfs: Vec<_> = cfs.into_iter().collect();

        let path = path.as_ref();
        let cpath = match CString::new(path.to_string_lossy().as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                return Err(Error::new(
                    "Failed to convert path to CString \
                     when opening DB."
                        .to_owned(),
                ));
            }
        };

        if let Err(e) = fs::create_dir_all(&path) {
            return Err(Error::new(format!(
                "Failed to create RocksDB directory: `{:?}`.",
                e
            )));
        }

        let db: *mut ffi::rocksdb_t;
        let mut cf_map = BTreeMap::new();

        if cfs.is_empty() {
            unsafe {
                db = ffi_try!(ffi::rocksdb_open(opts.inner, cpath.as_ptr() as *const _,));
            }
        } else {
            let mut cfs_v = cfs;
            // Always open the default column family.
            if !cfs_v.iter().any(|cf| cf.name == "default") {
                cfs_v.push(ColumnFamilyDescriptor {
                    name: String::from("default"),
                    options: Options::default(),
                });
            }
            // We need to store our CStrings in an intermediate vector
            // so that their pointers remain valid.
            let c_cfs: Vec<CString> = cfs_v
                .iter()
                .map(|cf| CString::new(cf.name.as_bytes()).unwrap())
                .collect();

            let mut cfnames: Vec<_> = c_cfs.iter().map(|cf| cf.as_ptr()).collect();

            // These handles will be populated by DB.
            let mut cfhandles: Vec<_> = cfs_v.iter().map(|_| ptr::null_mut()).collect();

            let mut cfopts: Vec<_> = cfs_v
                .iter()
                .map(|cf| cf.options.inner as *const _)
                .collect();

            unsafe {
                db = ffi_try!(ffi::rocksdb_open_column_families(
                    opts.inner,
                    cpath.as_ptr(),
                    cfs_v.len() as c_int,
                    cfnames.as_mut_ptr(),
                    cfopts.as_mut_ptr(),
                    cfhandles.as_mut_ptr(),
                ));
            }

            for handle in &cfhandles {
                if handle.is_null() {
                    return Err(Error::new(
                        "Received null column family \
                         handle from DB."
                            .to_owned(),
                    ));
                }
            }

            for (cf_desc, inner) in cfs_v.iter().zip(cfhandles) {
                cf_map.insert(cf_desc.name.clone(), ColumnFamily { inner });
            }
        }

        if db.is_null() {
            return Err(Error::new("Could not initialize database.".to_owned()));
        }

        Ok(DB {
            inner: db,
            cfs: cf_map,
            path: path.to_path_buf(),
        })
    }

    pub fn list_cf<P: AsRef<Path>>(opts: &Options, path: P) -> Result<Vec<String>, Error> {
        let cpath = to_cpath(path)?;
        let mut length = 0;

        unsafe {
            let ptr = ffi_try!(ffi::rocksdb_list_column_families(
                opts.inner,
                cpath.as_ptr() as *const _,
                &mut length,
            ));

            let vec = slice::from_raw_parts(ptr, length)
                .iter()
                .map(|ptr| CStr::from_ptr(*ptr).to_string_lossy().into_owned())
                .collect();
            ffi::rocksdb_list_column_families_destroy(ptr, length);
            Ok(vec)
        }
    }

    pub fn destroy<P: AsRef<Path>>(opts: &Options, path: P) -> Result<(), Error> {
        let cpath = to_cpath(path)?;
        unsafe {
            ffi_try!(ffi::rocksdb_destroy_db(opts.inner, cpath.as_ptr(),));
        }
        Ok(())
    }

    pub fn repair<P: AsRef<Path>>(opts: &Options, path: P) -> Result<(), Error> {
        let cpath = to_cpath(path)?;
        unsafe {
            ffi_try!(ffi::rocksdb_repair_db(opts.inner, cpath.as_ptr(),));
        }
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path.as_path()
    }

    /// Flush database memtable to SST files on disk (with options).
    pub fn flush_opt(&self, flushopts: &FlushOptions) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_flush(self.inner, flushopts.inner,));
        }
        Ok(())
    }

    /// Flush database memtable to SST files on disk.
    pub fn flush(&self) -> Result<(), Error> {
        self.flush_opt(&FlushOptions::default())
    }

    pub fn write_opt(&self, batch: WriteBatch, writeopts: &WriteOptions) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_write(self.inner, writeopts.inner, batch.inner,));
        }
        Ok(())
    }

    pub fn write(&self, batch: WriteBatch) -> Result<(), Error> {
        self.write_opt(batch, &WriteOptions::default())
    }

    pub fn write_without_wal(&self, batch: WriteBatch) -> Result<(), Error> {
        let mut wo = WriteOptions::new();
        wo.disable_wal(true);
        self.write_opt(batch, &wo)
    }

    /// Return the bytes associated with a key value with read options. If you only intend to use
    /// the vector returned temporarily, consider using [`get_pinned_opt`](#method.get_pinned_opt)
    /// to avoid unnecessary memory copy.
    pub fn get_opt<K: AsRef<[u8]>>(
        &self,
        key: K,
        readopts: &ReadOptions,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.get_pinned_opt(key, readopts)
            .map(|x| x.map(|v| v.as_ref().to_vec()))
    }

    /// Return the bytes associated with a key value. If you only intend to use the vector returned
    /// temporarily, consider using [`get_pinned`](#method.get_pinned) to avoid unnecessary memory
    /// copy.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, Error> {
        self.get_opt(key.as_ref(), &ReadOptions::default())
    }

    /// Return the bytes associated with a key value and the given column family with read options.
    /// If you only intend to use the vector returned temporarily, consider using
    /// [`get_pinned_cf_opt`](#method.get_pinned_cf_opt) to avoid unnecessary memory.
    pub fn get_cf_opt<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
        readopts: &ReadOptions,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.get_pinned_cf_opt(cf, key, readopts)
            .map(|x| x.map(|v| v.as_ref().to_vec()))
    }

    /// Return the bytes associated with a key value and the given column family. If you only
    /// intend to use the vector returned temporarily, consider using
    /// [`get_pinned_cf`](#method.get_pinned_cf) to avoid unnecessary memory.
    pub fn get_cf<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.get_cf_opt(cf, key.as_ref(), &ReadOptions::default())
    }

    /// Return the value associated with a key using RocksDB's PinnableSlice
    /// so as to avoid unnecessary memory copy.
    pub fn get_pinned_opt<K: AsRef<[u8]>>(
        &self,
        key: K,
        readopts: &ReadOptions,
    ) -> Result<Option<DBPinnableSlice>, Error> {
        if readopts.inner.is_null() {
            return Err(Error::new(
                "Unable to create RocksDB read options. \
                 This is a fairly trivial call, and its \
                 failure may be indicative of a \
                 mis-compiled or mis-loaded RocksDB \
                 library."
                    .to_owned(),
            ));
        }

        let key = key.as_ref();
        unsafe {
            let val = ffi_try!(ffi::rocksdb_get_pinned(
                self.inner,
                readopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBPinnableSlice::from_c(val)))
            }
        }
    }

    /// Return the value associated with a key using RocksDB's PinnableSlice
    /// so as to avoid unnecessary memory copy. Similar to get_pinned_opt but
    /// leverages default options.
    pub fn get_pinned<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<DBPinnableSlice>, Error> {
        self.get_pinned_opt(key, &ReadOptions::default())
    }

    /// Return the value associated with a key using RocksDB's PinnableSlice
    /// so as to avoid unnecessary memory copy. Similar to get_pinned_opt but
    /// allows specifying ColumnFamily
    pub fn get_pinned_cf_opt<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
        readopts: &ReadOptions,
    ) -> Result<Option<DBPinnableSlice>, Error> {
        if readopts.inner.is_null() {
            return Err(Error::new(
                "Unable to create RocksDB read options. \
                 This is a fairly trivial call, and its \
                 failure may be indicative of a \
                 mis-compiled or mis-loaded RocksDB \
                 library."
                    .to_owned(),
            ));
        }

        let key = key.as_ref();
        unsafe {
            let val = ffi_try!(ffi::rocksdb_get_pinned_cf(
                self.inner,
                readopts.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBPinnableSlice::from_c(val)))
            }
        }
    }

    /// Return the value associated with a key using RocksDB's PinnableSlice
    /// so as to avoid unnecessary memory copy. Similar to get_pinned_cf_opt but
    /// leverages default options.
    pub fn get_pinned_cf<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
    ) -> Result<Option<DBPinnableSlice>, Error> {
        self.get_pinned_cf_opt(cf, key, &ReadOptions::default())
    }

    pub fn create_cf<N: AsRef<str>>(&mut self, name: N, opts: &Options) -> Result<(), Error> {
        let cname = match CString::new(name.as_ref().as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                return Err(Error::new(
                    "Failed to convert path to CString \
                     when opening rocksdb"
                        .to_owned(),
                ));
            }
        };
        unsafe {
            let inner = ffi_try!(ffi::rocksdb_create_column_family(
                self.inner,
                opts.inner,
                cname.as_ptr(),
            ));

            self.cfs
                .insert(name.as_ref().to_string(), ColumnFamily { inner });
        };
        Ok(())
    }

    pub fn drop_cf(&mut self, name: &str) -> Result<(), Error> {
        if let Some(cf) = self.cfs.remove(name) {
            unsafe {
                ffi_try!(ffi::rocksdb_drop_column_family(self.inner, cf.inner,));
            }
            Ok(())
        } else {
            Err(Error::new(
                format!("Invalid column family: {}", name).to_owned(),
            ))
        }
    }

    /// Return the underlying column family handle.
    pub fn cf_handle(&self, name: &str) -> Option<&ColumnFamily> {
        self.cfs.get(name)
    }

    /// Opens a raw iterator over the database, using the default read options
    pub fn raw_iterator(&self) -> DBRawIterator {
        let opts = ReadOptions::default();
        DBRawIterator::new(self, &opts)
    }

    /// Opens a raw iterator over the database, using the given read options
    pub fn raw_iterator_opt(&self, readopts: &ReadOptions) -> DBRawIterator {
        DBRawIterator::new(self, readopts)
    }

    pub fn delete_cf_opt<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        let key = key.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_delete_cf(
                self.inner,
                writeopts.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn put<K, V>(&self, key: K, value: V) -> Result<(), Error>
        where
            K: AsRef<[u8]>,
            V: AsRef<[u8]>,
    {
        self.put_opt(key.as_ref(), value.as_ref(), &WriteOptions::default())
    }

    pub fn put_opt<K, V>(&self, key: K, value: V, writeopts: &WriteOptions) -> Result<(), Error>
        where
            K: AsRef<[u8]>,
            V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_put(
                self.inner,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn put_cf<K, V>(&self, cf: &ColumnFamily, key: K, value: V) -> Result<(), Error>
        where
            K: AsRef<[u8]>,
            V: AsRef<[u8]>,
    {
        self.put_cf_opt(cf, key.as_ref(), value.as_ref(), &WriteOptions::default())
    }

    pub fn put_cf_opt<K, V>(
        &self,
        cf: &ColumnFamily,
        key: K,
        value: V,
        writeopts: &WriteOptions,
    ) -> Result<(), Error>
        where
            K: AsRef<[u8]>,
            V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_put_cf(
                self.inner,
                writeopts.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), Error> {
        self.delete_opt(key.as_ref(), &WriteOptions::default())
    }

    pub fn delete_opt<K: AsRef<[u8]>>(
        &self,
        key: K,
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        let key = key.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_delete(
                self.inner,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn delete_cf<K: AsRef<[u8]>>(&self, cf: &ColumnFamily, key: K) -> Result<(), Error> {
        self.delete_cf_opt(cf, key.as_ref(), &WriteOptions::default())
    }

    pub fn merge_opt<K, V>(&self, key: K, value: V, writeopts: &WriteOptions) -> Result<(), Error>
        where
            K: AsRef<[u8]>,
            V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_merge(
                self.inner,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn merge_cf_opt<K, V>(
        &self,
        cf: &ColumnFamily,
        key: K,
        value: V,
        writeopts: &WriteOptions,
    ) -> Result<(), Error>
        where
            K: AsRef<[u8]>,
            V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi_try!(ffi::rocksdb_merge_cf(
                self.inner,
                writeopts.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn merge<K, V>(&self, key: K, value: V) -> Result<(), Error>
        where
            K: AsRef<[u8]>,
            V: AsRef<[u8]>,
    {
        self.merge_opt(key.as_ref(), value.as_ref(), &WriteOptions::default())
    }

    pub fn merge_cf<K, V>(&self, cf: &ColumnFamily, key: K, value: V) -> Result<(), Error>
        where
            K: AsRef<[u8]>,
            V: AsRef<[u8]>,
    {
        self.merge_cf_opt(cf, key.as_ref(), value.as_ref(), &WriteOptions::default())
    }

    pub fn compact_range<S: AsRef<[u8]>, E: AsRef<[u8]>>(&self, start: Option<S>, end: Option<E>) {
        unsafe {
            let start = start.as_ref().map(|s| s.as_ref());
            let end = end.as_ref().map(|e| e.as_ref());

            ffi::rocksdb_compact_range(
                self.inner,
                opt_bytes_to_ptr(start),
                start.map_or(0, |s| s.len()) as size_t,
                opt_bytes_to_ptr(end),
                end.map_or(0, |e| e.len()) as size_t,
            );
        }
    }

    pub fn compact_range_cf<S: AsRef<[u8]>, E: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        start: Option<S>,
        end: Option<E>,
    ) {
        unsafe {
            let start = start.as_ref().map(|s| s.as_ref());
            let end = end.as_ref().map(|e| e.as_ref());

            ffi::rocksdb_compact_range_cf(
                self.inner,
                cf.inner,
                opt_bytes_to_ptr(start),
                start.map_or(0, |s| s.len()) as size_t,
                opt_bytes_to_ptr(end),
                end.map_or(0, |e| e.len()) as size_t,
            );
        }
    }

    /// Retrieves a RocksDB property by name.
    ///
    /// For a full list of properties, see
    /// https://github.com/facebook/rocksdb/blob/08809f5e6cd9cc4bc3958dd4d59457ae78c76660/include/rocksdb/db.h#L428-L634
    pub fn property_value(&self, name: &str) -> Result<Option<String>, Error> {
        let prop_name = match CString::new(name) {
            Ok(c) => c,
            Err(e) => {
                return Err(Error::new(format!(
                    "Failed to convert property name to CString: {}",
                    e
                )));
            }
        };

        unsafe {
            let value = ffi::rocksdb_property_value(self.inner, prop_name.as_ptr());
            if value.is_null() {
                return Ok(None);
            }

            let str_value = match CStr::from_ptr(value).to_str() {
                Ok(s) => s.to_owned(),
                Err(e) => {
                    return Err(Error::new(format!(
                        "Failed to convert property value to string: {}",
                        e
                    )));
                }
            };

            libc::free(value as *mut c_void);
            Ok(Some(str_value))
        }
    }

    /// The sequence number of the most recent transaction.
    pub fn latest_sequence_number(&self) -> u64 {
        unsafe { ffi::rocksdb_get_latest_sequence_number(self.inner) }
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        // println!("drop db");
        // unsafe {
        //     for cf in self.cfs.values() {
        //         ffi::rocksdb_column_family_handle_destroy(cf.inner);
        //     }
        //     ffi::rocksdb_close(self.inner);
        // }
    }
}

impl fmt::Debug for DB {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RocksDB {{ path: {:?} }}", self.path())
    }
}


/// Wrapper around RocksDB PinnableSlice struct.
///
/// With a pinnable slice, we can directly leverage in-memory data within
/// RocksDB toa void unnecessary memory copies. The struct here wraps the
/// returned raw pointer and ensures proper finalization work.
pub struct DBPinnableSlice {
    ptr: *mut ffi::rocksdb_pinnableslice_t,
    db: PhantomData<DB>,
}

// Safety note: auto-implementing Send on most db-related types is prevented by the inner FFI
// pointer. In most cases, however, this pointer is Send-safe because it is never aliased and
// rocksdb internally does not rely on thread-local information for its user-exposed types.
unsafe impl Send for DBPinnableSlice {}

// Sync is similarly safe for many types because they do not expose interior mutability, and their
// use within the rocksdb library is generally behind a const reference
unsafe impl Sync for DBPinnableSlice {}

impl AsRef<[u8]> for DBPinnableSlice {
    fn as_ref(&self) -> &[u8] {
        // Implement this via Deref so as not to repeat ourselves
        &*self
    }
}

impl Deref for DBPinnableSlice {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe {
            let mut val_len: size_t = 0;
            let val = ffi::rocksdb_pinnableslice_value(self.ptr, &mut val_len) as *mut u8;
            slice::from_raw_parts(val, val_len)
        }
    }
}

impl Drop for DBPinnableSlice {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_pinnableslice_destroy(self.ptr);
        }
    }
}

impl DBPinnableSlice {
    /// Used to wrap a PinnableSlice from rocksdb to avoid unnecessary memcpy
    ///
    /// # Unsafe
    /// Requires that the pointer must be generated by rocksdb_get_pinned
    unsafe fn from_c(ptr: *mut ffi::rocksdb_pinnableslice_t) -> DBPinnableSlice {
        DBPinnableSlice {
            ptr,
            db: PhantomData,
        }
    }
}

fn to_cpath<P: AsRef<Path>>(path: P) -> Result<CString, Error> {
    match CString::new(path.as_ref().to_string_lossy().as_bytes()) {
        Ok(c) => Ok(c),
        Err(_) => Err(Error::new(
            "Failed to convert path to CString when opening DB.".to_owned(),
        )),
    }
}
