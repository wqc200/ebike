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
use std::path::PathBuf;

use super::ffi;
use super::ffi_util::opt_bytes_to_ptr;
use super::db::DB;
use super::option::ReadOptions;
use super::column_family::ColumnFamily;
use super::error::Error;


/// An iterator over a database or column family, with specifiable
/// ranges and direction.
///
/// This iterator is different to the standard ``DBIterator`` as it aims Into
/// replicate the underlying iterator API within RocksDB itself. This should
/// give access to more performance and flexibility but departs from the
/// widely recognised Rust idioms.
///
/// ```
/// use rocksdb::{DB, Options};
/// use sparrow::rocksdb::db::DB;
/// use sparrow::rocksdb::option::Options;
///
/// let path = "_path_for_rocksdb_storage4";
/// {
///     let db = DB::open_default(path).unwrap();
///     let mut iter = db.raw_iterator();
///
///     // Forwards iteration
///     iter.seek_to_first();
///     while iter.valid() {
///         println!("Saw {:?} {:?}", iter.key(), iter.value());
///         iter.next();
///     }
///
///     // Reverse iteration
///     iter.seek_to_last();
///     while iter.valid() {
///         println!("Saw {:?} {:?}", iter.key(), iter.value());
///         iter.prev();
///     }
///
///     // Seeking
///     iter.seek(b"my key");
///     while iter.valid() {
///         println!("Saw {:?} {:?}", iter.key(), iter.value());
///         iter.next();
///     }
///
///     // Reverse iteration from key
///     // Note, use seek_for_prev when reversing because if this key doesn't exist,
///     // this will make the iterator start from the previous key rather than the next.
///     iter.seek_for_prev(b"my key");
///     while iter.valid() {
///         println!("Saw {:?} {:?}", iter.key(), iter.value());
///         iter.prev();
///     }
/// }
/// let _ = DB::destroy(&Options::default(), path);
/// ```
pub struct DBRawIterator {
    inner: *mut ffi::rocksdb_iterator_t,
    db: PhantomData<DB>,
}

impl DBRawIterator {
    pub(crate) fn new(db: &DB, readopts: &ReadOptions) -> DBRawIterator {
        unsafe {
            DBRawIterator {
                inner: ffi::rocksdb_create_iterator(db.inner, readopts.inner),
                db: PhantomData,
            }
        }
    }

    pub(crate) fn new_cf(
        db: &DB,
        cf_handle: &ColumnFamily,
        readopts: &ReadOptions,
    ) -> Result<DBRawIterator, Error> {
        unsafe {
            Ok(DBRawIterator {
                inner: ffi::rocksdb_create_iterator_cf(db.inner, readopts.inner, cf_handle.inner),
                db: PhantomData,
            })
        }
    }

    /// Returns `true` if the iterator is valid. An iterator is invalidated when
    /// it reaches the end of its defined range, or when it encounters an error.
    ///
    /// To check whether the iterator encountered an error after `valid` has
    /// returned `false`, use the [`status`](DBRawIterator::status) method. `status` will never
    /// return an error when `valid` is `true`.
    pub fn valid(&self) -> bool {
        unsafe { ffi::rocksdb_iter_valid(self.inner) != 0 }
    }

    /// Returns an error `Result` if the iterator has encountered an error
    /// during operation. When an error is encountered, the iterator is
    /// invalidated and [`valid`](DBRawIterator::valid) will return `false` when called.
    ///
    /// Performing a seek will discard the current status.
    pub fn status(&self) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_iter_get_error(self.inner,));
        }
        Ok(())
    }

    /// Seeks to the first key in the database.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sparrow::rocksdb::db::DB;
    ///
    /// let path = "_path_for_rocksdb_storage5";
    /// {
    ///     let db = DB::open_default(path).unwrap();
    ///     let mut iter = db.raw_iterator();
    ///
    ///     // Iterate all keys from the start in lexicographic order
    ///     iter.seek_to_first();
    ///
    ///     while iter.valid() {
    ///         println!("{:?} {:?}", iter.key(), iter.value());
    ///         iter.next();
    ///     }
    ///
    ///     // Read just the first key
    ///     iter.seek_to_first();
    ///
    ///     if iter.valid() {
    ///         println!("{:?} {:?}", iter.key(), iter.value());
    ///     } else {
    ///         // There are no keys in the database
    ///     }
    /// }
    /// let _ = DB::destroy(&Options::default(), path);
    /// ```
    pub fn seek_to_first(&mut self) {
        unsafe {
            ffi::rocksdb_iter_seek_to_first(self.inner);
        }
    }

    /// Seeks to the last key in the database.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sparrow::rocksdb::db::DB;
    ///
    /// let path = "_path_for_rocksdb_storage6";
    /// {
    ///     let db = DB::open_default(path).unwrap();
    ///     let mut iter = db.raw_iterator();
    ///
    ///     // Iterate all keys from the end in reverse lexicographic order
    ///     iter.seek_to_last();
    ///
    ///     while iter.valid() {
    ///         println!("{:?} {:?}", iter.key(), iter.value());
    ///         iter.prev();
    ///     }
    ///
    ///     // Read just the last key
    ///     iter.seek_to_last();
    ///
    ///     if iter.valid() {
    ///         println!("{:?} {:?}", iter.key(), iter.value());
    ///     } else {
    ///         // There are no keys in the database
    ///     }
    /// }
    /// let _ = DB::destroy(&Options::default(), path);
    /// ```
    pub fn seek_to_last(&mut self) {
        unsafe {
            ffi::rocksdb_iter_seek_to_last(self.inner);
        }
    }

    /// Seeks to the specified key or the first key that lexicographically follows it.
    ///
    /// This method will attempt to seek to the specified key. If that key does not exist, it will
    /// find and seek to the key that lexicographically follows it instead.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sparrow::rocksdb::db::DB;
    ///
    /// let path = "_path_for_rocksdb_storage7";
    /// {
    ///     let db = DB::open_default(path).unwrap();
    ///     let mut iter = db.raw_iterator();
    ///
    ///     // Read the first key that starts with 'a'
    ///     iter.seek(b"a");
    ///
    ///     if iter.valid() {
    ///         println!("{:?} {:?}", iter.key(), iter.value());
    ///     } else {
    ///         // There are no keys in the database
    ///     }
    /// }
    /// let _ = DB::destroy(&Options::default(), path);
    /// ```
    pub fn seek<K: AsRef<[u8]>>(&mut self, key: K) {
        let key = key.as_ref();

        unsafe {
            ffi::rocksdb_iter_seek(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        }
    }

    /// Seeks to the specified key, or the first key that lexicographically precedes it.
    ///
    /// Like ``.seek()`` this method will attempt to seek to the specified key.
    /// The difference with ``.seek()`` is that if the specified key do not exist, this method will
    /// seek to key that lexicographically precedes it instead.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sparrow::rocksdb::db::DB;
    ///
    /// let path = "_path_for_rocksdb_storage8";
    /// {
    ///     let db = DB::open_default(path).unwrap();
    ///     let mut iter = db.raw_iterator();
    ///
    ///     // Read the last key that starts with 'a'
    ///     iter.seek_for_prev(b"b");
    ///
    ///     if iter.valid() {
    ///         println!("{:?} {:?}", iter.key(), iter.value());
    ///     } else {
    ///         // There are no keys in the database
    ///     }
    /// }
    /// let _ = DB::destroy(&Options::default(), path);
    /// ```
    pub fn seek_for_prev<K: AsRef<[u8]>>(&mut self, key: K) {
        let key = key.as_ref();

        unsafe {
            ffi::rocksdb_iter_seek_for_prev(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        }
    }

    /// Seeks to the next key.
    ///
    /// Returns true if the iterator is valid after this operation.
    pub fn next(&mut self) {
        unsafe {
            ffi::rocksdb_iter_next(self.inner);
        }
    }

    /// Seeks to the previous key.
    ///
    /// Returns true if the iterator is valid after this operation.
    pub fn prev(&mut self) {
        unsafe {
            ffi::rocksdb_iter_prev(self.inner);
        }
    }

    /// Returns a slice of the current key.
    pub fn key(&self) -> Option<&[u8]> {
        if self.valid() {
            // Safety Note: This is safe as all methods that may invalidate the buffer returned
            // take `&mut self`, so borrow checker will prevent use of buffer after seek.
            unsafe {
                let mut key_len: size_t = 0;
                let key_len_ptr: *mut size_t = &mut key_len;
                let key_ptr = ffi::rocksdb_iter_key(self.inner, key_len_ptr) as *const c_uchar;

                Some(slice::from_raw_parts(key_ptr, key_len as usize))
            }
        } else {
            None
        }
    }

    /// Returns a slice of the current value.
    pub fn value(&self) -> Option<&[u8]> {
        if self.valid() {
            // Safety Note: This is safe as all methods that may invalidate the buffer returned
            // take `&mut self`, so borrow checker will prevent use of buffer after seek.
            unsafe {
                let mut val_len: size_t = 0;
                let val_len_ptr: *mut size_t = &mut val_len;
                let val_ptr = ffi::rocksdb_iter_value(self.inner, val_len_ptr) as *const c_uchar;

                Some(slice::from_raw_parts(val_ptr, val_len as usize))
            }
        } else {
            None
        }
    }
}

impl Drop for DBRawIterator {
    fn drop(&mut self) {
        println!("drop iterator");
        unsafe {
            ffi::rocksdb_iter_destroy(self.inner);
        }
    }
}

// Safety note: auto-implementing Send on most db-related types is prevented by the inner FFI
// pointer. In most cases, however, this pointer is Send-safe because it is never aliased and
// rocksdb internally does not rely on thread-local information for its user-exposed types.
unsafe impl Send for DBRawIterator {}
unsafe impl Sync for DBRawIterator {}
