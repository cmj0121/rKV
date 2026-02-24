use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Mutex;

use rkv::{Config, Key, DB, DEFAULT_NAMESPACE};

/// Opaque handle returned to C callers.
pub struct RkvDb {
    inner: DB,
}

/// Per-thread last error message.
static LAST_ERROR: Mutex<Option<String>> = Mutex::new(None);

fn set_last_error(msg: String) {
    *LAST_ERROR.lock().unwrap() = Some(msg);
}

/// Open a database at `path`. Returns a pointer on success, null on failure.
///
/// # Safety
/// `path` must be a valid null-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn rkv_open(path: *const c_char) -> *mut RkvDb {
    if path.is_null() {
        set_last_error("path is null".into());
        return ptr::null_mut();
    }

    let c_str = unsafe { CStr::from_ptr(path) };
    let path_str = match c_str.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_last_error(format!("invalid utf-8 path: {e}"));
            return ptr::null_mut();
        }
    };

    let config = Config::new(path_str);
    match DB::open(config) {
        Ok(db) => Box::into_raw(Box::new(RkvDb { inner: db })),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

/// Close and free a database handle.
///
/// # Safety
/// `db` must be a pointer returned by `rkv_open`, and must not be used after this call.
#[no_mangle]
pub unsafe extern "C" fn rkv_close(db: *mut RkvDb) {
    if db.is_null() {
        return;
    }
    let boxed = unsafe { Box::from_raw(db) };
    if let Err(e) = boxed.inner.close() {
        set_last_error(e.to_string());
    }
}

/// Store a key-value pair in the default namespace.
/// Returns the revision ID (u128) on success, 0 on failure.
///
/// # Safety
/// `db` must be a valid pointer. `key` and `value` must be valid pointers with the given lengths.
#[no_mangle]
pub unsafe extern "C" fn rkv_put(
    db: *mut RkvDb,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> u128 {
    if db.is_null() || key.is_null() || value.is_null() {
        set_last_error("null pointer argument".into());
        return 0;
    }
    let db = unsafe { &*db };
    let ns = match db.inner.namespace(DEFAULT_NAMESPACE, None) {
        Ok(ns) => ns,
        Err(e) => {
            set_last_error(e.to_string());
            return 0;
        }
    };
    let key_bytes = unsafe { std::slice::from_raw_parts(key, key_len) };
    let key = match Key::from_bytes(key_bytes) {
        Ok(k) => k,
        Err(e) => {
            set_last_error(e.to_string());
            return 0;
        }
    };
    let value = unsafe { std::slice::from_raw_parts(value, value_len) };
    match ns.put(key, value, None) {
        Ok(rev) => rev.as_u128(),
        Err(e) => {
            set_last_error(e.to_string());
            0
        }
    }
}

/// Retrieve a value by key from the default namespace.
/// On success, writes the value pointer to `out` and length to `out_len`.
/// The caller must free the returned buffer with `rkv_free`. Returns 0 on success, -1 on failure.
///
/// # Safety
/// `db` must be a valid pointer. `key` must be valid with the given length.
/// `out` and `out_len` must be valid writable pointers.
#[no_mangle]
pub unsafe extern "C" fn rkv_get(
    db: *mut RkvDb,
    key: *const u8,
    key_len: usize,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if db.is_null() || key.is_null() || out.is_null() || out_len.is_null() {
        set_last_error("null pointer argument".into());
        return -1;
    }
    let db = unsafe { &*db };
    let ns = match db.inner.namespace(DEFAULT_NAMESPACE, None) {
        Ok(ns) => ns,
        Err(e) => {
            set_last_error(e.to_string());
            return -1;
        }
    };
    let key_bytes = unsafe { std::slice::from_raw_parts(key, key_len) };
    let key = match Key::from_bytes(key_bytes) {
        Ok(k) => k,
        Err(e) => {
            set_last_error(e.to_string());
            return -1;
        }
    };
    match ns.get(key) {
        Ok(val) => match val.into_bytes() {
            Some(bytes) => {
                let mut boxed = bytes.into_boxed_slice();
                unsafe {
                    *out_len = boxed.len();
                    *out = boxed.as_mut_ptr();
                }
                std::mem::forget(boxed);
                0
            }
            None => {
                // Null value — key exists but no payload
                unsafe {
                    *out_len = 0;
                    *out = ptr::null_mut();
                }
                0
            }
        },
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

/// Delete a key from the default namespace. Returns 0 on success, -1 on failure.
///
/// # Safety
/// `db` must be a valid pointer. `key` must be valid with the given length.
#[no_mangle]
pub unsafe extern "C" fn rkv_delete(db: *mut RkvDb, key: *const u8, key_len: usize) -> i32 {
    if db.is_null() || key.is_null() {
        set_last_error("null pointer argument".into());
        return -1;
    }
    let db = unsafe { &*db };
    let ns = match db.inner.namespace(DEFAULT_NAMESPACE, None) {
        Ok(ns) => ns,
        Err(e) => {
            set_last_error(e.to_string());
            return -1;
        }
    };
    let key_bytes = unsafe { std::slice::from_raw_parts(key, key_len) };
    let key = match Key::from_bytes(key_bytes) {
        Ok(k) => k,
        Err(e) => {
            set_last_error(e.to_string());
            return -1;
        }
    };
    match ns.delete(key) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

/// Free a buffer previously returned by `rkv_get`.
///
/// # Safety
/// `ptr` must be a pointer returned by `rkv_get` with matching `len`, and must not be used after this call.
#[no_mangle]
pub unsafe extern "C" fn rkv_free(ptr: *mut u8, len: usize) {
    if ptr.is_null() {
        return;
    }
    drop(unsafe { Vec::from_raw_parts(ptr, len, len) });
}

/// Copy the last error message into `buf` (up to `buf_len` bytes including null terminator).
/// Returns the number of bytes written (excluding null), or -1 if no error is stored.
///
/// # Safety
/// `buf` must be a writable buffer of at least `buf_len` bytes.
#[no_mangle]
pub unsafe extern "C" fn rkv_last_error(buf: *mut c_char, buf_len: usize) -> i32 {
    if buf.is_null() || buf_len == 0 {
        return -1;
    }
    let guard = LAST_ERROR.lock().unwrap();
    match guard.as_deref() {
        Some(msg) => {
            let bytes = msg.as_bytes();
            let copy_len = bytes.len().min(buf_len - 1);
            unsafe {
                ptr::copy_nonoverlapping(bytes.as_ptr(), buf as *mut u8, copy_len);
                *buf.add(copy_len) = 0;
            }
            copy_len as i32
        }
        None => -1,
    }
}
