use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Mutex;
use std::time::Duration;

use rkv::{Config, Key, Value, DB, DEFAULT_NAMESPACE};

/// Opaque handle returned to C callers.
pub struct RkvDb {
    inner: DB,
}

/// Per-thread last error message.
static LAST_ERROR: Mutex<Option<String>> = Mutex::new(None);

fn set_last_error(msg: String) {
    *LAST_ERROR.lock().unwrap_or_else(|e| e.into_inner()) = Some(msg);
}

/// Helper: extract a namespace name from a C string pointer.
/// If `ns` is null, returns the default namespace.
unsafe fn ns_str(ns: *const c_char) -> Result<String, String> {
    if ns.is_null() {
        return Ok(DEFAULT_NAMESPACE.to_owned());
    }
    let c_str = unsafe { CStr::from_ptr(ns) };
    c_str
        .to_str()
        .map(|s| s.to_owned())
        .map_err(|e| format!("invalid utf-8 namespace: {e}"))
}

/// Helper: parse a Key from raw bytes.
fn parse_key(key: *const u8, key_len: usize) -> Result<Key, String> {
    let key_bytes = unsafe { std::slice::from_raw_parts(key, key_len) };
    Key::from_bytes(key_bytes).map_err(|e| e.to_string())
}

/// Helper: write value bytes to output pointers. Returns 0 on success.
///
/// # Safety
/// `out` and `out_len` must be valid, non-null writable pointers.
/// The caller must free the returned buffer with `rkv_free`.
unsafe fn write_value_out(val: Value, out: *mut *mut u8, out_len: *mut usize) -> i32 {
    if out.is_null() || out_len.is_null() {
        set_last_error("null output pointer".into());
        return -1;
    }
    match val.into_bytes() {
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
            unsafe {
                *out_len = 0;
                *out = ptr::null_mut();
            }
            0
        }
    }
}

// =============================================================================
// Database lifecycle
// =============================================================================

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

// =============================================================================
// CRUD — default namespace (backwards compatible)
// =============================================================================

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
    rkv_put_ns(db, ptr::null(), key, key_len, value, value_len, 0)
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
    rkv_get_ns(db, ptr::null(), key, key_len, out, out_len)
}

/// Delete a key from the default namespace. Returns 0 on success, -1 on failure.
///
/// # Safety
/// `db` must be a valid pointer. `key` must be valid with the given length.
#[no_mangle]
pub unsafe extern "C" fn rkv_delete(db: *mut RkvDb, key: *const u8, key_len: usize) -> i32 {
    rkv_delete_ns(db, ptr::null(), key, key_len)
}

// =============================================================================
// CRUD — namespaced
// =============================================================================

/// Store a key-value pair in the given namespace with an optional TTL (milliseconds).
/// Pass `ns = NULL` for the default namespace. Pass `ttl_ms = 0` for no expiry.
/// Returns the revision ID (u128) on success, 0 on failure.
///
/// # Safety
/// `db` must be a valid pointer. `key` and `value` must be valid pointers with the given lengths.
/// `ns` must be a valid null-terminated C string or NULL.
#[no_mangle]
pub unsafe extern "C" fn rkv_put_ns(
    db: *mut RkvDb,
    ns: *const c_char,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    ttl_ms: u64,
) -> u128 {
    if db.is_null() || key.is_null() || value.is_null() {
        set_last_error("null pointer argument".into());
        return 0;
    }
    let ns_name = match unsafe { ns_str(ns) } {
        Ok(s) => s,
        Err(e) => {
            set_last_error(e);
            return 0;
        }
    };
    let db = unsafe { &*db };
    let namespace = match db.inner.namespace(&ns_name, None) {
        Ok(ns) => ns,
        Err(e) => {
            set_last_error(e.to_string());
            return 0;
        }
    };
    let key = match parse_key(key, key_len) {
        Ok(k) => k,
        Err(e) => {
            set_last_error(e);
            return 0;
        }
    };
    let value = unsafe { std::slice::from_raw_parts(value, value_len) };
    let ttl = if ttl_ms > 0 {
        Some(Duration::from_millis(ttl_ms))
    } else {
        None
    };
    match namespace.put(key, value, ttl) {
        Ok(rev) => rev.as_u128(),
        Err(e) => {
            set_last_error(e.to_string());
            0
        }
    }
}

/// Retrieve a value by key from the given namespace.
/// Pass `ns = NULL` for the default namespace.
/// On success, writes the value pointer to `out` and length to `out_len`.
/// The caller must free the returned buffer with `rkv_free`. Returns 0 on success, -1 on failure.
///
/// # Safety
/// `db` must be a valid pointer. `key` must be valid with the given length.
/// `ns` must be a valid null-terminated C string or NULL.
/// `out` and `out_len` must be valid writable pointers.
#[no_mangle]
pub unsafe extern "C" fn rkv_get_ns(
    db: *mut RkvDb,
    ns: *const c_char,
    key: *const u8,
    key_len: usize,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if db.is_null() || key.is_null() || out.is_null() || out_len.is_null() {
        set_last_error("null pointer argument".into());
        return -1;
    }
    let ns_name = match unsafe { ns_str(ns) } {
        Ok(s) => s,
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    };
    let db = unsafe { &*db };
    let namespace = match db.inner.namespace(&ns_name, None) {
        Ok(ns) => ns,
        Err(e) => {
            set_last_error(e.to_string());
            return -1;
        }
    };
    let key = match parse_key(key, key_len) {
        Ok(k) => k,
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    };
    match namespace.get(key) {
        Ok(val) => unsafe { write_value_out(val, out, out_len) },
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

/// Delete a key from the given namespace. Pass `ns = NULL` for the default namespace.
/// Returns 0 on success, -1 on failure.
///
/// # Safety
/// `db` must be a valid pointer. `key` must be valid with the given length.
/// `ns` must be a valid null-terminated C string or NULL.
#[no_mangle]
pub unsafe extern "C" fn rkv_delete_ns(
    db: *mut RkvDb,
    ns: *const c_char,
    key: *const u8,
    key_len: usize,
) -> i32 {
    if db.is_null() || key.is_null() {
        set_last_error("null pointer argument".into());
        return -1;
    }
    let ns_name = match unsafe { ns_str(ns) } {
        Ok(s) => s,
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    };
    let db = unsafe { &*db };
    let namespace = match db.inner.namespace(&ns_name, None) {
        Ok(ns) => ns,
        Err(e) => {
            set_last_error(e.to_string());
            return -1;
        }
    };
    let key = match parse_key(key, key_len) {
        Ok(k) => k,
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    };
    match namespace.delete(key) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

// =============================================================================
// Query operations
// =============================================================================

/// Check if a key exists in the given namespace. Pass `ns = NULL` for the default namespace.
/// Returns 1 if exists, 0 if not, -1 on error.
///
/// # Safety
/// `db` must be a valid pointer. `key` must be valid with the given length.
/// `ns` must be a valid null-terminated C string or NULL.
#[no_mangle]
pub unsafe extern "C" fn rkv_exists(
    db: *mut RkvDb,
    ns: *const c_char,
    key: *const u8,
    key_len: usize,
) -> i32 {
    if db.is_null() || key.is_null() {
        set_last_error("null pointer argument".into());
        return -1;
    }
    let ns_name = match unsafe { ns_str(ns) } {
        Ok(s) => s,
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    };
    let db = unsafe { &*db };
    let namespace = match db.inner.namespace(&ns_name, None) {
        Ok(ns) => ns,
        Err(e) => {
            set_last_error(e.to_string());
            return -1;
        }
    };
    let key = match parse_key(key, key_len) {
        Ok(k) => k,
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    };
    match namespace.exists(key) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

/// Get the remaining TTL in milliseconds for a key.
/// Returns the TTL in ms, 0 if the key has no expiry, or -1 on error (key not found, etc.).
///
/// # Safety
/// `db` must be a valid pointer. `key` must be valid with the given length.
/// `ns` must be a valid null-terminated C string or NULL.
#[no_mangle]
pub unsafe extern "C" fn rkv_ttl(
    db: *mut RkvDb,
    ns: *const c_char,
    key: *const u8,
    key_len: usize,
) -> i64 {
    if db.is_null() || key.is_null() {
        set_last_error("null pointer argument".into());
        return -1;
    }
    let ns_name = match unsafe { ns_str(ns) } {
        Ok(s) => s,
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    };
    let db = unsafe { &*db };
    let namespace = match db.inner.namespace(&ns_name, None) {
        Ok(ns) => ns,
        Err(e) => {
            set_last_error(e.to_string());
            return -1;
        }
    };
    let key = match parse_key(key, key_len) {
        Ok(k) => k,
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    };
    match namespace.ttl(key) {
        Ok(Some(d)) => d.as_millis() as i64,
        Ok(None) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

/// Scan keys by prefix. Returns the number of keys found, or -1 on error.
/// Keys are written as concatenated length-prefixed entries to `out`:
///   [key_len: 4 bytes LE][key_bytes] repeated.
/// The caller must free `*out` with `rkv_free`.
///
/// # Safety
/// `db` must be a valid pointer. `prefix` must be valid with the given length.
/// `ns` must be a valid null-terminated C string or NULL.
/// `out` and `out_len` must be valid writable pointers.
#[no_mangle]
pub unsafe extern "C" fn rkv_scan(
    db: *mut RkvDb,
    ns: *const c_char,
    prefix: *const u8,
    prefix_len: usize,
    limit: usize,
    offset: usize,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    rkv_scan_inner(
        db, ns, prefix, prefix_len, limit, offset, out, out_len, false,
    )
}

/// Reverse scan keys by prefix. Same interface as `rkv_scan`.
///
/// # Safety
/// Same as `rkv_scan`.
#[no_mangle]
pub unsafe extern "C" fn rkv_rscan(
    db: *mut RkvDb,
    ns: *const c_char,
    prefix: *const u8,
    prefix_len: usize,
    limit: usize,
    offset: usize,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    rkv_scan_inner(
        db, ns, prefix, prefix_len, limit, offset, out, out_len, true,
    )
}

#[allow(clippy::too_many_arguments)]
unsafe fn rkv_scan_inner(
    db: *mut RkvDb,
    ns: *const c_char,
    prefix: *const u8,
    prefix_len: usize,
    limit: usize,
    offset: usize,
    out: *mut *mut u8,
    out_len: *mut usize,
    reverse: bool,
) -> i32 {
    if db.is_null() || prefix.is_null() || out.is_null() || out_len.is_null() {
        set_last_error("null pointer argument".into());
        return -1;
    }
    let ns_name = match unsafe { ns_str(ns) } {
        Ok(s) => s,
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    };
    let db = unsafe { &*db };
    let namespace = match db.inner.namespace(&ns_name, None) {
        Ok(ns) => ns,
        Err(e) => {
            set_last_error(e.to_string());
            return -1;
        }
    };
    let prefix_key = match parse_key(prefix, prefix_len) {
        Ok(k) => k,
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    };
    let keys = if reverse {
        match namespace.rscan(&prefix_key, limit, offset) {
            Ok(k) => k,
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    } else {
        match namespace.scan(&prefix_key, limit, offset) {
            Ok(k) => k,
            Err(e) => {
                set_last_error(e.to_string());
                return -1;
            }
        }
    };

    // Encode keys as [len:4 LE][bytes]...
    let mut buf = Vec::new();
    for key in &keys {
        let key_bytes = key.to_bytes();
        buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&key_bytes);
    }

    if buf.is_empty() {
        unsafe {
            *out = ptr::null_mut();
            *out_len = 0;
        }
    } else {
        let mut boxed = buf.into_boxed_slice();
        unsafe {
            *out_len = boxed.len();
            *out = boxed.as_mut_ptr();
        }
        std::mem::forget(boxed);
    }
    keys.len() as i32
}

// =============================================================================
// Revision history
// =============================================================================

/// Get the number of revisions for a key. Returns the count, or -1 on error.
///
/// # Safety
/// `db` must be a valid pointer. `key` must be valid with the given length.
/// `ns` must be a valid null-terminated C string or NULL.
#[no_mangle]
pub unsafe extern "C" fn rkv_rev_count(
    db: *mut RkvDb,
    ns: *const c_char,
    key: *const u8,
    key_len: usize,
) -> i64 {
    if db.is_null() || key.is_null() {
        set_last_error("null pointer argument".into());
        return -1;
    }
    let ns_name = match unsafe { ns_str(ns) } {
        Ok(s) => s,
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    };
    let db = unsafe { &*db };
    let namespace = match db.inner.namespace(&ns_name, None) {
        Ok(ns) => ns,
        Err(e) => {
            set_last_error(e.to_string());
            return -1;
        }
    };
    let key = match parse_key(key, key_len) {
        Ok(k) => k,
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    };
    match namespace.rev_count(key) {
        Ok(n) => n as i64,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

/// Get a specific revision of a key by index (0 = oldest).
/// On success, writes the value to `out`/`out_len`. Returns 0 on success, -1 on failure.
/// The caller must free `*out` with `rkv_free`.
///
/// # Safety
/// `db` must be a valid pointer. `key` must be valid with the given length.
/// `ns` must be a valid null-terminated C string or NULL.
/// `out` and `out_len` must be valid writable pointers.
#[no_mangle]
pub unsafe extern "C" fn rkv_rev_get(
    db: *mut RkvDb,
    ns: *const c_char,
    key: *const u8,
    key_len: usize,
    index: u64,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if db.is_null() || key.is_null() || out.is_null() || out_len.is_null() {
        set_last_error("null pointer argument".into());
        return -1;
    }
    let ns_name = match unsafe { ns_str(ns) } {
        Ok(s) => s,
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    };
    let db = unsafe { &*db };
    let namespace = match db.inner.namespace(&ns_name, None) {
        Ok(ns) => ns,
        Err(e) => {
            set_last_error(e.to_string());
            return -1;
        }
    };
    let key = match parse_key(key, key_len) {
        Ok(k) => k,
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    };
    match namespace.rev_get(key, index) {
        Ok(val) => unsafe { write_value_out(val, out, out_len) },
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

// =============================================================================
// Maintenance
// =============================================================================

/// Flush in-memory write buffers to disk. Returns 0 on success, -1 on failure.
///
/// # Safety
/// `db` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn rkv_flush(db: *mut RkvDb) -> i32 {
    if db.is_null() {
        set_last_error("null pointer argument".into());
        return -1;
    }
    let db = unsafe { &*db };
    match db.inner.flush() {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

/// Trigger manual compaction. Returns 0 on success, -1 on failure.
///
/// # Safety
/// `db` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn rkv_compact(db: *mut RkvDb) -> i32 {
    if db.is_null() {
        set_last_error("null pointer argument".into());
        return -1;
    }
    let db = unsafe { &*db };
    match db.inner.compact() {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

/// Flush and fsync all data to durable storage. Returns 0 on success, -1 on failure.
///
/// # Safety
/// `db` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn rkv_sync(db: *mut RkvDb) -> i32 {
    if db.is_null() {
        set_last_error("null pointer argument".into());
        return -1;
    }
    let db = unsafe { &*db };
    match db.inner.sync() {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

// =============================================================================
// Stats
// =============================================================================

/// Get database statistics as a JSON string.
/// The caller must free the returned string with `rkv_free_string`.
/// Returns null on failure.
///
/// # Safety
/// `db` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn rkv_stats(db: *mut RkvDb) -> *mut c_char {
    if db.is_null() {
        set_last_error("null pointer argument".into());
        return ptr::null_mut();
    }
    let db = unsafe { &*db };
    let stats = db.inner.stats();
    let json = format!(
        concat!(
            "{{",
            "\"total_keys\":{},",
            "\"data_size_bytes\":{},",
            "\"namespace_count\":{},",
            "\"level_count\":{},",
            "\"sstable_count\":{},",
            "\"write_buffer_bytes\":{},",
            "\"pending_compactions\":{},",
            "\"op_puts\":{},",
            "\"op_gets\":{},",
            "\"op_deletes\":{},",
            "\"cache_hits\":{},",
            "\"cache_misses\":{},",
            "\"uptime_secs\":{}",
            "}}"
        ),
        stats.total_keys,
        stats.data_size_bytes,
        stats.namespace_count,
        stats.level_count,
        stats.sstable_count,
        stats.write_buffer_bytes,
        stats.pending_compactions,
        stats.op_puts,
        stats.op_gets,
        stats.op_deletes,
        stats.cache_hits,
        stats.cache_misses,
        stats.uptime.as_secs()
    );
    match std::ffi::CString::new(json) {
        Ok(c) => c.into_raw(),
        Err(e) => {
            set_last_error(format!("stats serialization error: {e}"));
            ptr::null_mut()
        }
    }
}

// =============================================================================
// Memory management
// =============================================================================

/// Free a buffer previously returned by `rkv_get`, `rkv_rev_get`, `rkv_scan`, or `rkv_rscan`.
///
/// # Safety
/// `ptr` must be a pointer returned by an rkv function with matching `len`,
/// and must not be used after this call.
#[no_mangle]
pub unsafe extern "C" fn rkv_free(ptr: *mut u8, len: usize) {
    if ptr.is_null() {
        return;
    }
    drop(unsafe { Vec::from_raw_parts(ptr, len, len) });
}

/// Free a C string previously returned by `rkv_stats`.
///
/// # Safety
/// `ptr` must be a pointer returned by `rkv_stats` and must not be used after this call.
#[no_mangle]
pub unsafe extern "C" fn rkv_free_string(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    drop(unsafe { std::ffi::CString::from_raw(ptr) });
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
    let guard = LAST_ERROR.lock().unwrap_or_else(|e| e.into_inner());
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
