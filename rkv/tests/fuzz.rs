//! Time-controlled fuzz test for rKV.
//!
//! Generates random sequences of DB operations, applies them to both the real DB
//! and an in-memory oracle (`HashMap`), then asserts they agree. This catches
//! subtle interaction bugs that hand-written tests miss.
//!
//! # Environment variables
//!
//! - `RKV_FUZZ_SECS`: runtime in seconds (default: 5)
//! - `RKV_FUZZ_SEED`: RNG seed for reproducibility (default: random)
//!
//! # Scope
//!
//! Tests all DB operations including `put`, `get`, `delete`, `exists`,
//! `count`, `scan`, `rscan`, `delete_range`, `delete_prefix`, `rev_count`,
//! `close+reopen`, `flush`, `compact`, and namespace switching.

use std::collections::HashMap;
use std::time::Instant;

use rkv::{Config, Error, Key, DB, DEFAULT_NAMESPACE};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const KEY_SPACE: u32 = 100;
const NAMESPACES: &[&str] = &[DEFAULT_NAMESPACE, "ns1", "ns2"];
const VERIFY_INTERVAL: u64 = 500;

// ---------------------------------------------------------------------------
// Oracle
// ---------------------------------------------------------------------------

/// Ground-truth model: `ns -> key_string -> Option<bytes>`.
/// `Some(bytes)` = live value, `None` = deleted.
struct Oracle {
    namespaces: HashMap<String, HashMap<String, Option<Vec<u8>>>>,
    /// Track write count per (ns, key) for rev_count lower-bound check.
    write_counts: HashMap<(String, String), u64>,
}

impl Oracle {
    fn new() -> Self {
        Self {
            namespaces: HashMap::new(),
            write_counts: HashMap::new(),
        }
    }

    fn ns_mut(&mut self, ns: &str) -> &mut HashMap<String, Option<Vec<u8>>> {
        self.namespaces.entry(ns.to_owned()).or_default()
    }

    fn put(&mut self, ns: &str, key: &str, value: Vec<u8>) {
        self.ns_mut(ns).insert(key.to_owned(), Some(value));
        *self
            .write_counts
            .entry((ns.to_owned(), key.to_owned()))
            .or_insert(0) += 1;
    }

    fn delete(&mut self, ns: &str, key: &str) {
        self.ns_mut(ns).insert(key.to_owned(), None);
        *self
            .write_counts
            .entry((ns.to_owned(), key.to_owned()))
            .or_insert(0) += 1;
    }

    fn get(&self, ns: &str, key: &str) -> Option<&[u8]> {
        self.namespaces
            .get(ns)
            .and_then(|m| m.get(key))
            .and_then(|v| v.as_deref())
    }

    fn exists(&self, ns: &str, key: &str) -> bool {
        self.get(ns, key).is_some()
    }

    fn count(&self, ns: &str) -> u64 {
        self.namespaces
            .get(ns)
            .map(|m| m.values().filter(|v| v.is_some()).count() as u64)
            .unwrap_or(0)
    }

    fn write_count(&self, ns: &str, key: &str) -> u64 {
        self.write_counts
            .get(&(ns.to_owned(), key.to_owned()))
            .copied()
            .unwrap_or(0)
    }

    fn scan(&self, ns: &str, prefix: &str, limit: usize, offset: usize) -> Vec<String> {
        let Some(m) = self.namespaces.get(ns) else {
            return Vec::new();
        };
        let mut keys: Vec<&String> = m
            .iter()
            .filter(|(k, v)| v.is_some() && k.starts_with(prefix))
            .map(|(k, _)| k)
            .collect();
        keys.sort();
        keys.into_iter().skip(offset).take(limit).cloned().collect()
    }

    fn rscan(&self, ns: &str, prefix: &str, limit: usize, offset: usize) -> Vec<String> {
        let Some(m) = self.namespaces.get(ns) else {
            return Vec::new();
        };
        let mut keys: Vec<&String> = m
            .iter()
            .filter(|(k, v)| v.is_some() && k.starts_with(prefix))
            .map(|(k, _)| k)
            .collect();
        keys.sort();
        keys.reverse();
        keys.into_iter().skip(offset).take(limit).cloned().collect()
    }

    fn delete_range(&mut self, ns: &str, start: &str, end: &str, inclusive: bool) -> u64 {
        let Some(m) = self.namespaces.get(ns) else {
            return 0;
        };
        let keys: Vec<String> = m
            .iter()
            .filter(|(k, v)| {
                if v.is_none() {
                    return false;
                }
                let k: &str = k.as_str();
                if inclusive {
                    k >= start && k <= end
                } else {
                    k >= start && k < end
                }
            })
            .map(|(k, _)| k.clone())
            .collect();
        let count = keys.len() as u64;
        for k in &keys {
            self.ns_mut(ns).insert(k.clone(), None);
            *self
                .write_counts
                .entry((ns.to_owned(), k.clone()))
                .or_insert(0) += 1;
        }
        count
    }

    fn delete_prefix(&mut self, ns: &str, prefix: &str) -> u64 {
        let Some(m) = self.namespaces.get(ns) else {
            return 0;
        };
        let keys: Vec<String> = m
            .iter()
            .filter(|(k, v)| v.is_some() && k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect();
        let count = keys.len() as u64;
        for k in &keys {
            self.ns_mut(ns).insert(k.clone(), None);
            *self
                .write_counts
                .entry((ns.to_owned(), k.clone()))
                .or_insert(0) += 1;
        }
        count
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn gen_key_str(rng: &mut fastrand::Rng) -> String {
    format!("k{}", rng.u32(0..KEY_SPACE))
}

fn gen_value(rng: &mut fastrand::Rng) -> Vec<u8> {
    let len = rng.usize(0..200);
    let mut buf = vec![0u8; len];
    rng.fill(&mut buf);
    buf
}

fn gen_prefix(rng: &mut fastrand::Rng) -> String {
    let choice = rng.u32(0..11);
    if choice == 10 {
        "k".to_owned()
    } else {
        format!("k{choice}")
    }
}

/// Pick a weighted random operation index (0..14).
fn gen_op(rng: &mut fastrand::Rng) -> u32 {
    let roll = rng.u32(0..100);
    match roll {
        0..28 => 0,   // put            28%
        28..46 => 1,  // get            18%
        46..56 => 2,  // delete         10%
        56..61 => 3,  // exists          5%
        61..66 => 4,  // count           5%
        66..73 => 5,  // scan            7%
        73..80 => 6,  // rscan           7%
        80..84 => 7,  // delete_range    4%
        84..88 => 8,  // delete_prefix   4%
        88..91 => 9,  // close+reopen    3%
        91..94 => 10, // switch ns       3%
        94..96 => 11, // rev_count       2%
        96..98 => 12, // flush           2%
        _ => 13,      // compact         2%
    }
}

/// Full verification: walk every oracle entry and compare against the DB.
fn verify_full(db: &DB, oracle: &Oracle, label: &str) {
    for (ns_name, entries) in &oracle.namespaces {
        let ns = db.namespace(ns_name, None).unwrap();
        for (key_str, expected) in entries {
            let result = ns.get(key_str.as_str());
            match expected {
                Some(bytes) => {
                    let val = result.unwrap_or_else(|e| {
                        panic!("[{label}] ns={ns_name} key={key_str}: expected data, got err: {e}")
                    });
                    assert_eq!(
                        val.as_bytes().unwrap(),
                        bytes.as_slice(),
                        "[{label}] ns={ns_name} key={key_str}: value mismatch"
                    );
                }
                None => match result {
                    Err(Error::KeyNotFound) => {}
                    Err(e) => panic!(
                        "[{label}] ns={ns_name} key={key_str}: expected KeyNotFound, got err: {e}"
                    ),
                    Ok(val) => panic!(
                        "[{label}] ns={ns_name} key={key_str}: expected KeyNotFound, got {val:?}"
                    ),
                },
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Main fuzz test
// ---------------------------------------------------------------------------

#[test]
fn fuzz_random_ops() {
    let fuzz_secs: u64 = std::env::var("RKV_FUZZ_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    let seed: u64 = std::env::var("RKV_FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| fastrand::u64(..));

    eprintln!("fuzz: seed={seed} duration={fuzz_secs}s");

    let mut rng = fastrand::Rng::with_seed(seed);
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("fuzzdb");

    let config = Config::new(&db_path);
    let mut db = DB::open(config).unwrap();
    let mut oracle = Oracle::new();
    let mut current_ns = DEFAULT_NAMESPACE.to_owned();

    let deadline = Instant::now() + std::time::Duration::from_secs(fuzz_secs);
    let mut op_count: u64 = 0;

    while Instant::now() < deadline {
        let op = gen_op(&mut rng);
        op_count += 1;

        match op {
            // --- put ---
            0 => {
                let key_str = gen_key_str(&mut rng);
                let value = gen_value(&mut rng);
                let ns = db.namespace(&current_ns, None).unwrap();
                let rev = ns.put(key_str.as_str(), value.as_slice(), None).unwrap();
                assert!(rev.as_u128() > 0, "put should return a positive revision");
                oracle.put(&current_ns, &key_str, value);
            }

            // --- get ---
            1 => {
                let key_str = gen_key_str(&mut rng);
                let ns = db.namespace(&current_ns, None).unwrap();
                let result = ns.get(key_str.as_str());
                match oracle.get(&current_ns, &key_str) {
                    Some(expected) => {
                        let val = result.unwrap_or_else(|e| {
                            panic!("op#{op_count} get({key_str}): expected data, got err: {e}")
                        });
                        assert_eq!(
                            val.as_bytes().unwrap(),
                            expected,
                            "op#{op_count} get({key_str}): value mismatch"
                        );
                    }
                    None => {
                        assert!(
                            matches!(result, Err(Error::KeyNotFound)),
                            "op#{op_count} get({key_str}): expected KeyNotFound, got {result:?}"
                        );
                    }
                }
            }

            // --- delete ---
            2 => {
                let key_str = gen_key_str(&mut rng);
                let ns = db.namespace(&current_ns, None).unwrap();
                let _ = ns.delete(key_str.as_str());
                oracle.delete(&current_ns, &key_str);
            }

            // --- exists ---
            3 => {
                let key_str = gen_key_str(&mut rng);
                let ns = db.namespace(&current_ns, None).unwrap();
                let db_exists = ns.exists(key_str.as_str()).unwrap();
                let oracle_exists = oracle.exists(&current_ns, &key_str);
                assert_eq!(
                    db_exists, oracle_exists,
                    "op#{op_count} exists({key_str}): db={db_exists} oracle={oracle_exists}"
                );
            }

            // --- count ---
            4 => {
                let ns = db.namespace(&current_ns, None).unwrap();
                let db_count = ns.count().unwrap();
                let oracle_count = oracle.count(&current_ns);
                assert_eq!(
                    db_count, oracle_count,
                    "op#{op_count} count: db={db_count} oracle={oracle_count}"
                );
            }

            // --- scan ---
            5 => {
                let prefix = gen_prefix(&mut rng);
                let limit = rng.usize(1..=20);
                let offset = rng.usize(0..5);
                let ns = db.namespace(&current_ns, None).unwrap();
                let prefix_key = Key::from(prefix.as_str());
                let db_keys: Vec<String> = ns
                    .scan(&prefix_key, limit, offset, false)
                    .unwrap()
                    .into_iter()
                    .map(|k| k.to_string())
                    .collect();
                let oracle_keys = oracle.scan(&current_ns, &prefix, limit, offset);
                assert_eq!(
                    db_keys, oracle_keys,
                    "op#{op_count} scan(prefix={prefix}, limit={limit}, offset={offset})"
                );
            }

            // --- rscan ---
            6 => {
                let prefix = gen_prefix(&mut rng);
                let limit = rng.usize(1..=20);
                let offset = rng.usize(0..5);
                let ns = db.namespace(&current_ns, None).unwrap();
                let prefix_key = Key::from(prefix.as_str());
                let db_keys: Vec<String> = ns
                    .rscan(&prefix_key, limit, offset, false)
                    .unwrap()
                    .into_iter()
                    .map(|k| k.to_string())
                    .collect();
                let oracle_keys = oracle.rscan(&current_ns, &prefix, limit, offset);
                assert_eq!(
                    db_keys, oracle_keys,
                    "op#{op_count} rscan(prefix={prefix}, limit={limit}, offset={offset})"
                );
            }

            // --- delete_range ---
            7 => {
                let a = gen_key_str(&mut rng);
                let b = gen_key_str(&mut rng);
                let (start, end) = if a <= b { (a, b) } else { (b, a) };
                let inclusive = rng.bool();
                let ns = db.namespace(&current_ns, None).unwrap();
                let db_count = ns
                    .delete_range(start.as_str(), end.as_str(), inclusive)
                    .unwrap();
                let oracle_count = oracle.delete_range(&current_ns, &start, &end, inclusive);
                assert_eq!(
                    db_count, oracle_count,
                    "op#{op_count} delete_range({start}..{end}, inclusive={inclusive}): db={db_count} oracle={oracle_count}"
                );
            }

            // --- delete_prefix ---
            8 => {
                let prefix = gen_prefix(&mut rng);
                let ns = db.namespace(&current_ns, None).unwrap();
                let db_count = ns.delete_prefix(&prefix).unwrap();
                let oracle_count = oracle.delete_prefix(&current_ns, &prefix);
                assert_eq!(
                    db_count, oracle_count,
                    "op#{op_count} delete_prefix({prefix}): db={db_count} oracle={oracle_count}"
                );
            }

            // --- close + reopen (AOL replay) ---
            9 => {
                db.close().unwrap();
                let config = Config::new(&db_path);
                db = DB::open(config).unwrap();
                verify_full(&db, &oracle, &format!("op#{op_count} after reopen"));
            }

            // --- switch namespace ---
            10 => {
                current_ns = NAMESPACES[rng.usize(0..NAMESPACES.len())].to_owned();
            }

            // --- rev_count ---
            11 => {
                let key_str = gen_key_str(&mut rng);
                let ns = db.namespace(&current_ns, None).unwrap();
                let result = ns.rev_count(key_str.as_str());
                let oracle_writes = oracle.write_count(&current_ns, &key_str);
                if oracle_writes == 0 {
                    assert!(
                        matches!(result, Err(Error::KeyNotFound)),
                        "op#{op_count} rev_count({key_str}): never written, expected KeyNotFound, got {result:?}"
                    );
                } else {
                    let db_revs = result.unwrap_or_else(|e| {
                        panic!("op#{op_count} rev_count({key_str}): expected count, got err: {e}")
                    });
                    assert!(
                        db_revs >= oracle_writes,
                        "op#{op_count} rev_count({key_str}): db={db_revs} < oracle_writes={oracle_writes}"
                    );
                }
            }

            // --- flush ---
            12 => {
                db.flush().unwrap();
            }

            // --- compact ---
            13 => {
                db.compact().unwrap();
                db.wait_for_compaction();
            }

            _ => unreachable!(),
        }

        // Periodic full verification.
        if op_count.is_multiple_of(VERIFY_INTERVAL) {
            verify_full(&db, &oracle, &format!("periodic @{op_count}"));
        }
    }

    // Final full verification.
    verify_full(&db, &oracle, "final");

    eprintln!("fuzz: completed {op_count} ops in {fuzz_secs}s (seed={seed})");
    db.close().unwrap();
}
