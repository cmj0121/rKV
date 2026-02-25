use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use rkv::{Config, Key, Namespace, DB, DEFAULT_NAMESPACE};
use rustyline::DefaultEditor;

#[derive(Parser)]
#[command(name = "rkv", about = "rKV — revisioned key-value store")]
struct Args {
    /// Database directory path
    path: Option<String>,

    /// Initial namespace to use
    #[arg(short, long, default_value = DEFAULT_NAMESPACE)]
    namespace: String,

    /// Create the database if it does not exist
    #[arg(short, long, default_value_t = true)]
    create: bool,
}

enum Action {
    Continue,
    Switch(String, Option<String>),
    Exit,
}

fn parse_duration(s: &str) -> Option<Duration> {
    let (num, unit) = if let Some(n) = s.strip_suffix('d') {
        (n.parse::<u64>().ok()?, 86400)
    } else if let Some(n) = s.strip_suffix('h') {
        (n.parse::<u64>().ok()?, 3600)
    } else if let Some(n) = s.strip_suffix('m') {
        (n.parse::<u64>().ok()?, 60)
    } else if let Some(n) = s.strip_suffix('s') {
        (n.parse::<u64>().ok()?, 1)
    } else {
        (s.parse::<u64>().ok()?, 1)
    };
    Some(Duration::from_secs(num * unit))
}

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs == 0 {
        return "0s".to_owned();
    }
    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let mins = (secs % 3600) / 60;
    let s = secs % 60;
    let mut parts = Vec::new();
    if days > 0 {
        parts.push(format!("{days}d"));
    }
    if hours > 0 {
        parts.push(format!("{hours}h"));
    }
    if mins > 0 {
        parts.push(format!("{mins}m"));
    }
    if s > 0 {
        parts.push(format!("{s}s"));
    }
    parts.join("")
}

fn format_bytes(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = 1024 * 1024;
    const GB: usize = 1024 * 1024 * 1024;

    if bytes == 0 {
        "0".to_owned()
    } else if bytes.is_multiple_of(GB) {
        format!("{} GB", bytes / GB)
    } else if bytes.is_multiple_of(MB) {
        format!("{} MB", bytes / MB)
    } else if bytes.is_multiple_of(KB) {
        format!("{} KB", bytes / KB)
    } else {
        format!("{bytes}")
    }
}

fn parse_key(token: &str) -> Key {
    match token {
        "true" => Key::Int(1),
        "false" => Key::Int(0),
        _ => token
            .parse::<i64>()
            .map(Key::Int)
            .unwrap_or_else(|_| Key::from(token)),
    }
}

fn print_command_help(cmd: &str) {
    match cmd {
        "put" => {
            println!("put <key> <value|@file> [ttl]");
            println!();
            println!("  Store a key-value pair in the current namespace.");
            println!();
            println!("  Arguments:");
            println!("    key     Integer or string key");
            println!("    value   Literal value, @file to read from file, @@x for literal \"@x\"");
            println!("    ttl     Optional expiration: 10s, 5m, 2h, 1d");
            println!();
            println!("  Examples:");
            println!("    put name Alice");
            println!("    put counter 42 10m");
            println!("    put avatar @photo.png");
            println!();
            println!("  See also: get, del, ttl");
        }
        "get" => {
            println!("get <key>");
            println!();
            println!("  Retrieve the value for a key in the current namespace.");
            println!("  Returns an error if the key does not exist or has expired.");
            println!();
            println!("  Examples:");
            println!("    get name");
            println!("    get 42");
            println!();
            println!("  See also: put, has, rev");
        }
        "del" => {
            println!("del <key>");
            println!();
            println!("  Remove a single key from the current namespace.");
            println!("  The key is tombstoned and will no longer appear in scans.");
            println!();
            println!("  Examples:");
            println!("    del name");
            println!("    del 42");
            println!();
            println!("  See also: wipe, has");
        }
        "wipe" => {
            println!("wipe <prefix>*           Remove keys matching prefix");
            println!("wipe <start>..<end>      Remove keys in range [start, end)");
            println!("wipe <start>..=<end>     Remove keys in range [start, end]");
            println!();
            println!("  Bulk-delete keys by prefix or range. Returns the count of");
            println!("  keys actually deleted. Tombstoned/expired keys are excluded.");
            println!();
            println!("  Prefix mode (trailing *):");
            println!("    Deletes all live keys whose string form starts with the prefix.");
            println!("    The `*` is required and must have at least one char before it.");
            println!();
            println!("  Range mode (..):");
            println!("    Deletes keys in the range. Uses BTreeMap ordering.");
            println!("    `..` is exclusive (half-open), `..=` is inclusive (closed).");
            println!("    Both start and end are required.");
            println!();
            println!("  Examples:");
            println!("    wipe user_*          delete user_1, user_2, user_abc, ...");
            println!("    wipe 1..10           delete keys 1..9 (exclusive end)");
            println!("    wipe 1..=10          delete keys 1..10 (inclusive end)");
            println!("    wipe aaa..zzz        delete string keys in [aaa, zzz)");
            println!();
            println!("  See also: del, scan, count");
        }
        "has" => {
            println!("has <key>");
            println!();
            println!("  Check if a key exists (non-expired, non-tombstoned).");
            println!("  Prints \"true\" or \"false\".");
            println!();
            println!("  Examples:");
            println!("    has name");
            println!();
            println!("  See also: get, del");
        }
        "ttl" => {
            println!("ttl <key>");
            println!();
            println!("  Show the remaining time-to-live for a key.");
            println!("  Prints a duration (e.g., 4m30s) or \"none\" if no expiration.");
            println!();
            println!("  Examples:");
            println!("    ttl session_token");
            println!();
            println!("  See also: put");
        }
        "scan" => {
            println!("scan [prefix] [:n] [+offset]");
            println!();
            println!("  Forward-scan keys in sorted order. Shows up to n keys");
            println!("  (default 10) starting after offset matching keys.");
            println!();
            println!("  Arguments:");
            println!("    prefix   Optional key prefix filter");
            println!("    :n       Limit results (default :10)");
            println!("    +offset  Skip first N matches");
            println!();
            println!("  Examples:");
            println!("    scan");
            println!("    scan user :5");
            println!("    scan user :10 +20");
            println!();
            println!("  See also: rscan, count");
        }
        "rscan" => {
            println!("rscan [prefix] [:n] [+offset]");
            println!();
            println!("  Reverse-scan keys (descending order). Same arguments as scan.");
            println!();
            println!("  Examples:");
            println!("    rscan");
            println!("    rscan user :5");
            println!();
            println!("  See also: scan, count");
        }
        "count" => {
            println!("count");
            println!();
            println!("  Count all live keys in the current namespace.");
            println!("  Tombstoned and expired keys are excluded.");
            println!();
            println!("  See also: scan, stats");
        }
        "rev" => {
            println!("rev <key>          Show total revision count");
            println!("rev <key> <index>  Show value at revision index (0 = oldest)");
            println!();
            println!("  Access the revision history for a key. Each put creates a");
            println!("  new revision. Index 0 is the oldest, last index is current.");
            println!();
            println!("  Examples:");
            println!("    rev name          prints: 3");
            println!("    rev name 0        prints: first-value");
            println!("    rev name 2        prints: latest-value");
            println!();
            println!("  See also: get, put");
        }
        "use" => {
            println!("use <namespace> [+]");
            println!();
            println!("  Switch to a different namespace. Creates the namespace if");
            println!("  it does not exist. Append + to enable encryption (will");
            println!("  prompt for a password).");
            println!();
            println!("  Examples:");
            println!("    use logs");
            println!("    use secrets +");
            println!();
            println!("  See also: namespaces, drop");
        }
        "namespaces" => {
            println!("namespaces");
            println!();
            println!("  List all namespaces in the database.");
            println!();
            println!("  See also: use, drop");
        }
        "drop" => {
            println!("drop <namespace>");
            println!();
            println!("  Drop a namespace and all its data. This is irreversible.");
            println!();
            println!("  Examples:");
            println!("    drop temp_data");
            println!();
            println!("  See also: use, namespaces");
        }
        "stats" => {
            println!("stats");
            println!();
            println!("  Print database statistics (storage, LSM, operations, uptime).");
            println!("  Reads live counters without persisting. For a persistent");
            println!("  snapshot, use `analyze`.");
            println!();
            println!("  See also: analyze, config");
        }
        "analyze" => {
            println!("analyze");
            println!();
            println!("  Re-derive statistics from live state and persist operation");
            println!("  counters to stats.meta. Same output as `stats` but ensures");
            println!("  counters survive a restart.");
            println!();
            println!("  See also: stats");
        }
        "config" => {
            println!("config                       Print current configuration");
            println!("config <group.key> <value>   Set a configuration value");
            println!();
            println!("  View or modify runtime configuration. Changes take effect");
            println!("  immediately but are not persisted to disk.");
            println!();
            println!("  Groups: storage, lsm, object, io, aol, revision");
            println!();
            println!("  Examples:");
            println!("    config");
            println!("    config lsm.write_buffer_size 8388608");
            println!("    config object.compress false");
            println!();
            println!("  See also: stats");
        }
        "flush" => {
            println!("flush");
            println!();
            println!("  Flush the write buffer (memtable) to an SSTable on disk.");
            println!();
            println!("  See also: sync, compact");
        }
        "sync" => {
            println!("sync");
            println!();
            println!("  Flush and fsync to ensure data reaches durable storage.");
            println!();
            println!("  See also: flush, compact");
        }
        "compact" => {
            println!("compact");
            println!();
            println!("  Trigger manual compaction to merge SSTables and reclaim space.");
            println!();
            println!("  See also: flush, sync");
        }
        "destroy" => {
            println!("destroy <path>");
            println!();
            println!("  Destroy a database directory and all its data.");
            println!("  WARNING: This is irreversible.");
            println!();
            println!("  Examples:");
            println!("    destroy /tmp/test-db");
            println!();
            println!("  See also: repair");
        }
        "repair" => {
            println!("repair <path>");
            println!();
            println!("  Attempt to repair a corrupted database. Reports the number");
            println!("  of records scanned, skipped, recovered, and lost.");
            println!();
            println!("  Examples:");
            println!("    repair /tmp/broken-db");
            println!();
            println!("  See also: destroy, dump");
        }
        "dump" => {
            println!("dump <path>");
            println!();
            println!("  Export the database to a backup file.");
            println!();
            println!("  Examples:");
            println!("    dump /tmp/backup.rkv");
            println!();
            println!("  See also: repair");
        }
        _ => {
            eprintln!("unknown command: {cmd} (type 'help' for usage)");
        }
    }
}

fn execute(db: &DB, ns: &Namespace<'_>, line: &str) -> Action {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    if tokens.is_empty() {
        return Action::Continue;
    }

    match tokens[0] {
        "use" => {
            if tokens.len() < 2 {
                eprintln!("usage: use <namespace> [+]");
                return Action::Continue;
            }
            let name = tokens[1].to_owned();
            let password = if tokens.get(2) == Some(&"+") {
                eprint!("Password: ");
                match rpassword::read_password() {
                    Ok(pw) if pw.is_empty() => {
                        eprintln!("error: password must not be empty");
                        return Action::Continue;
                    }
                    Ok(pw) => Some(pw),
                    Err(e) => {
                        eprintln!("error: failed to read password: {e}");
                        return Action::Continue;
                    }
                }
            } else {
                None
            };
            return Action::Switch(name, password);
        }
        "namespaces" => match db.list_namespaces() {
            Ok(names) => {
                for name in &names {
                    println!("{name}");
                }
            }
            Err(e) => eprintln!("error: {e}"),
        },
        "drop" => {
            if tokens.len() < 2 {
                eprintln!("usage: drop <namespace>");
                return Action::Continue;
            }
            match db.drop_namespace(tokens[1]) {
                Ok(()) => {
                    println!("OK");
                    // If the user dropped the namespace they're currently in,
                    // switch back to the default namespace.
                    if tokens[1] == ns.name() {
                        return Action::Switch(DEFAULT_NAMESPACE.to_owned(), None);
                    }
                }
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "put" => {
            if tokens.len() < 3 {
                eprintln!("usage: put <key> <value|@file> [ttl]");
                return Action::Continue;
            }
            let ttl = if let Some(ttl_str) = tokens.get(3) {
                match parse_duration(ttl_str) {
                    Some(ttl) => Some(ttl),
                    None => {
                        eprintln!("error: invalid TTL '{ttl_str}' (e.g., 10s, 5m, 2h, 1d)");
                        return Action::Continue;
                    }
                }
            } else {
                None
            };
            let value: Vec<u8> = if let Some(rest) = tokens[2].strip_prefix("@@") {
                // Escape: @@foo → literal "@foo"
                format!("@{rest}").into_bytes()
            } else if let Some(path) = tokens[2].strip_prefix('@') {
                // File read: @/path/to/file → file contents
                match std::fs::read(path) {
                    Ok(data) => data,
                    Err(e) => {
                        eprintln!("error: cannot read file '{path}': {e}");
                        return Action::Continue;
                    }
                }
            } else {
                tokens[2].as_bytes().to_vec()
            };
            let result = ns.put(parse_key(tokens[1]), value, ttl);
            match result {
                Ok(_) => {}
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "get" => {
            if tokens.len() < 2 {
                eprintln!("usage: get <key>");
                return Action::Continue;
            }
            match ns.get(parse_key(tokens[1])) {
                Ok(val) => println!("{val}"),
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "del" => {
            if tokens.len() < 2 {
                eprintln!("usage: del <key>");
                return Action::Continue;
            }
            match ns.delete(parse_key(tokens[1])) {
                Ok(()) => println!("OK"),
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "wipe" => {
            if tokens.len() < 2 {
                eprintln!("usage: wipe <prefix>* | <start>..<end> | <start>..=<end>");
                return Action::Continue;
            }
            let arg = tokens[1];
            if let Some((start_str, end_str)) = arg.split_once("..=") {
                // Inclusive range: wipe 1..=10
                if start_str.is_empty() || end_str.is_empty() {
                    eprintln!("usage: wipe <start>..=<end> (both sides required)");
                    return Action::Continue;
                }
                let start = parse_key(start_str);
                let end = parse_key(end_str);
                match ns.delete_range(start, end, true) {
                    Ok(n) => println!("({n} deleted)"),
                    Err(e) => eprintln!("error: {e}"),
                }
            } else if let Some((start_str, end_str)) = arg.split_once("..") {
                // Exclusive range: wipe 1..10
                if start_str.is_empty() || end_str.is_empty() {
                    eprintln!("usage: wipe <start>..<end> (both sides required)");
                    return Action::Continue;
                }
                let start = parse_key(start_str);
                let end = parse_key(end_str);
                match ns.delete_range(start, end, false) {
                    Ok(n) => println!("({n} deleted)"),
                    Err(e) => eprintln!("error: {e}"),
                }
            } else if arg.len() > 1 && arg.ends_with('*') {
                // Prefix: wipe user_*
                let prefix = &arg[..arg.len() - 1];
                match ns.delete_prefix(prefix) {
                    Ok(n) => println!("({n} deleted)"),
                    Err(e) => eprintln!("error: {e}"),
                }
            } else {
                eprintln!("usage: wipe <prefix>* | <start>..<end> | <start>..=<end>");
            }
        }
        "has" => {
            if tokens.len() < 2 {
                eprintln!("usage: has <key>");
                return Action::Continue;
            }
            match ns.exists(parse_key(tokens[1])) {
                Ok(true) => println!("true"),
                Ok(false) => println!("false"),
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "scan" | "rscan" => {
            let mut prefix_token = "";
            let mut limit: usize = 10;
            let mut offset: usize = 0;
            for tok in &tokens[1..] {
                if let Some(n) = tok.strip_prefix(':') {
                    if let Ok(v) = n.parse::<usize>() {
                        limit = v;
                    }
                } else if let Some(n) = tok.strip_prefix('+') {
                    if let Ok(v) = n.parse::<usize>() {
                        offset = v;
                    }
                } else if prefix_token.is_empty() {
                    prefix_token = tok;
                }
            }
            let prefix = parse_key(prefix_token);
            let result = if tokens[0] == "scan" {
                ns.scan(&prefix, limit, offset)
            } else {
                ns.rscan(&prefix, limit, offset)
            };
            match result {
                Ok(keys) => {
                    for k in &keys {
                        println!("{k}");
                    }
                }
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "count" => match ns.count() {
            Ok(n) => println!("{n}"),
            Err(e) => eprintln!("error: {e}"),
        },
        "rev" => {
            if tokens.len() < 2 {
                eprintln!("usage: rev <key> [index]");
                return Action::Continue;
            }
            let key = parse_key(tokens[1]);
            if let Some(idx_str) = tokens.get(2) {
                match idx_str.parse::<u64>() {
                    Ok(idx) => match ns.rev_get(key, idx) {
                        Ok(val) => println!("{val}"),
                        Err(e) => eprintln!("error: {e}"),
                    },
                    Err(_) => eprintln!("error: '{idx_str}' is not a valid revision index"),
                }
            } else {
                match ns.rev_count(key) {
                    Ok(n) => println!("{n}"),
                    Err(e) => eprintln!("error: {e}"),
                }
            }
        }
        "ttl" => {
            if tokens.len() < 2 {
                eprintln!("usage: ttl <key>");
                return Action::Continue;
            }
            match ns.ttl(parse_key(tokens[1])) {
                Ok(Some(d)) => println!("{}", format_duration(d)),
                Ok(None) => println!("none"),
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "stats" | "analyze" => {
            let s = if tokens[0] == "analyze" {
                db.analyze()
            } else {
                db.stats()
            };
            println!("Storage:");
            println!("  total_keys:        {}", s.total_keys);
            println!("  data_size_bytes:   {}", s.data_size_bytes);
            println!("  namespace_count:   {}", s.namespace_count);
            println!("LSM:");
            println!("  level_count:       {}", s.level_count);
            println!("  sstable_count:     {}", s.sstable_count);
            println!("  write_buffer_bytes:{}", s.write_buffer_bytes);
            println!("  pending_compactions:{}", s.pending_compactions);
            println!("Operations:");
            println!("  op_puts:           {}", s.op_puts);
            println!("  op_gets:           {}", s.op_gets);
            println!("  op_deletes:        {}", s.op_deletes);
            println!("  cache_hits:        {}", s.cache_hits);
            println!("  cache_misses:      {}", s.cache_misses);
            println!("Uptime:");
            println!("  uptime:            {}", format_duration(s.uptime));
        }
        "config" => {
            // config print is handled here; config set is handled in run_repl
            let c = db.config();
            let items: &[(&str, &str, String)] = &[
                ("Storage", "", String::new()),
                ("  storage.path", "", c.path.display().to_string()),
                (
                    "  storage.create_if_missing",
                    "create dir if absent",
                    c.create_if_missing.to_string(),
                ),
                ("", "", String::new()),
                ("LSM", "", String::new()),
                (
                    "  lsm.write_buffer_size",
                    "memtable flush threshold",
                    format_bytes(c.write_buffer_size),
                ),
                (
                    "  lsm.max_levels",
                    "SSTable levels",
                    c.max_levels.to_string(),
                ),
                (
                    "  lsm.block_size",
                    "SSTable block size",
                    format_bytes(c.block_size),
                ),
                (
                    "  lsm.compression",
                    "SSTable block compression",
                    c.compression.to_string(),
                ),
                (
                    "  lsm.cache_size",
                    "block cache size",
                    format_bytes(c.cache_size),
                ),
                (
                    "  lsm.bloom_bits",
                    "bits per key (0 = disabled)",
                    c.bloom_bits.to_string(),
                ),
                (
                    "  lsm.verify_checksums",
                    "verify on read",
                    c.verify_checksums.to_string(),
                ),
                (
                    "  lsm.l0_max_count",
                    "L0 file count trigger",
                    c.l0_max_count.to_string(),
                ),
                (
                    "  lsm.l0_max_size",
                    "L0 total size trigger",
                    format_bytes(c.l0_max_size),
                ),
                (
                    "  lsm.l1_max_size",
                    "L1 size cap",
                    format_bytes(c.l1_max_size),
                ),
                (
                    "  lsm.default_max_size",
                    "L2+ default size cap",
                    format_bytes(c.default_max_size),
                ),
                ("", "", String::new()),
                ("Objects", "", String::new()),
                (
                    "  object.size",
                    "value separation threshold",
                    format_bytes(c.object_size),
                ),
                (
                    "  object.compress",
                    "LZ4-compress bin objects",
                    c.compress.to_string(),
                ),
                ("", "", String::new()),
                ("I/O", "", String::new()),
                (
                    "  io.model",
                    "file I/O strategy (none, directio, mmap)",
                    c.io_model.to_string(),
                ),
                ("", "", String::new()),
                ("AOL", "", String::new()),
                (
                    "  aol.buffer_size",
                    "flush threshold (0 = per-record)",
                    c.aol_buffer_size.to_string(),
                ),
                ("", "", String::new()),
                ("Revision", "", String::new()),
                (
                    "  revision.cluster_id",
                    "RevisionID cluster (none = random)",
                    c.cluster_id
                        .map(|id| format!("{id}"))
                        .unwrap_or_else(|| "none".to_owned()),
                ),
            ];
            for (key, desc, val) in items {
                if key.is_empty() {
                    println!();
                } else if desc.is_empty() && !key.starts_with(' ') {
                    println!("{key}:");
                } else if desc.is_empty() {
                    println!("  {:<24} {}", &key[2..], val);
                } else {
                    println!("  {:<24} {:<7} # {}", &key[2..], val, desc);
                }
            }
        }
        "flush" => match db.flush() {
            Ok(()) => println!("OK"),
            Err(e) => eprintln!("error: {e}"),
        },
        "sync" => match db.sync() {
            Ok(()) => println!("OK"),
            Err(e) => eprintln!("error: {e}"),
        },
        "compact" => match db.compact() {
            Ok(()) => println!("OK"),
            Err(e) => eprintln!("error: {e}"),
        },
        "destroy" => {
            if tokens.len() < 2 {
                eprintln!("usage: destroy <path>");
                return Action::Continue;
            }
            match DB::destroy(tokens[1]) {
                Ok(()) => println!("OK"),
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "repair" => {
            if tokens.len() < 2 {
                eprintln!("usage: repair <path>");
                return Action::Continue;
            }
            match DB::repair(tokens[1]) {
                Ok(report) => {
                    println!("Repair complete:");
                    println!("  wal_records_scanned:     {}", report.wal_records_scanned);
                    println!("  wal_records_skipped:     {}", report.wal_records_skipped);
                    println!(
                        "  sstable_blocks_scanned:  {}",
                        report.sstable_blocks_scanned
                    );
                    println!(
                        "  sstable_blocks_corrupted:{}",
                        report.sstable_blocks_corrupted
                    );
                    println!("  objects_scanned:         {}", report.objects_scanned);
                    println!("  objects_corrupted:       {}", report.objects_corrupted);
                    println!("  keys_recovered:          {}", report.keys_recovered);
                    println!("  keys_lost:               {}", report.keys_lost);
                    if report.is_clean() {
                        println!("  status: clean");
                    } else if report.has_data_loss() {
                        println!("  status: DATA LOSS ({} keys lost)", report.keys_lost);
                    } else {
                        println!(
                            "  status: repaired ({} corrupted entries fixed)",
                            report.total_corrupted()
                        );
                    }
                    for warning in &report.warnings {
                        println!("  warning: {warning}");
                    }
                }
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "dump" => {
            if tokens.len() < 2 {
                eprintln!("usage: dump <path>");
                return Action::Continue;
            }
            match db.dump(tokens[1]) {
                Ok(()) => println!("OK"),
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "help" | "?" => {
            if let Some(cmd) = tokens.get(1) {
                print_command_help(cmd);
            } else {
                println!("Data operations:");
                println!(
                    "  put <key> <value|@file> [ttl]  Store a key-value pair (ttl: 10s, 5m, 2h, 1d)"
                );
                println!("  get <key>                Retrieve a value by key");
                println!("  del <key>                Remove a key");
                println!("  wipe <prefix>*           Remove keys matching prefix");
                println!("  wipe <start>..<end>      Remove keys in range [start, end)");
                println!("  wipe <start>..=<end>     Remove keys in range [start, end]");
                println!("  has <key>                Check if a key exists");
                println!("  ttl <key>                Show remaining TTL or \"none\"");
                println!("  scan [prefix] [:n] [+offset]   Forward scan keys");
                println!("  rscan [prefix] [:n] [+offset]  Reverse scan keys");
                println!("  count                    Count all keys");
                println!("  rev <key>                Show total revisions for a key");
                println!("  rev <key> <index>        Show value at revision index (0 = oldest)");
                println!();
                println!("Namespace:");
                println!(
                    "  use <namespace> [+]  Switch to namespace (+ = encrypted, prompts for password)"
                );
                println!("  namespaces           List all namespaces");
                println!("  drop <namespace>     Drop a namespace and all its data");
                println!();
                println!("Admin:");
                println!("  stats                Print database statistics");
                println!("  analyze              Re-derive stats and persist counters");
                println!("  config                    Print current configuration");
                println!("  config <group.key> <value> Set a configuration value");
                println!("  flush                Flush write buffer to disk");
                println!("  sync                 Flush and fsync to durable storage");
                println!("  compact              Trigger manual compaction");
                println!("  destroy <path>       Destroy a database (all data deleted)");
                println!("  repair <path>        Attempt to repair a corrupted database");
                println!("  dump <path>          Export database to a backup file");
                println!();
                println!("Misc:");
                println!("  clear                Clear the screen");
                println!("  help [command]       Show help (alias: ?)");
                println!("  exit                 Quit the REPL (alias: quit)");
            }
        }
        "clear" => {
            print!("\x1B[2J\x1B[H");
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
        "exit" | "quit" => return Action::Exit,
        other => eprintln!("unknown command: {other} (type 'help' for usage)"),
    }

    Action::Continue
}

fn set_config(db: &mut DB, key: &str, value: &str) {
    let c = db.config_mut();
    match key {
        "storage.create_if_missing" => match value.parse::<bool>() {
            Ok(v) => {
                c.create_if_missing = v;
                println!("OK");
            }
            Err(_) => eprintln!("error: expected true or false"),
        },
        "lsm.write_buffer_size" => match value.parse::<usize>() {
            Ok(v) => {
                c.write_buffer_size = v;
                println!("OK");
            }
            Err(_) => eprintln!("error: expected a number"),
        },
        "lsm.max_levels" => match value.parse::<usize>() {
            Ok(v) => {
                c.max_levels = v;
                println!("OK");
            }
            Err(_) => eprintln!("error: expected a number"),
        },
        "lsm.block_size" => match value.parse::<usize>() {
            Ok(v) => {
                c.block_size = v;
                println!("OK");
            }
            Err(_) => eprintln!("error: expected a number"),
        },
        "lsm.compression" => match value.parse::<rkv::Compression>() {
            Ok(v) => {
                c.compression = v;
                println!("OK");
            }
            Err(e) => eprintln!("error: {e}"),
        },
        "lsm.cache_size" => match value.parse::<usize>() {
            Ok(v) => {
                c.cache_size = v;
                println!("OK");
            }
            Err(_) => eprintln!("error: expected a number"),
        },
        "object.size" => match value.parse::<usize>() {
            Ok(v) => {
                c.object_size = v;
                println!("OK");
            }
            Err(_) => eprintln!("error: expected a number"),
        },
        "object.compress" => match value.parse::<bool>() {
            Ok(v) => {
                c.compress = v;
                println!("OK");
            }
            Err(_) => eprintln!("error: expected true or false"),
        },
        "lsm.verify_checksums" => match value.parse::<bool>() {
            Ok(v) => {
                c.verify_checksums = v;
                println!("OK");
            }
            Err(_) => eprintln!("error: expected true or false"),
        },
        "lsm.bloom_bits" => match value.parse::<usize>() {
            Ok(v) => {
                c.bloom_bits = v;
                println!("OK");
            }
            Err(_) => eprintln!("error: expected a number"),
        },
        "io.model" => match value.parse::<rkv::IoModel>() {
            Ok(v) => {
                c.io_model = v;
                println!("OK");
            }
            Err(e) => eprintln!("error: {e}"),
        },
        "revision.cluster_id" => match value {
            "none" => {
                c.cluster_id = None;
                println!("OK");
            }
            _ => match value.parse::<u16>() {
                Ok(v) => {
                    c.cluster_id = Some(v);
                    println!("OK");
                }
                Err(_) => eprintln!("error: expected a number or 'none'"),
            },
        },
        "aol.buffer_size" => match value.parse::<usize>() {
            Ok(v) => {
                c.aol_buffer_size = v;
                println!("OK");
            }
            Err(_) => eprintln!("error: expected a number"),
        },
        "lsm.l0_max_count" => match value.parse::<usize>() {
            Ok(v) => {
                c.l0_max_count = v;
                println!("OK");
            }
            Err(_) => eprintln!("error: expected a number"),
        },
        "lsm.l0_max_size" => match value.parse::<usize>() {
            Ok(v) => {
                c.l0_max_size = v;
                println!("OK");
            }
            Err(_) => eprintln!("error: expected a number"),
        },
        "lsm.l1_max_size" => match value.parse::<usize>() {
            Ok(v) => {
                c.l1_max_size = v;
                println!("OK");
            }
            Err(_) => eprintln!("error: expected a number"),
        },
        "lsm.default_max_size" => match value.parse::<usize>() {
            Ok(v) => {
                c.default_max_size = v;
                println!("OK");
            }
            Err(_) => eprintln!("error: expected a number"),
        },
        "storage.path" => eprintln!("error: path cannot be changed at runtime"),
        _ => eprintln!("error: unknown config key '{key}'"),
    }
}

fn history_path() -> Option<PathBuf> {
    dirs_sys::home_dir().map(|h| h.join(".rkv_history"))
}

fn prompt(ns_name: &str, encrypted: bool) -> String {
    if ns_name == DEFAULT_NAMESPACE {
        "rkv> ".to_owned()
    } else if encrypted {
        format!("rkv [{ns_name}+]> ")
    } else {
        format!("rkv [{ns_name}]> ")
    }
}

fn run_repl(db: &mut DB, initial_ns: &str) {
    let mut rl = match DefaultEditor::new() {
        Ok(rl) => rl,
        Err(e) => {
            eprintln!("failed to initialize editor: {e}");
            return;
        }
    };

    if let Some(path) = history_path() {
        let _ = rl.load_history(&path);
    }

    let mut ns_name = initial_ns.to_owned();
    let mut ns_password: Option<String> = None;
    let mut ns_encrypted = false;

    loop {
        match rl.readline(&prompt(&ns_name, ns_encrypted)) {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(line);

                // Handle config set before creating namespace (needs &mut db)
                let tokens: Vec<&str> = line.split_whitespace().collect();
                if tokens[0] == "config" && tokens.len() == 3 {
                    set_config(db, tokens[1], tokens[2]);
                    continue;
                }

                let ns = match db.namespace(&ns_name, ns_password.as_deref()) {
                    Ok(ns) => ns,
                    Err(e) => {
                        eprintln!("error switching namespace: {e}");
                        ns_name = DEFAULT_NAMESPACE.to_owned();
                        ns_password = None;
                        ns_encrypted = false;
                        continue;
                    }
                };
                match execute(db, &ns, line) {
                    Action::Continue => {}
                    Action::Switch(name, pw) => {
                        // Validate the switch before committing state
                        let encrypted = pw.is_some();
                        match db.namespace(&name, pw.as_deref()) {
                            Ok(_) => {
                                ns_name = name;
                                ns_password = pw;
                                ns_encrypted = encrypted;
                            }
                            Err(e) => eprintln!("error: {e}"),
                        }
                    }
                    Action::Exit => break,
                }
            }
            Err(
                rustyline::error::ReadlineError::Interrupted | rustyline::error::ReadlineError::Eof,
            ) => {
                break;
            }
            Err(e) => {
                eprintln!("error: {e}");
                break;
            }
        }
    }

    if let Some(path) = history_path() {
        let _ = rl.save_history(&path);
    }

    println!("~ Bye ~");
}

fn main() {
    let args = Args::parse();

    let path = args.path.map(PathBuf::from).unwrap_or_else(|| {
        dirs_sys::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".rkv")
    });

    let mut config = Config::new(&path);
    config.create_if_missing = args.create;

    let mut db = match DB::open(config) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("failed to open database: {e}");
            std::process::exit(1);
        }
    };

    run_repl(&mut db, &args.namespace);

    if let Err(e) = db.close() {
        eprintln!("error closing database: {e}");
    }
}
