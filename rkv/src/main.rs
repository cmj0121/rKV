use std::path::PathBuf;

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
    Switch(String),
    Exit,
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

fn execute(db: &DB, ns: &Namespace<'_>, line: &str) -> Action {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    if tokens.is_empty() {
        return Action::Continue;
    }

    match tokens[0] {
        "use" => {
            if tokens.len() < 2 {
                eprintln!("usage: use <namespace>");
                return Action::Continue;
            }
            return Action::Switch(tokens[1].to_owned());
        }
        "namespaces" | "ns" => match db.list_namespaces() {
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
                Ok(()) => println!("OK"),
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "put" => {
            if tokens.len() < 3 {
                eprintln!("usage: put <key> <value>");
                return Action::Continue;
            }
            match ns.put(parse_key(tokens[1]), tokens[2].as_bytes()) {
                Ok(rev) => println!("{rev}"),
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
        "delete" | "del" => {
            if tokens.len() < 2 {
                eprintln!("usage: delete <key>");
                return Action::Continue;
            }
            match ns.delete(parse_key(tokens[1])) {
                Ok(()) => println!("OK"),
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "exists" => {
            if tokens.len() < 2 {
                eprintln!("usage: exists <key>");
                return Action::Continue;
            }
            match ns.exists(parse_key(tokens[1])) {
                Ok(true) => println!("true"),
                Ok(false) => println!("false"),
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "scan" => {
            let prefix = parse_key(tokens.get(1).unwrap_or(&""));
            let limit: usize = tokens.get(2).and_then(|s| s.parse().ok()).unwrap_or(10);
            match ns.scan(&prefix, limit) {
                Ok(keys) => {
                    for k in &keys {
                        println!("{k}");
                    }
                }
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "rscan" => {
            let prefix = parse_key(tokens.get(1).unwrap_or(&""));
            let limit: usize = tokens.get(2).and_then(|s| s.parse().ok()).unwrap_or(10);
            match ns.rscan(&prefix, limit) {
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
        "help" | "?" => {
            println!("Data operations:");
            println!("  put <key> <value>    Store a key-value pair");
            println!("  get <key>            Retrieve a value by key");
            println!("  delete <key>         Remove a key (alias: del)");
            println!("  exists <key>         Check if a key exists");
            println!("  scan [prefix] [n]    Forward scan keys");
            println!("  rscan [prefix] [n]   Reverse scan keys");
            println!("  count                Count all keys");
            println!("  rev <key>            Show total revisions for a key");
            println!("  rev <key> <index>    Show value at revision index (0 = oldest)");
            println!();
            println!("Namespace:");
            println!("  use <namespace>      Switch to a namespace (create if needed)");
            println!("  namespaces           List all namespaces (alias: ns)");
            println!("  drop <namespace>     Drop a namespace and all its data");
            println!();
            println!("Misc:");
            println!("  help                 Show this message (alias: ?)");
            println!("  exit                 Quit the REPL (alias: quit)");
        }
        "exit" | "quit" => return Action::Exit,
        other => eprintln!("unknown command: {other} (type 'help' for usage)"),
    }

    Action::Continue
}

fn history_path() -> Option<PathBuf> {
    dirs_sys::home_dir().map(|h| h.join(".rkv_history"))
}

fn prompt(ns_name: &str) -> String {
    if ns_name == DEFAULT_NAMESPACE {
        "rkv> ".to_owned()
    } else {
        format!("rkv [{ns_name}]> ")
    }
}

fn run_repl(db: &DB, initial_ns: &str) {
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

    loop {
        let ns = match db.namespace(&ns_name) {
            Ok(ns) => ns,
            Err(e) => {
                eprintln!("error switching namespace: {e}");
                ns_name = DEFAULT_NAMESPACE.to_owned();
                continue;
            }
        };

        match rl.readline(&prompt(ns.name())) {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(line);
                match execute(db, &ns, line) {
                    Action::Continue => {}
                    Action::Switch(name) => ns_name = name,
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

    let config = Config {
        path,
        create_if_missing: args.create,
    };

    let db = match DB::open(config) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("failed to open database: {e}");
            std::process::exit(1);
        }
    };

    run_repl(&db, &args.namespace);

    if let Err(e) = db.close() {
        eprintln!("error closing database: {e}");
    }
}
