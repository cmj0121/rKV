use std::path::PathBuf;

use clap::Parser;
use rkv::{Config, DB};
use rustyline::DefaultEditor;

#[derive(Parser)]
#[command(name = "rkv", about = "rKV — revisioned key-value store")]
struct Args {
    /// Database directory path
    path: Option<String>,

    /// Create the database if it does not exist
    #[arg(short, long, default_value_t = true)]
    create: bool,
}

enum Action {
    Continue,
    Exit,
}

fn execute(db: &DB, line: &str) -> Action {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    if tokens.is_empty() {
        return Action::Continue;
    }

    match tokens[0] {
        "put" => {
            if tokens.len() < 3 {
                eprintln!("usage: put <key> <value>");
                return Action::Continue;
            }
            match db.put(tokens[1].as_bytes(), tokens[2].as_bytes()) {
                Ok(rev) => println!("{rev:032x}"),
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "get" => {
            if tokens.len() < 2 {
                eprintln!("usage: get <key>");
                return Action::Continue;
            }
            match db.get(tokens[1].as_bytes()) {
                Ok(val) => println!("{}", String::from_utf8_lossy(&val)),
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "delete" | "del" => {
            if tokens.len() < 2 {
                eprintln!("usage: delete <key>");
                return Action::Continue;
            }
            match db.delete(tokens[1].as_bytes()) {
                Ok(()) => println!("OK"),
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "exists" => {
            if tokens.len() < 2 {
                eprintln!("usage: exists <key>");
                return Action::Continue;
            }
            match db.exists(tokens[1].as_bytes()) {
                Ok(true) => println!("true"),
                Ok(false) => println!("false"),
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "scan" => {
            let prefix = tokens.get(1).unwrap_or(&"");
            let limit: usize = tokens.get(2).and_then(|s| s.parse().ok()).unwrap_or(10);
            match db.scan(prefix.as_bytes(), limit) {
                Ok(keys) => {
                    for k in &keys {
                        println!("{}", String::from_utf8_lossy(k));
                    }
                }
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "rscan" => {
            let prefix = tokens.get(1).unwrap_or(&"");
            let limit: usize = tokens.get(2).and_then(|s| s.parse().ok()).unwrap_or(10);
            match db.rscan(prefix.as_bytes(), limit) {
                Ok(keys) => {
                    for k in &keys {
                        println!("{}", String::from_utf8_lossy(k));
                    }
                }
                Err(e) => eprintln!("error: {e}"),
            }
        }
        "count" => match db.count() {
            Ok(n) => println!("{n}"),
            Err(e) => eprintln!("error: {e}"),
        },
        "help" | "?" => {
            println!("Commands:");
            println!("  put <key> <value>    Store a key-value pair");
            println!("  get <key>            Retrieve a value by key");
            println!("  delete <key>         Remove a key (alias: del)");
            println!("  exists <key>         Check if a key exists");
            println!("  scan [prefix] [n]    Forward scan keys");
            println!("  rscan [prefix] [n]   Reverse scan keys");
            println!("  count                Count all keys");
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

fn run_repl(db: &DB) {
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

    loop {
        match rl.readline("rkv> ") {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(line);
                match execute(db, line) {
                    Action::Continue => {}
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

    run_repl(&db);

    if let Err(e) = db.close() {
        eprintln!("error closing database: {e}");
    }
}
