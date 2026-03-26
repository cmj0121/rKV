use std::path::PathBuf;
use std::sync::Arc;

use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

mod repl;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let command = args.get(1).map(|s| s.as_str()).unwrap_or("repl");

    match command {
        "serve" => run_server(&args),
        "repl" => run_repl(&args),
        "--help" | "-h" | "help" => print_usage(),
        _ => {
            // Default: treat first arg as path for REPL
            run_repl(&args);
        }
    }
}

fn print_usage() {
    println!(
        r#"knot — schema-free, graph-based, temporal database

Usage:
  knot repl [path]           Start interactive REPL (default)
  knot serve [path] [--port PORT]  Start HTTP server
  knot help                  Show this help

Options:
  path    Database directory (default: .knot-data)
  --port  HTTP server port (default: 8400)
"#
    );
}

fn open_db(args: &[String], default_arg_pos: usize) -> (PathBuf, rkv::DB) {
    let path = args
        .get(default_arg_pos)
        .map(|s| s.to_owned())
        .unwrap_or_else(|| ".knot-data".to_owned());

    let path = PathBuf::from(&path);
    let config = rkv::Config::new(&path);
    let db = match rkv::DB::open(config) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("ERROR: failed to open database at {}: {e}", path.display());
            std::process::exit(1);
        }
    };
    (path, db)
}

fn run_repl(args: &[String]) {
    let (_path, db) = open_db(
        args,
        if args.get(1).map(|s| s.as_str()) == Some("repl") {
            2
        } else {
            1
        },
    );

    let mut rl = DefaultEditor::new().expect("failed to create editor");
    let mut state = repl::State::new(&db);

    loop {
        let prompt = state.prompt();
        match rl.readline(&prompt) {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(line);
                match repl::execute(&mut state, line) {
                    repl::Action::Continue => {}
                    repl::Action::Exit => break,
                }
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
            Err(e) => {
                eprintln!("ERROR: {e}");
                break;
            }
        }
    }
}

fn run_server(args: &[String]) {
    let mut port: u16 = 8400;
    let mut db_path_idx = 2;

    // Parse --port flag
    let mut i = 2;
    while i < args.len() {
        if args[i] == "--port" {
            if let Some(p) = args.get(i + 1) {
                port = p.parse().unwrap_or_else(|_| {
                    eprintln!("ERROR: invalid port: {p}");
                    std::process::exit(1);
                });
                i += 2;
                continue;
            }
        }
        db_path_idx = i;
        i += 1;
    }

    let (path, db) = open_db(args, db_path_idx);
    // Leak the DB to get 'static lifetime for the server
    let db: &'static rkv::DB = Box::leak(Box::new(db));

    let state = Arc::new(knot::server::AppState::new(db));
    let router = knot::server::build_router(state);

    println!("Knot server starting on http://0.0.0.0:{port}");
    println!("  Database: {}", path.display());
    println!("  UI:       http://localhost:{port}/");
    println!("  API:      http://localhost:{port}/api/");
    println!("  Docs:     http://localhost:{port}/docs");

    let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    rt.block_on(async {
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
            .await
            .unwrap_or_else(|e| {
                eprintln!("ERROR: failed to bind port {port}: {e}");
                std::process::exit(1);
            });
        axum::serve(listener, router).await.unwrap();
    });
}
