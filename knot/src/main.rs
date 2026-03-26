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
        _ => run_repl(&args),
    }
}

fn print_usage() {
    println!(
        r#"knot — schema-free, graph-based, temporal database

Usage:
  knot repl [path]                              Start interactive REPL (embedded)
  knot serve [path] [--port PORT]               Start HTTP server (embedded rKV)
  knot serve --remote <rkv-url> [--port PORT]   Start HTTP server (remote rKV)
  knot help                                     Show this help

Options:
  path       Database directory (default: .knot-data)
  --port     HTTP server port (default: 8400)
  --remote   Connect to a remote rKV server instead of embedded
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

fn make_embedded_backend(db: &rkv::DB) -> Arc<dyn knot::engine::backend::Backend> {
    Arc::new(unsafe { knot::engine::embedded::EmbeddedBackend::new(db) })
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
    let backend = make_embedded_backend(&db);
    let mut rl = DefaultEditor::new().expect("failed to create editor");
    let mut state = repl::State::new(backend);

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
    let mut remote_url: Option<String> = None;
    let mut db_path_idx: Option<usize> = None;

    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                if let Some(p) = args.get(i + 1) {
                    port = p.parse().unwrap_or_else(|_| {
                        eprintln!("ERROR: invalid port: {p}");
                        std::process::exit(1);
                    });
                    i += 2;
                    continue;
                }
            }
            "--remote" => {
                if let Some(url) = args.get(i + 1) {
                    remote_url = Some(url.clone());
                    i += 2;
                    continue;
                } else {
                    eprintln!("ERROR: --remote requires a URL");
                    std::process::exit(1);
                }
            }
            _ => {
                db_path_idx = Some(i);
            }
        }
        i += 1;
    }

    let (backend, mode_label): (Arc<dyn knot::engine::backend::Backend>, String) =
        if let Some(url) = &remote_url {
            let backend = Arc::new(knot::engine::remote::RemoteBackend::new(url));
            (backend, format!("remote ({url})"))
        } else {
            let path_idx = db_path_idx.unwrap_or(2);
            let (path, db) = open_db(args, path_idx);
            let db: &'static rkv::DB = Box::leak(Box::new(db));
            let backend = make_embedded_backend(db);
            (backend, format!("embedded ({})", path.display()))
        };

    let state = Arc::new(knot::server::AppState::new(backend));
    let router = knot::server::build_router(state);

    println!("Knot server starting on http://0.0.0.0:{port}");
    println!("  Backend:  {mode_label}");
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
