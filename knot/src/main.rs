use std::path::PathBuf;

use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

mod repl;

fn main() {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".knot-data".to_owned());

    let config = rkv::Config::new(PathBuf::from(&path));
    let db = match rkv::DB::open(config) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("ERROR: failed to open database at {path}: {e}");
            std::process::exit(1);
        }
    };

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
