mod executor;
pub mod parser;
mod text_cmd;

use std::sync::Arc;

use knot::engine::backend::Backend;
use knot::Knot;

/// REPL state: holds the backend and current namespace.
pub struct State {
    backend: Arc<dyn Backend>,
    namespace: Option<String>,
    knot: Option<Knot>,
}

/// What the REPL should do after executing a command.
pub enum Action {
    Continue,
    Exit,
}

impl State {
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self {
            backend,
            namespace: None,
            knot: None,
        }
    }

    pub fn prompt(&self) -> String {
        match &self.namespace {
            Some(ns) => format!("knot [{ns}]> "),
            None => "knot> ".to_owned(),
        }
    }

    pub fn use_namespace(&mut self, ns: &str) -> Result<(), knot::Error> {
        let knot = Knot::open(self.backend.clone(), ns)?;
        self.namespace = Some(ns.to_owned());
        self.knot = Some(knot);
        Ok(())
    }

    pub fn knot(&self) -> Option<&Knot> {
        self.knot.as_ref()
    }

    pub fn knot_mut(&mut self) -> Option<&mut Knot> {
        self.knot.as_mut()
    }
}

/// Execute a single REPL line.
pub fn execute(state: &mut State, line: &str) -> Action {
    let trimmed = line.trim();

    match trimmed.chars().next() {
        Some('?') | Some('+') | Some('-') => {
            if state.knot().is_none() {
                eprintln!("ERROR: no namespace selected. Use: USE <namespace>");
                return Action::Continue;
            }
            execute_expression(state, trimmed);
            Action::Continue
        }
        Some(_) => text_cmd::execute(state, trimmed),
        None => Action::Continue,
    }
}

fn execute_expression(state: &mut State, line: &str) {
    match parser::parse(line) {
        Ok(expr) => executor::execute(state, expr),
        Err(e) => eprintln!("ERROR: {e}"),
    }
}
