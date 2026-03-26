pub mod parser;
mod text_cmd;

use knot::Knot;
use rkv::DB;

/// REPL state: holds the DB reference and current namespace.
pub struct State<'db> {
    db: &'db DB,
    namespace: Option<String>,
    knot: Option<Knot<'db>>,
}

/// What the REPL should do after executing a command.
pub enum Action {
    Continue,
    Exit,
}

impl<'db> State<'db> {
    pub fn new(db: &'db DB) -> Self {
        Self {
            db,
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

    /// Switch to a namespace, creating a Knot instance.
    pub fn use_namespace(&mut self, ns: &str) -> Result<(), knot::Error> {
        let knot = Knot::new(self.db, ns)?;
        self.namespace = Some(ns.to_owned());
        self.knot = Some(knot);
        Ok(())
    }

    /// Get the current Knot instance, or print an error.
    pub fn knot(&self) -> Option<&Knot<'db>> {
        self.knot.as_ref()
    }

    /// Get a mutable reference to the current Knot instance.
    pub fn knot_mut(&mut self) -> Option<&mut Knot<'db>> {
        self.knot.as_mut()
    }
}

/// Execute a single REPL line.
pub fn execute(state: &mut State<'_>, line: &str) -> Action {
    let trimmed = line.trim();

    // Dispatch based on first character
    match trimmed.chars().next() {
        Some('?') | Some('+') | Some('-') => {
            // Expression command — requires namespace
            if state.knot().is_none() {
                eprintln!("ERROR: no namespace selected. Use: USE <namespace>");
                return Action::Continue;
            }
            execute_expression(state, trimmed);
            Action::Continue
        }
        Some(_) => {
            // Text command
            text_cmd::execute(state, trimmed)
        }
        None => Action::Continue,
    }
}

fn execute_expression(_state: &mut State<'_>, _line: &str) {
    // TODO: implement expression parsing
    eprintln!("ERROR: expression commands not yet implemented");
}
