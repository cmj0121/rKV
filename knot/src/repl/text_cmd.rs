use super::{Action, State};

/// Execute a text command (schema/navigation).
pub fn execute(state: &mut State, line: &str) -> Action {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    if tokens.is_empty() {
        return Action::Continue;
    }

    let cmd = tokens[0].to_uppercase();
    match cmd.as_str() {
        "EXIT" | "QUIT" => Action::Exit,
        "HELP" | "?" => {
            print_help();
            Action::Continue
        }
        "USE" => {
            cmd_use(state, &tokens);
            Action::Continue
        }
        "NAMESPACES" => {
            cmd_namespaces(state);
            Action::Continue
        }
        "CREATE" => {
            cmd_create(state, &tokens);
            Action::Continue
        }
        "DROP" => {
            cmd_drop(state, &tokens);
            Action::Continue
        }
        "ALTER" => {
            cmd_alter(state, &tokens);
            Action::Continue
        }
        "TABLES" => {
            cmd_tables(state);
            Action::Continue
        }
        "LINKS" => {
            cmd_links(state);
            Action::Continue
        }
        _ => {
            eprintln!("ERROR: unknown command: {}", tokens[0]);
            Action::Continue
        }
    }
}

fn print_help() {
    println!(
        r#"
  Query:    ?{{table | condition}}          ?{{person | age>30}}
  Get:      ?{{table key}}                  ?{{person alice}}
  Limit:    ?:N{{...}}                      ?:10{{person | role=teacher}}
  Insert:   +{{table key}}[props]           +{{person alice}}[role=teacher]
  Delete:   -{{table key}}                  -{{person alice}}
  Cascade:  -!{{table key}}                 -!{{person alice}}
  Link:     +(link from -> to)[props]     +(attends alice -> mit)[year=2020]
  Unlink:   -(link from -> to)            -(attends alice -> mit)
  BIDI:     <-> instead of ->             +(friends alice <-> bob)
  Traverse: ?{{source}} -> (link) -> ...    ?{{person alice}} -> (attends)
  Discover: ?{{source}} -> (*:N)            ?{{person alice}} -> (*:3)
  Sort:     [field:asc|desc, ...]         ?{{person}}[age:asc]
  Schema:   CREATE|DROP|ALTER ...          CREATE TABLE person
  Navigate: USE namespace                  USE campus
  List:     TABLES | LINKS | NAMESPACES
  Exit:     exit | quit
"#
    );
}

fn require_namespace(state: &State) -> Option<&knot::Knot> {
    match state.knot() {
        Some(k) => Some(k),
        None => {
            eprintln!("ERROR: no namespace selected. Use: USE <namespace>");
            None
        }
    }
}

fn cmd_use(state: &mut State, tokens: &[&str]) {
    if tokens.len() < 2 {
        eprintln!("Usage: USE <namespace>");
        return;
    }
    match state.use_namespace(tokens[1]) {
        Ok(()) => println!("OK"),
        Err(e) => eprintln!("ERROR: {e}"),
    }
}

fn cmd_namespaces(_state: &State) {
    // rKV doesn't have a "list namespaces" API that filters knot namespaces.
    // For now, print a message.
    eprintln!("ERROR: NAMESPACES not yet implemented");
}

fn cmd_create(state: &mut State, tokens: &[&str]) {
    if tokens.len() < 3 {
        eprintln!("Usage: CREATE TABLE|LINK|NAMESPACE <name> ...");
        return;
    }

    let kind = tokens[1].to_uppercase();
    match kind.as_str() {
        "TABLE" => cmd_create_table(state, tokens),
        "LINK" => cmd_create_link(state, tokens),
        "NAMESPACE" => cmd_create_namespace(state, tokens),
        _ => eprintln!("ERROR: unknown CREATE target: {}", tokens[1]),
    }
}

fn cmd_create_table(state: &mut State, tokens: &[&str]) {
    let knot = match state.knot_mut() {
        Some(k) => k,
        None => {
            eprintln!("ERROR: no namespace selected. Use: USE <namespace>");
            return;
        }
    };

    let name = tokens[2];
    let if_not_exists = tokens.len() > 5
        && tokens[3].eq_ignore_ascii_case("IF")
        && tokens[4].eq_ignore_ascii_case("NOT")
        && tokens[5].eq_ignore_ascii_case("EXISTS");

    let result = if if_not_exists {
        knot.create_table_if_not_exists(name)
    } else {
        knot.create_table(name)
    };

    match result {
        Ok(()) => println!("OK"),
        Err(e) => eprintln!("ERROR: {e}"),
    }
}

fn cmd_create_link(state: &mut State, tokens: &[&str]) {
    // CREATE LINK name source -> target [BIDI] [CASCADE]
    if tokens.len() < 6 {
        eprintln!("Usage: CREATE LINK <name> <source> -> <target> [BIDI] [CASCADE]");
        return;
    }

    let knot = match state.knot_mut() {
        Some(k) => k,
        None => {
            eprintln!("ERROR: no namespace selected. Use: USE <namespace>");
            return;
        }
    };

    let name = tokens[2];
    let source = tokens[3];
    // tokens[4] should be "->"
    if tokens[4] != "->" {
        eprintln!("Usage: CREATE LINK <name> <source> -> <target> [BIDI] [CASCADE]");
        return;
    }
    let target = tokens[5];

    let mut bidi = false;
    let mut cascade = false;
    for t in &tokens[6..] {
        match t.to_uppercase().as_str() {
            "BIDI" => bidi = true,
            "CASCADE" => cascade = true,
            _ => {
                eprintln!("ERROR: unknown flag: {t}");
                return;
            }
        }
    }

    match knot.create_link(name, source, target, bidi, cascade) {
        Ok(()) => println!("OK"),
        Err(e) => eprintln!("ERROR: {e}"),
    }
}

fn cmd_create_namespace(state: &mut State, tokens: &[&str]) {
    let name = tokens[2];
    match state.use_namespace(name) {
        Ok(()) => println!("OK"),
        Err(e) => eprintln!("ERROR: {e}"),
    }
}

fn cmd_drop(state: &mut State, tokens: &[&str]) {
    if tokens.len() < 3 {
        eprintln!("Usage: DROP TABLE|LINK|NAMESPACE <name>");
        return;
    }

    let kind = tokens[1].to_uppercase();
    match kind.as_str() {
        "TABLE" => {
            let knot = match state.knot_mut() {
                Some(k) => k,
                None => {
                    eprintln!("ERROR: no namespace selected");
                    return;
                }
            };
            match knot.drop_table(tokens[2]) {
                Ok(()) => println!("OK"),
                Err(e) => eprintln!("ERROR: {e}"),
            }
        }
        "LINK" => {
            let knot = match state.knot_mut() {
                Some(k) => k,
                None => {
                    eprintln!("ERROR: no namespace selected");
                    return;
                }
            };
            match knot.drop_link(tokens[2]) {
                Ok(()) => println!("OK"),
                Err(e) => eprintln!("ERROR: {e}"),
            }
        }
        "NAMESPACE" => {
            eprintln!("ERROR: DROP NAMESPACE not yet implemented");
        }
        _ => eprintln!("ERROR: unknown DROP target: {}", tokens[1]),
    }
}

fn cmd_alter(state: &mut State, tokens: &[&str]) {
    // ALTER LINK name CASCADE|BIDI
    if tokens.len() < 4 {
        eprintln!("Usage: ALTER LINK <name> CASCADE|BIDI");
        return;
    }

    if !tokens[1].eq_ignore_ascii_case("LINK") {
        eprintln!("ERROR: only ALTER LINK is supported");
        return;
    }

    let knot = match state.knot_mut() {
        Some(k) => k,
        None => {
            eprintln!("ERROR: no namespace selected");
            return;
        }
    };

    let name = tokens[2];
    let flag = tokens[3].to_uppercase();

    let (bidi, cascade) = match flag.as_str() {
        "BIDI" => (Some(true), None),
        "CASCADE" => (None, Some(true)),
        _ => {
            eprintln!("ERROR: unknown ALTER flag: {}", tokens[3]);
            return;
        }
    };

    match knot.alter_link(name, bidi, cascade) {
        Ok(()) => println!("OK"),
        Err(e) => eprintln!("ERROR: {e}"),
    }
}

fn cmd_tables(state: &State) {
    let knot = match require_namespace(state) {
        Some(k) => k,
        None => return,
    };
    let mut tables = knot.tables();
    tables.sort();
    for t in &tables {
        println!("{t}");
    }
    println!("({} tables)", tables.len());
}

fn cmd_links(state: &State) {
    let knot = match require_namespace(state) {
        Some(k) => k,
        None => return,
    };
    let mut links = knot.links();
    links.sort();
    for l in &links {
        println!("{l}");
    }
    println!("({} links)", links.len());
}
