use knot::{Sort, SortOrder};

use super::parser::{Expr, SortSpec};
use super::State;

/// Execute a parsed expression against the Knot engine.
pub fn execute(state: &mut State<'_>, expr: Expr) {
    match expr {
        Expr::InsertNode {
            table,
            key,
            properties,
        } => exec_insert_node(state, &table, &key, properties),
        Expr::InsertLink {
            link,
            from,
            to,
            bidi: _,
            properties,
        } => exec_insert_link(state, &link, &from, &to, properties),
        Expr::DeleteNode { table, key } => exec_delete_node(state, &table, &key),
        Expr::CascadeDeleteNode { table, key } => {
            exec_cascade_delete_node(state, &table, &key);
        }
        Expr::DeleteLink { link, from, to } => exec_delete_link(state, &link, &from, &to),
        Expr::CascadeDeleteLink { link, from, to } => {
            exec_cascade_delete_link(state, &link, &from, &to);
        }
        Expr::GetNode { table, key } => exec_get_node(state, &table, &key),
        Expr::QueryNodes {
            table,
            filter,
            sort,
            limit,
            offset: _,
        } => exec_query_nodes(state, &table, filter.as_ref(), sort.as_ref(), limit),
        Expr::Traverse { table, key, hops } => exec_traverse(state, &table, &key, &hops),
        Expr::Discover {
            table,
            key,
            max_hops,
            bidi,
        } => exec_discover(state, &table, &key, max_hops, bidi),
    }
}

fn exec_insert_node(
    state: &State<'_>,
    table: &str,
    key: &str,
    properties: Option<knot::Properties>,
) {
    let knot = state.knot().unwrap();
    let tbl = match knot.table(table) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("ERROR: {e}");
            return;
        }
    };
    let result = match properties {
        Some(props) => tbl.insert(key, &props),
        None => tbl.insert_set(key),
    };
    match result {
        Ok(()) => println!("OK"),
        Err(e) => eprintln!("ERROR: {e}"),
    }
}

fn exec_insert_link(
    state: &State<'_>,
    link: &str,
    from: &str,
    to: &str,
    properties: Option<knot::Properties>,
) {
    let knot = state.knot().unwrap();
    let lnk = match knot.link(link) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("ERROR: {e}");
            return;
        }
    };
    let result = match properties {
        Some(props) => lnk.insert(from, to, &props),
        None => lnk.insert_bare(from, to),
    };
    match result {
        Ok(()) => println!("OK"),
        Err(e) => eprintln!("ERROR: {e}"),
    }
}

fn exec_delete_node(state: &State<'_>, table: &str, key: &str) {
    let knot = state.knot().unwrap();
    let tbl = match knot.table(table) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("ERROR: {e}");
            return;
        }
    };
    match tbl.delete(key) {
        Ok(()) => println!("OK"),
        Err(e) => eprintln!("ERROR: {e}"),
    }
}

fn exec_cascade_delete_node(state: &State<'_>, table: &str, key: &str) {
    let knot = state.knot().unwrap();
    let tbl = match knot.table(table) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("ERROR: {e}");
            return;
        }
    };
    match tbl.delete_cascade(key, true) {
        Ok(()) => println!("OK"),
        Err(e) => eprintln!("ERROR: {e}"),
    }
}

fn exec_delete_link(state: &State<'_>, link: &str, from: &str, to: &str) {
    let knot = state.knot().unwrap();
    let lnk = match knot.link(link) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("ERROR: {e}");
            return;
        }
    };
    match lnk.delete(from, to) {
        Ok(()) => println!("OK"),
        Err(e) => eprintln!("ERROR: {e}"),
    }
}

fn exec_cascade_delete_link(state: &State<'_>, link: &str, from: &str, to: &str) {
    // For now, cascade delete on link just deletes the link
    // Full cascade to target node would need more work
    exec_delete_link(state, link, from, to);
}

fn exec_get_node(state: &State<'_>, table: &str, key: &str) {
    let knot = state.knot().unwrap();
    let tbl = match knot.table(table) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("ERROR: {e}");
            return;
        }
    };
    match tbl.get(key) {
        Ok(Some(node)) => {
            println!("{}", node.key);
            if let Some(props) = &node.properties {
                for (k, v) in props {
                    println!("  {k} = {}", format_value(v));
                }
            }
        }
        Ok(None) => eprintln!("(not found)"),
        Err(e) => eprintln!("ERROR: {e}"),
    }
}

fn exec_query_nodes(
    state: &State<'_>,
    table: &str,
    filter: Option<&knot::Condition>,
    sort: Option<&SortSpec>,
    limit: Option<usize>,
) {
    let knot = state.knot().unwrap();
    let tbl = match knot.table(table) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("ERROR: {e}");
            return;
        }
    };

    let knot_sort = sort.map(|s| Sort {
        field: s.field.clone(),
        order: if s.desc {
            SortOrder::Desc
        } else {
            SortOrder::Asc
        },
    });

    let lim = limit.unwrap_or(100);

    match tbl.query(filter, knot_sort.as_ref(), None, lim, None) {
        Ok(page) => {
            for node in &page.items {
                if let Some(props) = &node.properties {
                    let formatted: Vec<String> = props
                        .iter()
                        .map(|(k, v)| format!("{k}: {}", format_value(v)))
                        .collect();
                    println!("{}  {{{}}}", node.key, formatted.join(", "));
                } else {
                    println!("{}", node.key);
                }
            }
            let count = page.items.len();
            if page.has_more {
                println!("({count} nodes, more available)");
            } else {
                println!("({count} nodes)");
            }
        }
        Err(e) => eprintln!("ERROR: {e}"),
    }
}

fn exec_traverse(state: &State<'_>, table: &str, key: &str, hops: &[super::parser::TraversalHop]) {
    let knot = state.knot().unwrap();
    let link_names: Vec<&str> = hops.iter().map(|h| h.link.as_str()).collect();

    match knot.traverse(table, key, &link_names, None, None, false) {
        Ok(result) => {
            for (tbl, k) in &result.leaves {
                println!("{tbl}.{k}");
            }
            println!("({} results)", result.leaves.len());
        }
        Err(e) => eprintln!("ERROR: {e}"),
    }
}

fn exec_discover(state: &State<'_>, table: &str, key: &str, max_hops: usize, bidi: bool) {
    let knot = state.knot().unwrap();

    match knot.discover(table, key, max_hops, bidi) {
        Ok(result) => {
            for (tbl, k) in &result.leaves {
                println!("{tbl}.{k}");
            }
            println!("({} results)", result.leaves.len());
        }
        Err(e) => eprintln!("ERROR: {e}"),
    }
}

fn format_value(v: &knot::PropertyValue) -> String {
    match v {
        knot::PropertyValue::String(s) => format!("\"{s}\""),
        knot::PropertyValue::Integer(n) => n.to_string(),
        knot::PropertyValue::Float(f) => f.to_string(),
        knot::PropertyValue::Boolean(b) => b.to_string(),
        knot::PropertyValue::Binary(b) => format!("<{} bytes>", b.len()),
        knot::PropertyValue::Geo(lat, lon) => format!("({lat}, {lon})"),
    }
}
