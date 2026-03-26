use std::collections::HashSet;

use super::error::Result;
use super::Knot;

/// Delete a node and clean up all links to/from it.
pub fn delete_node(knot: &Knot, table_name: &str, key: &str, cascade: bool) -> Result<()> {
    let mut visited = HashSet::new();
    delete_node_inner(knot, table_name, key, cascade, &mut visited)
}

fn delete_node_inner(
    knot: &Knot,
    table_name: &str,
    key: &str,
    cascade: bool,
    visited: &mut HashSet<(String, String)>,
) -> Result<()> {
    let node_id = (table_name.to_owned(), key.to_owned());
    if visited.contains(&node_id) {
        return Ok(());
    }
    visited.insert(node_id);

    let links_as_source: Vec<_> = knot
        .meta
        .links
        .values()
        .filter(|l| l.source == table_name)
        .cloned()
        .collect();

    let links_as_target: Vec<_> = knot
        .meta
        .links
        .values()
        .filter(|l| l.target == table_name)
        .cloned()
        .collect();

    for link_def in &links_as_source {
        let link = knot.link(&link_def.name)?;
        let outgoing = link.from(key)?;
        for entry in &outgoing {
            link.delete(&entry.from, &entry.to)?;
            if cascade || link_def.cascade {
                delete_node_inner(knot, &link_def.target, &entry.to, cascade, visited)?;
            }
        }
    }

    for link_def in &links_as_target {
        let link = knot.link(&link_def.name)?;
        let incoming = link.to(key)?;
        for entry in &incoming {
            link.delete(&entry.from, &entry.to)?;
            if cascade || link_def.cascade {
                delete_node_inner(knot, &link_def.source, &entry.from, cascade, visited)?;
            }
        }
    }

    let ns_name = format!("knot.{}.t.{table_name}", knot.namespace);
    knot.backend.delete(&ns_name, key)?;
    Ok(())
}
