use std::collections::HashSet;

use super::error::{self, Result};
use super::Knot;

/// Delete a node and clean up all links to/from it.
/// If cascade is true (or the link table has cascade=true), recursively
/// delete connected nodes.
pub fn delete_node(knot: &Knot<'_>, table_name: &str, key: &str, cascade: bool) -> Result<()> {
    let mut visited = HashSet::new();
    delete_node_inner(knot, table_name, key, cascade, &mut visited)
}

fn delete_node_inner(
    knot: &Knot<'_>,
    table_name: &str,
    key: &str,
    cascade: bool,
    visited: &mut HashSet<(String, String)>,
) -> Result<()> {
    let node_id = (table_name.to_owned(), key.to_owned());
    if visited.contains(&node_id) {
        return Ok(()); // cycle detection
    }
    visited.insert(node_id);

    // Find all link tables involving this table
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

    // Remove outgoing links and optionally cascade to targets
    for link_def in &links_as_source {
        let link = knot.link(&link_def.name)?;
        let outgoing = link.from(key)?;
        for entry in &outgoing {
            link.delete(&entry.from, &entry.to)?;

            // Cascade if requested or if schema-level cascade
            if cascade || link_def.cascade {
                delete_node_inner(knot, &link_def.target, &entry.to, cascade, visited)?;
            }
        }
    }

    // Remove incoming links and optionally cascade to sources
    for link_def in &links_as_target {
        let link = knot.link(&link_def.name)?;
        let incoming = link.to(key)?;
        for entry in &incoming {
            link.delete(&entry.from, &entry.to)?;

            // Cascade if requested or if schema-level cascade
            if cascade || link_def.cascade {
                delete_node_inner(knot, &link_def.source, &entry.from, cascade, visited)?;
            }
        }
    }

    // Delete the node itself
    let ns_name = format!("knot.{}.t.{table_name}", knot.namespace);
    let ns = knot.db.namespace(&ns_name, None).map_err(error::storage)?;
    let _ = ns.delete(key);

    Ok(())
}
