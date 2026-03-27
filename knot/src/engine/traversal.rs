use std::collections::HashSet;

use super::condition::{self, Condition};
use super::error::{self, Result};
use super::link::LinkEntry;
use super::Knot;

/// Traversal result.
#[derive(Debug)]
pub struct TraversalResult {
    /// Leaf node keys (table, key) reached by traversal.
    pub leaves: Vec<(String, String)>,
    /// Full paths if requested. Each path is a list of (table, key) pairs.
    pub paths: Option<Vec<Vec<(String, String)>>>,
}

/// Directed traversal: follow a specified sequence of link tables.
pub fn directed(
    knot: &Knot,
    start_table: &str,
    start_key: &str,
    link_names: &[&str],
    link_filter: Option<&Condition>,
    node_filter: Option<&Condition>,
    with_paths: bool,
) -> Result<TraversalResult> {
    error::validate_key(start_key)?;

    let mut current: Vec<(String, String)> = vec![(start_table.to_owned(), start_key.to_owned())];
    let mut visited: HashSet<(String, String)> = HashSet::new();
    visited.insert((start_table.to_owned(), start_key.to_owned()));

    // Track paths: path_map[node_id] = path from start to this node
    let mut path_map: std::collections::HashMap<(String, String), Vec<(String, String)>> =
        std::collections::HashMap::new();
    if with_paths {
        path_map.insert(
            (start_table.to_owned(), start_key.to_owned()),
            vec![(start_table.to_owned(), start_key.to_owned())],
        );
    }

    for link_name in link_names {
        let link_def = knot
            .meta
            .links
            .get(*link_name)
            .ok_or_else(|| error::Error::LinkTableNotFound(link_name.to_string()))?;

        let link = knot.link(link_name)?;
        let mut next = Vec::new();

        for (_, key) in &current {
            // Forward direction
            let entries = link.from(key)?;
            let filtered =
                filter_entries(&entries, link_filter, knot, &link_def.target, node_filter)?;

            for entry in &filtered {
                let node_id = (link_def.target.clone(), entry.to.clone());
                if visited.contains(&node_id) {
                    continue;
                }
                visited.insert(node_id.clone());

                if with_paths {
                    let prev_path = path_map
                        .get(&(link_def.source.clone(), key.clone()))
                        .cloned()
                        .unwrap_or_default();
                    let mut new_path = prev_path;
                    new_path.push(node_id.clone());
                    path_map.insert(node_id.clone(), new_path);
                }

                next.push(node_id);
            }

            // Bidirectional: also check reverse direction
            if link_def.bidirectional {
                let rev_entries = link.to(key)?;
                let filtered = filter_entries(
                    &rev_entries,
                    link_filter,
                    knot,
                    &link_def.source,
                    node_filter,
                )?;

                for entry in &filtered {
                    let node_id = (link_def.source.clone(), entry.from.clone());
                    if visited.contains(&node_id) {
                        continue;
                    }
                    visited.insert(node_id.clone());

                    if with_paths {
                        let prev_path = path_map
                            .get(&(link_def.target.clone(), key.clone()))
                            .cloned()
                            .unwrap_or_default();
                        let mut new_path = prev_path;
                        new_path.push(node_id.clone());
                        path_map.insert(node_id.clone(), new_path);
                    }

                    next.push(node_id);
                }
            }
        }

        current = next;
    }

    let paths = if with_paths {
        Some(
            current
                .iter()
                .map(|id| path_map.get(id).cloned().unwrap_or_default())
                .collect(),
        )
    } else {
        None
    };

    Ok(TraversalResult {
        leaves: current,
        paths,
    })
}

/// Discovery traversal: follow all applicable links up to max_hops.
pub fn discovery(
    knot: &Knot,
    start_table: &str,
    start_key: &str,
    max_hops: usize,
    bidi: bool,
) -> Result<TraversalResult> {
    error::validate_key(start_key)?;

    let mut visited: HashSet<(String, String)> = HashSet::new();
    visited.insert((start_table.to_owned(), start_key.to_owned()));

    let mut current: Vec<(String, String)> = vec![(start_table.to_owned(), start_key.to_owned())];
    let mut all_leaves: Vec<(String, String)> = Vec::new();

    for _ in 0..max_hops {
        let mut next = Vec::new();

        for (table, key) in &current {
            // Find all link tables where this table is the source
            let source_links: Vec<_> = knot
                .meta
                .links
                .values()
                .filter(|l| l.source == *table)
                .collect();

            for link_def in &source_links {
                let link = knot.link(&link_def.name)?;
                let entries = link.from(key)?;
                for entry in &entries {
                    let node_id = (link_def.target.clone(), entry.to.clone());
                    if !visited.contains(&node_id) {
                        visited.insert(node_id.clone());
                        next.push(node_id);
                    }
                }
            }

            // If bidi, also follow links where this table is the target
            if bidi {
                let target_links: Vec<_> = knot
                    .meta
                    .links
                    .values()
                    .filter(|l| l.target == *table && l.bidirectional)
                    .collect();

                for link_def in &target_links {
                    let link = knot.link(&link_def.name)?;
                    let entries = link.to(key)?;
                    for entry in &entries {
                        let node_id = (link_def.source.clone(), entry.from.clone());
                        if !visited.contains(&node_id) {
                            visited.insert(node_id.clone());
                            next.push(node_id);
                        }
                    }
                }
            }
        }

        if next.is_empty() {
            break;
        }

        all_leaves.extend(next.clone());
        current = next;
    }

    Ok(TraversalResult {
        leaves: all_leaves,
        paths: None,
    })
}

/// Filter link entries by link property condition and destination node condition.
fn filter_entries(
    entries: &[LinkEntry],
    link_filter: Option<&Condition>,
    knot: &Knot,
    target_table: &str,
    node_filter: Option<&Condition>,
) -> Result<Vec<LinkEntry>> {
    let empty_props = std::collections::HashMap::new();
    let mut result = Vec::new();
    for entry in entries {
        // Link property filter
        if let Some(cond) = link_filter {
            let props = entry.properties.as_ref().map_or(&empty_props, |p| p);
            if !condition::evaluate(cond, props) {
                continue;
            }
        }

        // Node property filter
        if let Some(cond) = node_filter {
            let ns_name = format!("knot.{}.t.{target_table}", knot.namespace);
            match knot.backend.get(&ns_name, &entry.to)? {
                Some(rkv::Value::Data(bytes)) => {
                    let props = super::property::decode_properties(&bytes)?;
                    if !condition::evaluate(cond, &props) {
                        continue;
                    }
                }
                _ => continue,
            }
        }

        result.push(entry.clone());
    }
    Ok(result)
}
