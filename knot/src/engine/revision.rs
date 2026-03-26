use rkv::RevisionID;

use super::error::{self, Result};
use super::property::{self, Properties};
use super::Knot;

/// A single revision of a node or link.
#[derive(Debug, Clone)]
pub struct Revision {
    /// Revision ID (ULID-like, contains timestamp).
    pub id: RevisionID,
    /// Timestamp in milliseconds since epoch.
    pub timestamp_ms: u64,
    /// Properties at this revision, or None for set-mode/bare entries.
    pub properties: Option<Properties>,
}

/// Get revision history for a node.
pub fn node_history(knot: &Knot<'_>, table_name: &str, key: &str) -> Result<Vec<Revision>> {
    let ns_name = format!("knot.{}.t.{table_name}", knot.namespace);
    let ns = knot.db.namespace(&ns_name, None).map_err(error::storage)?;

    let count = ns.rev_count(key).map_err(error::storage)?;
    let mut revisions = Vec::with_capacity(count as usize);

    for i in 0..count {
        let value = ns.rev_get(key, i).map_err(error::storage)?;
        let (rev_id, props) = parse_revision_value(&ns, key, i, &value)?;
        revisions.push(Revision {
            id: rev_id,
            timestamp_ms: rev_id.timestamp_ms(),
            properties: props,
        });
    }

    Ok(revisions)
}

/// Get the number of revisions for a node. Returns 0 if key doesn't exist.
pub fn node_rev_count(knot: &Knot<'_>, table_name: &str, key: &str) -> Result<u64> {
    let ns_name = format!("knot.{}.t.{table_name}", knot.namespace);
    let ns = knot.db.namespace(&ns_name, None).map_err(error::storage)?;
    match ns.rev_count(key) {
        Ok(count) => Ok(count),
        Err(rkv::Error::KeyNotFound) => Ok(0),
        Err(e) => Err(error::storage(e)),
    }
}

/// Get a node at a specific revision index.
pub fn node_at_revision(
    knot: &Knot<'_>,
    table_name: &str,
    key: &str,
    index: u64,
) -> Result<Option<Revision>> {
    let ns_name = format!("knot.{}.t.{table_name}", knot.namespace);
    let ns = knot.db.namespace(&ns_name, None).map_err(error::storage)?;

    let count = ns.rev_count(key).map_err(error::storage)?;
    if index >= count {
        return Ok(None);
    }

    let value = ns.rev_get(key, index).map_err(error::storage)?;
    let (rev_id, props) = parse_revision_value(&ns, key, index, &value)?;
    Ok(Some(Revision {
        id: rev_id,
        timestamp_ms: rev_id.timestamp_ms(),
        properties: props,
    }))
}

fn parse_revision_value(
    ns: &rkv::Namespace<'_>,
    key: &str,
    _index: u64,
    value: &rkv::Value,
) -> Result<(RevisionID, Option<Properties>)> {
    // Get the revision ID from the current state (best effort)
    let rev_id = match ns.get_with_revision(key) {
        Ok((_, rev)) => rev,
        Err(_) => RevisionID::ZERO,
    };

    let props = match value {
        rkv::Value::Data(bytes) => Some(property::decode_properties(bytes)?),
        rkv::Value::Null => None,
        _ => None,
    };

    Ok((rev_id, props))
}

// Wire into Table handle
impl<'k, 'db> super::table::Table<'k, 'db> {
    /// Get revision history for a node.
    pub fn history(&self, key: &str) -> Result<Vec<Revision>> {
        super::error::validate_key(key)?;
        node_history(self.knot(), self.name(), key)
    }

    /// Get the number of revisions for a node.
    pub fn rev_count(&self, key: &str) -> Result<u64> {
        super::error::validate_key(key)?;
        node_rev_count(self.knot(), self.name(), key)
    }

    /// Get a node at a specific revision index.
    pub fn at_revision(&self, key: &str, index: u64) -> Result<Option<Revision>> {
        super::error::validate_key(key)?;
        node_at_revision(self.knot(), self.name(), key, index)
    }
}
