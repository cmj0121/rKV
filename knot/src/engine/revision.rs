use rkv::RevisionID;

use super::error::Result;
use super::property::{self, Properties};
use super::Knot;

/// A single revision of a node or link.
#[derive(Debug, Clone)]
pub struct Revision {
    pub id: RevisionID,
    pub timestamp_ms: u64,
    pub properties: Option<Properties>,
}

pub fn node_history(knot: &Knot, table_name: &str, key: &str) -> Result<Vec<Revision>> {
    let ns_name = format!("knot.{}.t.{table_name}", knot.namespace);
    let count = knot.backend.rev_count(&ns_name, key)?;
    let mut revisions = Vec::with_capacity(count as usize);

    for i in 0..count {
        let value = match knot.backend.rev_get(&ns_name, key, i)? {
            Some(v) => v,
            None => continue,
        };
        let rev_id = knot.backend.get_revision_id(&ns_name, key)?;
        let props = match &value {
            rkv::Value::Data(bytes) => Some(property::decode_properties(bytes)?),
            _ => None,
        };
        revisions.push(Revision {
            id: rev_id,
            timestamp_ms: rev_id.timestamp_ms(),
            properties: props,
        });
    }
    Ok(revisions)
}

pub fn node_rev_count(knot: &Knot, table_name: &str, key: &str) -> Result<u64> {
    let ns_name = format!("knot.{}.t.{table_name}", knot.namespace);
    knot.backend.rev_count(&ns_name, key)
}

pub fn node_at_revision(
    knot: &Knot,
    table_name: &str,
    key: &str,
    index: u64,
) -> Result<Option<Revision>> {
    let ns_name = format!("knot.{}.t.{table_name}", knot.namespace);
    let count = knot.backend.rev_count(&ns_name, key)?;
    if index >= count {
        return Ok(None);
    }
    let value = match knot.backend.rev_get(&ns_name, key, index)? {
        Some(v) => v,
        None => return Ok(None),
    };
    let rev_id = knot.backend.get_revision_id(&ns_name, key)?;
    let props = match &value {
        rkv::Value::Data(bytes) => Some(property::decode_properties(bytes)?),
        _ => None,
    };
    Ok(Some(Revision {
        id: rev_id,
        timestamp_ms: rev_id.timestamp_ms(),
        properties: props,
    }))
}

impl<'k> super::table::Table<'k> {
    pub fn history(&self, key: &str) -> Result<Vec<Revision>> {
        super::error::validate_key(key)?;
        node_history(self.knot(), self.name(), key)
    }

    pub fn rev_count(&self, key: &str) -> Result<u64> {
        super::error::validate_key(key)?;
        node_rev_count(self.knot(), self.name(), key)
    }

    pub fn at_revision(&self, key: &str, index: u64) -> Result<Option<Revision>> {
        super::error::validate_key(key)?;
        node_at_revision(self.knot(), self.name(), key, index)
    }
}
