use rkv::Value;

use super::error::{self, Error, Result};
use super::metadata::{LinkDef, Metadata};
use super::property::{self, Properties};
use super::Knot;

/// Handle to a link table within a Knot namespace.
pub struct Link<'k, 'db> {
    knot: &'k Knot<'db>,
    name: String,
    def: LinkDef,
}

/// A link entry: source key, target key, optional properties.
#[derive(Debug, Clone)]
pub struct LinkEntry {
    pub from: String,
    pub to: String,
    pub properties: Option<Properties>,
}

impl<'k, 'db> Link<'k, 'db> {
    pub(crate) fn new(knot: &'k Knot<'db>, name: &str, def: LinkDef) -> Self {
        Self {
            knot,
            name: name.to_owned(),
            def,
        }
    }

    /// Forward namespace: knot.{ns}.l.{link}
    fn forward_ns(&self) -> String {
        format!("knot.{}.l.{}", self.knot.namespace, self.name)
    }

    /// Reverse namespace: knot.{ns}.r.{link}
    fn reverse_ns(&self) -> String {
        format!("knot.{}.r.{}", self.knot.namespace, self.name)
    }

    /// Composite key: "{from}\x00{to}"
    fn composite_key(from: &str, to: &str) -> String {
        format!("{from}\x00{to}")
    }

    /// Get a link entry. Returns None if not found.
    pub fn get(&self, from: &str, to: &str) -> Result<Option<LinkEntry>> {
        error::validate_key(from)?;
        error::validate_key(to)?;

        let ns = self
            .knot
            .db
            .namespace(&self.forward_ns(), None)
            .map_err(error::storage)?;

        let key = Self::composite_key(from, to);
        match ns.get(key.as_str()) {
            Ok(Value::Data(bytes)) => {
                let props = property::decode_properties(&bytes)?;
                Ok(Some(LinkEntry {
                    from: from.to_owned(),
                    to: to.to_owned(),
                    properties: Some(props),
                }))
            }
            Ok(Value::Null) => Ok(Some(LinkEntry {
                from: from.to_owned(),
                to: to.to_owned(),
                properties: None,
            })),
            Ok(_) => Ok(None),
            Err(rkv::Error::KeyNotFound) => Ok(None),
            Err(e) => Err(error::storage(e)),
        }
    }

    /// Check if a link entry exists.
    #[allow(dead_code)]
    pub fn exists(&self, from: &str, to: &str) -> Result<bool> {
        error::validate_key(from)?;
        error::validate_key(to)?;

        let ns = self
            .knot
            .db
            .namespace(&self.forward_ns(), None)
            .map_err(error::storage)?;

        let key = Self::composite_key(from, to);
        ns.exists(key.as_str()).map_err(error::storage)
    }

    /// Insert a link with properties. Both endpoints must exist. Upsert.
    pub fn insert(&self, from: &str, to: &str, props: &Properties) -> Result<()> {
        error::validate_key(from)?;
        error::validate_key(to)?;
        for prop_name in props.keys() {
            error::validate_name(prop_name)?;
        }

        // Validate endpoints exist
        self.validate_endpoints(from, to)?;

        let fwd_ns = self
            .knot
            .db
            .namespace(&self.forward_ns(), None)
            .map_err(error::storage)?;
        let rev_ns = self
            .knot
            .db
            .namespace(&self.reverse_ns(), None)
            .map_err(error::storage)?;

        let fwd_key = Self::composite_key(from, to);
        let rev_key = Self::composite_key(to, from);

        let bytes = property::encode_properties(props);
        fwd_ns
            .put(fwd_key.as_str(), Value::Data(bytes), None)
            .map_err(error::storage)?;
        rev_ns
            .put(rev_key.as_str(), Value::Null, None)
            .map_err(error::storage)?;

        Ok(())
    }

    /// Insert a bare link (no properties). Both endpoints must exist.
    pub fn insert_bare(&self, from: &str, to: &str) -> Result<()> {
        error::validate_key(from)?;
        error::validate_key(to)?;

        self.validate_endpoints(from, to)?;

        let fwd_ns = self
            .knot
            .db
            .namespace(&self.forward_ns(), None)
            .map_err(error::storage)?;
        let rev_ns = self
            .knot
            .db
            .namespace(&self.reverse_ns(), None)
            .map_err(error::storage)?;

        let fwd_key = Self::composite_key(from, to);
        let rev_key = Self::composite_key(to, from);

        fwd_ns
            .put(fwd_key.as_str(), Value::Null, None)
            .map_err(error::storage)?;
        rev_ns
            .put(rev_key.as_str(), Value::Null, None)
            .map_err(error::storage)?;

        Ok(())
    }

    /// Delete a link entry. No-op if not found.
    pub fn delete(&self, from: &str, to: &str) -> Result<()> {
        error::validate_key(from)?;
        error::validate_key(to)?;

        let fwd_ns = self
            .knot
            .db
            .namespace(&self.forward_ns(), None)
            .map_err(error::storage)?;
        let rev_ns = self
            .knot
            .db
            .namespace(&self.reverse_ns(), None)
            .map_err(error::storage)?;

        let fwd_key = Self::composite_key(from, to);
        let rev_key = Self::composite_key(to, from);

        let _ = fwd_ns.delete(fwd_key.as_str());
        let _ = rev_ns.delete(rev_key.as_str());

        Ok(())
    }

    /// Scan outgoing links from a source key.
    pub fn from(&self, key: &str) -> Result<Vec<LinkEntry>> {
        error::validate_key(key)?;
        self.scan_prefix(&self.forward_ns(), key, true)
    }

    /// Scan incoming links to a target key (reverse lookup).
    pub fn to(&self, key: &str) -> Result<Vec<LinkEntry>> {
        error::validate_key(key)?;
        self.scan_prefix(&self.reverse_ns(), key, false)
    }

    /// Scan a namespace for keys with the given prefix, parse as link entries.
    fn scan_prefix(
        &self,
        ns_name: &str,
        prefix_key: &str,
        forward: bool,
    ) -> Result<Vec<LinkEntry>> {
        let ns = self
            .knot
            .db
            .namespace(ns_name, None)
            .map_err(error::storage)?;

        let scan_prefix = format!("{prefix_key}\x00");
        let keys = ns
            .scan(
                &rkv::Key::Str(scan_prefix),
                0, // no limit
                0, // no offset
                false,
            )
            .map_err(error::storage)?;

        let mut entries = Vec::new();
        for key in keys {
            let key_str = match key.as_str() {
                Some(s) => s,
                None => continue,
            };
            // Split on \x00 to get the two parts
            if let Some(pos) = key_str.find('\x00') {
                let (a, b_with_sep) = key_str.split_at(pos);
                let b = &b_with_sep[1..]; // skip the \x00

                let (from, to) = if forward { (a, b) } else { (b, a) };

                // Read properties from forward namespace
                let fwd_ns = self
                    .knot
                    .db
                    .namespace(&self.forward_ns(), None)
                    .map_err(error::storage)?;
                let fwd_key = Self::composite_key(from, to);
                let properties = match fwd_ns.get(fwd_key.as_str()) {
                    Ok(Value::Data(bytes)) => Some(property::decode_properties(&bytes)?),
                    _ => None,
                };

                entries.push(LinkEntry {
                    from: from.to_owned(),
                    to: to.to_owned(),
                    properties,
                });
            }
        }

        Ok(entries)
    }

    /// Validate that source and target nodes exist.
    fn validate_endpoints(&self, from: &str, to: &str) -> Result<()> {
        let source_ns = format!("knot.{}.t.{}", self.knot.namespace, self.def.source);
        let target_ns = format!("knot.{}.t.{}", self.knot.namespace, self.def.target);

        let src = self
            .knot
            .db
            .namespace(&source_ns, None)
            .map_err(error::storage)?;
        if !src.exists(from).map_err(error::storage)? {
            return Err(Error::EndpointNotFound(format!(
                "{}.{from} (source)",
                self.def.source
            )));
        }

        let tgt = self
            .knot
            .db
            .namespace(&target_ns, None)
            .map_err(error::storage)?;
        if !tgt.exists(to).map_err(error::storage)? {
            return Err(Error::EndpointNotFound(format!(
                "{}.{to} (target)",
                self.def.target
            )));
        }

        Ok(())
    }
}

// Schema operations on Knot for link tables.
impl<'db> Knot<'db> {
    /// Create a link table.
    pub fn create_link(
        &mut self,
        name: &str,
        source: &str,
        target: &str,
        bidirectional: bool,
        cascade: bool,
    ) -> Result<()> {
        error::validate_name(name)?;
        error::validate_name(source)?;
        error::validate_name(target)?;

        if self.meta.links.contains_key(name) {
            return Err(Error::LinkTableExists(name.to_owned()));
        }
        if !self.meta.tables.contains_key(source) {
            return Err(Error::TableNotFound(source.to_owned()));
        }
        if !self.meta.tables.contains_key(target) {
            return Err(Error::TableNotFound(target.to_owned()));
        }

        let def = LinkDef {
            name: name.to_owned(),
            source: source.to_owned(),
            target: target.to_owned(),
            bidirectional,
            cascade,
        };

        // Write metadata
        let meta_ns_name = format!("knot.{}.meta", self.namespace);
        let meta_ns = self
            .db
            .namespace(&meta_ns_name, None)
            .map_err(error::storage)?;
        let meta_key = format!("link:{name}");
        let bytes = Metadata::encode_link_def(&def);
        meta_ns
            .put(meta_key.as_str(), Value::Data(bytes), None)
            .map_err(error::storage)?;

        // Create forward and reverse namespaces
        let fwd = format!("knot.{}.l.{name}", self.namespace);
        let rev = format!("knot.{}.r.{name}", self.namespace);
        let _ = self.db.namespace(&fwd, None).map_err(error::storage)?;
        let _ = self.db.namespace(&rev, None).map_err(error::storage)?;

        self.meta.links.insert(name.to_owned(), def);
        Ok(())
    }

    /// Create a link table if it does not exist.
    pub fn create_link_if_not_exists(
        &mut self,
        name: &str,
        source: &str,
        target: &str,
        bidirectional: bool,
        cascade: bool,
    ) -> Result<()> {
        match self.create_link(name, source, target, bidirectional, cascade) {
            Ok(()) => Ok(()),
            Err(Error::LinkTableExists(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Drop a link table. Removes forward, reverse, and metadata.
    pub fn drop_link(&mut self, name: &str) -> Result<()> {
        error::validate_name(name)?;
        if !self.meta.links.contains_key(name) {
            return Err(Error::LinkTableNotFound(name.to_owned()));
        }

        let fwd = format!("knot.{}.l.{name}", self.namespace);
        let rev = format!("knot.{}.r.{name}", self.namespace);
        let _ = self.db.drop_namespace(&fwd);
        let _ = self.db.drop_namespace(&rev);

        let meta_ns_name = format!("knot.{}.meta", self.namespace);
        let meta_ns = self
            .db
            .namespace(&meta_ns_name, None)
            .map_err(error::storage)?;
        let meta_key = format!("link:{name}");
        let _ = meta_ns.delete(meta_key.as_str());

        self.meta.links.remove(name);
        Ok(())
    }

    /// List all link table names.
    pub fn links(&self) -> Vec<String> {
        self.meta.links.keys().cloned().collect()
    }

    /// Get a link table handle.
    pub fn link(&self, name: &str) -> Result<Link<'_, 'db>> {
        let def = self
            .meta
            .links
            .get(name)
            .ok_or_else(|| Error::LinkTableNotFound(name.to_owned()))?;
        Ok(Link::new(self, name, def.clone()))
    }

    /// Alter a link table's bidirectional or cascade flags.
    pub fn alter_link(
        &mut self,
        name: &str,
        bidirectional: Option<bool>,
        cascade: Option<bool>,
    ) -> Result<()> {
        error::validate_name(name)?;
        let def = self
            .meta
            .links
            .get_mut(name)
            .ok_or_else(|| Error::LinkTableNotFound(name.to_owned()))?;

        if let Some(b) = bidirectional {
            def.bidirectional = b;
        }
        if let Some(c) = cascade {
            def.cascade = c;
        }

        // Update metadata
        let meta_ns_name = format!("knot.{}.meta", self.namespace);
        let meta_ns = self
            .db
            .namespace(&meta_ns_name, None)
            .map_err(error::storage)?;
        let meta_key = format!("link:{name}");
        let bytes = Metadata::encode_link_def(def);
        meta_ns
            .put(meta_key.as_str(), Value::Data(bytes), None)
            .map_err(error::storage)?;

        Ok(())
    }
}
