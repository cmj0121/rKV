use rkv::Value;

use super::error::{self, Error, Result};
use super::metadata::{LinkDef, Metadata};
use super::property::{self, Properties};
use super::Knot;

/// Handle to a link table within a Knot namespace.
pub struct Link<'k> {
    knot: &'k Knot,
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

impl<'k> Link<'k> {
    pub(crate) fn new(knot: &'k Knot, name: &str, def: LinkDef) -> Self {
        Self {
            knot,
            name: name.to_owned(),
            def,
        }
    }

    fn forward_ns(&self) -> String {
        format!("knot.{}.l.{}", self.knot.namespace, self.name)
    }

    fn reverse_ns(&self) -> String {
        format!("knot.{}.r.{}", self.knot.namespace, self.name)
    }

    fn composite_key(from: &str, to: &str) -> String {
        format!("{from}\x00{to}")
    }

    pub fn get(&self, from: &str, to: &str) -> Result<Option<LinkEntry>> {
        error::validate_key(from)?;
        error::validate_key(to)?;
        let key = Self::composite_key(from, to);
        match self.knot.backend.get(&self.forward_ns(), &key)? {
            Some(Value::Data(bytes)) => {
                let props = property::decode_properties(&bytes)?;
                Ok(Some(LinkEntry {
                    from: from.to_owned(),
                    to: to.to_owned(),
                    properties: Some(props),
                }))
            }
            Some(Value::Null) => Ok(Some(LinkEntry {
                from: from.to_owned(),
                to: to.to_owned(),
                properties: None,
            })),
            _ => Ok(None),
        }
    }

    #[allow(dead_code)]
    pub fn exists(&self, from: &str, to: &str) -> Result<bool> {
        error::validate_key(from)?;
        error::validate_key(to)?;
        let key = Self::composite_key(from, to);
        self.knot.backend.exists(&self.forward_ns(), &key)
    }

    pub fn insert(&self, from: &str, to: &str, props: &Properties) -> Result<()> {
        error::validate_key(from)?;
        error::validate_key(to)?;
        for prop_name in props.keys() {
            error::validate_name(prop_name)?;
        }
        self.validate_endpoints(from, to)?;

        let fwd_key = Self::composite_key(from, to);
        let rev_key = Self::composite_key(to, from);
        let bytes = property::encode_properties(props);

        self.knot
            .backend
            .put(&self.forward_ns(), &fwd_key, Value::Data(bytes))?;
        self.knot
            .backend
            .put(&self.reverse_ns(), &rev_key, Value::Null)?;
        Ok(())
    }

    pub fn insert_bare(&self, from: &str, to: &str) -> Result<()> {
        error::validate_key(from)?;
        error::validate_key(to)?;
        self.validate_endpoints(from, to)?;

        let fwd_key = Self::composite_key(from, to);
        let rev_key = Self::composite_key(to, from);

        self.knot
            .backend
            .put(&self.forward_ns(), &fwd_key, Value::Null)?;
        self.knot
            .backend
            .put(&self.reverse_ns(), &rev_key, Value::Null)?;
        Ok(())
    }

    pub fn delete(&self, from: &str, to: &str) -> Result<()> {
        error::validate_key(from)?;
        error::validate_key(to)?;
        let fwd_key = Self::composite_key(from, to);
        let rev_key = Self::composite_key(to, from);
        self.knot.backend.delete(&self.forward_ns(), &fwd_key)?;
        self.knot.backend.delete(&self.reverse_ns(), &rev_key)?;
        Ok(())
    }

    pub fn from(&self, key: &str) -> Result<Vec<LinkEntry>> {
        error::validate_key(key)?;
        self.scan_prefix(&self.forward_ns(), key, true)
    }

    pub fn to(&self, key: &str) -> Result<Vec<LinkEntry>> {
        error::validate_key(key)?;
        self.scan_prefix(&self.reverse_ns(), key, false)
    }

    fn scan_prefix(
        &self,
        ns_name: &str,
        prefix_key: &str,
        forward: bool,
    ) -> Result<Vec<LinkEntry>> {
        let scan_prefix = format!("{prefix_key}\x00");
        let keys = self.knot.backend.scan(ns_name, &scan_prefix, usize::MAX)?;

        let mut entries = Vec::new();
        for key_str in &keys {
            if let Some(pos) = key_str.find('\x00') {
                let (a, b_with_sep) = key_str.split_at(pos);
                let b = &b_with_sep[1..];
                let (from, to) = if forward { (a, b) } else { (b, a) };

                let fwd_key = Self::composite_key(from, to);
                let properties = match self.knot.backend.get(&self.forward_ns(), &fwd_key)? {
                    Some(Value::Data(bytes)) => Some(property::decode_properties(&bytes)?),
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

    fn validate_endpoints(&self, from: &str, to: &str) -> Result<()> {
        let source_ns = format!("knot.{}.t.{}", self.knot.namespace, self.def.source);
        let target_ns = format!("knot.{}.t.{}", self.knot.namespace, self.def.target);

        if !self.knot.backend.exists(&source_ns, from)? {
            return Err(Error::EndpointNotFound(format!(
                "{}.{from} (source)",
                self.def.source
            )));
        }
        if !self.knot.backend.exists(&target_ns, to)? {
            return Err(Error::EndpointNotFound(format!(
                "{}.{to} (target)",
                self.def.target
            )));
        }
        Ok(())
    }
}

// Schema operations on Knot for link tables.
impl Knot {
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

        let meta_ns = format!("knot.{}.meta", self.namespace);
        let meta_key = format!("link:{name}");
        let bytes = Metadata::encode_link_def(&def);
        self.backend.put(&meta_ns, &meta_key, Value::Data(bytes))?;

        let fwd = format!("knot.{}.l.{name}", self.namespace);
        let rev = format!("knot.{}.r.{name}", self.namespace);
        self.backend.ensure_namespace(&fwd)?;
        self.backend.ensure_namespace(&rev)?;

        self.meta.links.insert(name.to_owned(), def);
        Ok(())
    }

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

    pub fn drop_link(&mut self, name: &str) -> Result<()> {
        error::validate_name(name)?;
        if !self.meta.links.contains_key(name) {
            return Err(Error::LinkTableNotFound(name.to_owned()));
        }
        let fwd = format!("knot.{}.l.{name}", self.namespace);
        let rev = format!("knot.{}.r.{name}", self.namespace);
        self.backend.drop_namespace(&fwd)?;
        self.backend.drop_namespace(&rev)?;

        let meta_ns = format!("knot.{}.meta", self.namespace);
        let meta_key = format!("link:{name}");
        self.backend.delete(&meta_ns, &meta_key)?;

        self.meta.links.remove(name);
        Ok(())
    }

    pub fn links(&self) -> Vec<String> {
        self.meta.links.keys().cloned().collect()
    }

    pub fn link(&self, name: &str) -> Result<Link<'_>> {
        let def = self
            .meta
            .links
            .get(name)
            .ok_or_else(|| Error::LinkTableNotFound(name.to_owned()))?;
        Ok(Link::new(self, name, def.clone()))
    }

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

        let meta_ns = format!("knot.{}.meta", self.namespace);
        let meta_key = format!("link:{name}");
        let bytes = Metadata::encode_link_def(def);
        self.backend.put(&meta_ns, &meta_key, Value::Data(bytes))?;
        Ok(())
    }
}
