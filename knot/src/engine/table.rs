use rkv::Value;

use super::cascade;
use super::condition::Condition;
use super::error::{self, Error, Result};
use super::metadata::TableDef;
use super::property::{self, Node, Properties, PropertyValue};
use super::query::{self, Page, Sort};
use super::Knot;

/// Handle to a data table within a Knot namespace.
pub struct Table<'k> {
    knot: &'k Knot,
    name: String,
}

impl<'k> Table<'k> {
    pub(crate) fn new(knot: &'k Knot, name: &str) -> Self {
        Self {
            knot,
            name: name.to_owned(),
        }
    }

    pub(crate) fn knot(&self) -> &'k Knot {
        self.knot
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    fn rkv_ns(&self) -> String {
        format!("knot.{}.t.{}", self.knot.namespace, self.name)
    }

    pub fn get(&self, key: &str) -> Result<Option<Node>> {
        error::validate_key(key)?;
        let ns = self.rkv_ns();
        match self.knot.backend.get(&ns, key)? {
            Some(Value::Data(bytes)) => {
                let props = property::decode_properties(&bytes)?;
                Ok(Some(Node {
                    key: key.to_owned(),
                    properties: Some(props),
                }))
            }
            Some(Value::Null) => Ok(Some(Node {
                key: key.to_owned(),
                properties: None,
            })),
            _ => Ok(None),
        }
    }

    pub fn exists(&self, key: &str) -> Result<bool> {
        error::validate_key(key)?;
        self.knot.backend.exists(&self.rkv_ns(), key)
    }

    pub fn insert(&self, key: &str, props: &Properties) -> Result<()> {
        error::validate_key(key)?;
        for prop_name in props.keys() {
            error::validate_name(prop_name)?;
        }
        let bytes = property::encode_properties(props);
        self.knot
            .backend
            .put(&self.rkv_ns(), key, Value::Data(bytes))
    }

    pub fn insert_set(&self, key: &str) -> Result<()> {
        error::validate_key(key)?;
        self.knot.backend.put(&self.rkv_ns(), key, Value::Null)
    }

    pub fn replace(&self, key: &str, props: &Properties) -> Result<()> {
        self.insert(key, props)
    }

    pub fn update(&self, key: &str, changes: &Properties) -> Result<()> {
        error::validate_key(key)?;
        for prop_name in changes.keys() {
            error::validate_name(prop_name)?;
        }
        let existing = self.get(key)?;
        let node = existing.ok_or_else(|| Error::KeyNotFound(key.to_owned()))?;
        let mut merged = node.properties.unwrap_or_default();
        for (k, v) in changes {
            merged.insert(k.clone(), v.clone());
        }
        self.insert(key, &merged)
    }

    pub fn update_with_nulls(
        &self,
        key: &str,
        changes: &std::collections::HashMap<String, Option<PropertyValue>>,
    ) -> Result<()> {
        error::validate_key(key)?;
        for prop_name in changes.keys() {
            error::validate_name(prop_name)?;
        }
        let existing = self.get(key)?;
        let node = existing.ok_or_else(|| Error::KeyNotFound(key.to_owned()))?;
        let mut merged = node.properties.unwrap_or_default();
        for (k, v) in changes {
            match v {
                Some(val) => {
                    merged.insert(k.clone(), val.clone());
                }
                None => {
                    merged.remove(k);
                }
            }
        }
        if merged.is_empty() {
            self.insert_set(key)
        } else {
            self.insert(key, &merged)
        }
    }

    pub fn delete(&self, key: &str) -> Result<()> {
        error::validate_key(key)?;
        self.knot.backend.delete(&self.rkv_ns(), key)
    }

    pub fn delete_cascade(&self, key: &str, do_cascade: bool) -> Result<()> {
        error::validate_key(key)?;
        cascade::delete_node(self.knot, &self.name, key, do_cascade)
    }

    pub fn query(
        &self,
        filter: Option<&Condition>,
        sort: Option<&Sort>,
        projection: Option<&[String]>,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Page> {
        query::query_nodes(
            &*self.knot.backend,
            &self.rkv_ns(),
            filter,
            sort,
            projection,
            limit,
            cursor,
        )
    }

    pub fn count(&self, filter: Option<&Condition>) -> Result<u64> {
        query::count_nodes(&*self.knot.backend, &self.rkv_ns(), filter)
    }
}

// Schema operations on Knot for tables.
impl Knot {
    pub fn create_table(&mut self, name: &str) -> Result<()> {
        error::validate_name(name)?;
        if self.meta.tables.contains_key(name) {
            return Err(Error::TableExists(name.to_owned()));
        }
        let meta_ns = format!("knot.{}.meta", self.namespace);
        let meta_key = format!("table:{name}");
        self.backend.put(&meta_ns, &meta_key, Value::Null)?;
        let data_ns = format!("knot.{}.t.{name}", self.namespace);
        self.backend.ensure_namespace(&data_ns)?;
        self.meta.tables.insert(
            name.to_owned(),
            TableDef {
                name: name.to_owned(),
            },
        );
        Ok(())
    }

    pub fn create_table_if_not_exists(&mut self, name: &str) -> Result<()> {
        match self.create_table(name) {
            Ok(()) => Ok(()),
            Err(Error::TableExists(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        error::validate_name(name)?;
        if !self.meta.tables.contains_key(name) {
            return Err(Error::TableNotFound(name.to_owned()));
        }
        let meta_ns = format!("knot.{}.meta", self.namespace);
        let meta_key = format!("table:{name}");
        self.backend.delete(&meta_ns, &meta_key)?;
        let data_ns = format!("knot.{}.t.{name}", self.namespace);
        self.backend.drop_namespace(&data_ns)?;
        self.meta.tables.remove(name);
        Ok(())
    }

    pub fn tables(&self) -> Vec<String> {
        self.meta.tables.keys().cloned().collect()
    }

    pub fn table(&self, name: &str) -> Result<Table<'_>> {
        if !self.meta.tables.contains_key(name) {
            return Err(Error::TableNotFound(name.to_owned()));
        }
        Ok(Table::new(self, name))
    }
}
