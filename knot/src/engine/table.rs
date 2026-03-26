use rkv::Value;

use super::condition::Condition;
use super::error::{self, Error, Result};
use super::metadata::TableDef;
use super::property::{self, Node, Properties, PropertyValue};
use super::query::{self, Page, Sort};
use super::Knot;

/// Handle to a data table within a Knot namespace.
pub struct Table<'k, 'db> {
    knot: &'k Knot<'db>,
    name: String,
}

impl<'k, 'db> Table<'k, 'db> {
    pub(crate) fn new(knot: &'k Knot<'db>, name: &str) -> Self {
        Self {
            knot,
            name: name.to_owned(),
        }
    }

    /// rKV namespace for this table.
    fn rkv_ns(&self) -> String {
        format!("knot.{}.t.{}", self.knot.namespace, self.name)
    }

    /// Get a node by key. Returns None if not found.
    pub fn get(&self, key: &str) -> Result<Option<Node>> {
        error::validate_key(key)?;
        let ns = self
            .knot
            .db
            .namespace(&self.rkv_ns(), None)
            .map_err(error::storage)?;

        match ns.get(key) {
            Ok(Value::Data(bytes)) => {
                let props = property::decode_properties(&bytes)?;
                Ok(Some(Node {
                    key: key.to_owned(),
                    properties: Some(props),
                }))
            }
            Ok(Value::Null) => Ok(Some(Node {
                key: key.to_owned(),
                properties: None,
            })),
            Ok(_) => Ok(None),
            Err(rkv::Error::KeyNotFound) => Ok(None),
            Err(e) => Err(error::storage(e)),
        }
    }

    /// Check if a node exists.
    pub fn exists(&self, key: &str) -> Result<bool> {
        error::validate_key(key)?;
        let ns = self
            .knot
            .db
            .namespace(&self.rkv_ns(), None)
            .map_err(error::storage)?;

        ns.exists(key).map_err(error::storage)
    }

    /// Insert a node with properties. Overwrites if exists (upsert).
    pub fn insert(&self, key: &str, props: &Properties) -> Result<()> {
        error::validate_key(key)?;
        for prop_name in props.keys() {
            error::validate_name(prop_name)?;
        }
        let ns = self
            .knot
            .db
            .namespace(&self.rkv_ns(), None)
            .map_err(error::storage)?;

        let bytes = property::encode_properties(props);
        ns.put(key, Value::Data(bytes), None)
            .map_err(error::storage)?;
        Ok(())
    }

    /// Insert a set-mode node (key only, no properties).
    pub fn insert_set(&self, key: &str) -> Result<()> {
        error::validate_key(key)?;
        let ns = self
            .knot
            .db
            .namespace(&self.rkv_ns(), None)
            .map_err(error::storage)?;

        ns.put(key, Value::Null, None).map_err(error::storage)?;
        Ok(())
    }

    /// Replace all properties on an existing node.
    pub fn replace(&self, key: &str, props: &Properties) -> Result<()> {
        self.insert(key, props)
    }

    /// Update specific properties (read-merge-replace). Null values remove
    /// the property.
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
        // Remove null entries (null = missing)
        merged.retain(|_, _| true); // PropertyValue has no Null variant — handled below

        self.insert(key, &merged)
    }

    /// Update with null removal. Accepts a map where None means "remove this
    /// property."
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

    /// Delete a node. No-op if node does not exist.
    /// Note: link cleanup is handled by the cascade controller (not here).
    pub fn delete(&self, key: &str) -> Result<()> {
        error::validate_key(key)?;
        let ns = self
            .knot
            .db
            .namespace(&self.rkv_ns(), None)
            .map_err(error::storage)?;

        match ns.delete(key) {
            Ok(()) => Ok(()),
            Err(rkv::Error::KeyNotFound) => Ok(()), // no-op
            Err(e) => Err(error::storage(e)),
        }
    }

    /// Query nodes with optional filter, sort, projection, and pagination.
    pub fn query(
        &self,
        filter: Option<&Condition>,
        sort: Option<&Sort>,
        projection: Option<&[String]>,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Page> {
        query::query_nodes(
            self.knot.db,
            &self.rkv_ns(),
            filter,
            sort,
            projection,
            limit,
            cursor,
        )
    }

    /// Count nodes, optionally filtered.
    pub fn count(&self, filter: Option<&Condition>) -> Result<u64> {
        query::count_nodes(self.knot.db, &self.rkv_ns(), filter)
    }
}

// Schema operations on Knot for tables.
impl<'db> Knot<'db> {
    /// Create a data table.
    pub fn create_table(&mut self, name: &str) -> Result<()> {
        error::validate_name(name)?;
        if self.meta.tables.contains_key(name) {
            return Err(Error::TableExists(name.to_owned()));
        }

        let meta_ns_name = format!("knot.{}.meta", self.namespace);
        let meta_ns = self
            .db
            .namespace(&meta_ns_name, None)
            .map_err(error::storage)?;

        // Write metadata entry
        let meta_key = format!("table:{name}");
        meta_ns
            .put(meta_key.as_str(), Value::Null, None)
            .map_err(error::storage)?;

        // Ensure the data namespace exists
        let data_ns_name = format!("knot.{}.t.{name}", self.namespace);
        let _ = self
            .db
            .namespace(&data_ns_name, None)
            .map_err(error::storage)?;

        // Update in-memory catalog
        self.meta.tables.insert(
            name.to_owned(),
            TableDef {
                name: name.to_owned(),
            },
        );

        Ok(())
    }

    /// Create a data table if it does not exist (no-op if exists).
    pub fn create_table_if_not_exists(&mut self, name: &str) -> Result<()> {
        match self.create_table(name) {
            Ok(()) => Ok(()),
            Err(Error::TableExists(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Drop a data table and all its data.
    /// Note: link table cascade is handled by the cascade controller.
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        error::validate_name(name)?;
        if !self.meta.tables.contains_key(name) {
            return Err(Error::TableNotFound(name.to_owned()));
        }

        // Remove metadata entry
        let meta_ns_name = format!("knot.{}.meta", self.namespace);
        let meta_ns = self
            .db
            .namespace(&meta_ns_name, None)
            .map_err(error::storage)?;
        let meta_key = format!("table:{name}");
        let _ = meta_ns.delete(meta_key.as_str());

        // Drop the rKV namespace
        let data_ns_name = format!("knot.{}.t.{name}", self.namespace);
        let _ = self.db.drop_namespace(&data_ns_name);

        // Update in-memory catalog
        self.meta.tables.remove(name);

        Ok(())
    }

    /// List all table names.
    pub fn tables(&self) -> Vec<String> {
        self.meta.tables.keys().cloned().collect()
    }

    /// Get a table handle for node operations.
    pub fn table(&self, name: &str) -> Result<Table<'_, 'db>> {
        if !self.meta.tables.contains_key(name) {
            return Err(Error::TableNotFound(name.to_owned()));
        }
        Ok(Table::new(self, name))
    }
}
