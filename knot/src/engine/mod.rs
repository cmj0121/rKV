mod cascade;
pub mod condition;
pub mod error;
pub mod link;
mod metadata;
pub mod property;
pub mod query;
pub mod table;
pub mod traversal;

use rkv::DB;

use error::Result;
use metadata::Metadata;

/// One Knot instance serves one namespace. Borrows an rKV `DB` reference.
#[allow(dead_code)]
pub struct Knot<'db> {
    db: &'db DB,
    namespace: String,
    meta: Metadata,
}

impl<'db> Knot<'db> {
    /// Open or create a Knot namespace.
    pub fn new(db: &'db DB, namespace: &str) -> Result<Self> {
        error::validate_name(namespace)?;
        let meta_ns = format!("knot.{namespace}.meta");
        let _ = db.namespace(&meta_ns, None).map_err(error::storage)?;
        let meta = Metadata::load(db, namespace)?;
        Ok(Self {
            db,
            namespace: namespace.to_owned(),
            meta,
        })
    }

    /// Returns the namespace name.
    #[allow(dead_code)]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns a reference to the underlying rKV database.
    #[allow(dead_code)]
    pub fn db(&self) -> &'db DB {
        self.db
    }

    /// Directed traversal: follow a sequence of link tables from a start node.
    pub fn traverse(
        &self,
        start_table: &str,
        start_key: &str,
        link_names: &[&str],
        link_filter: Option<&condition::Condition>,
        node_filter: Option<&condition::Condition>,
        with_paths: bool,
    ) -> Result<traversal::TraversalResult> {
        traversal::directed(
            self,
            start_table,
            start_key,
            link_names,
            link_filter,
            node_filter,
            with_paths,
        )
    }

    /// Discovery traversal: follow all applicable links up to max_hops.
    pub fn discover(
        &self,
        start_table: &str,
        start_key: &str,
        max_hops: usize,
        bidi: bool,
    ) -> Result<traversal::TraversalResult> {
        traversal::discovery(self, start_table, start_key, max_hops, bidi)
    }
}
