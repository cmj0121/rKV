pub mod backend;
mod cascade;
pub mod condition;
pub mod embedded;
pub mod error;
pub mod link;
mod metadata;
pub mod property;
pub mod query;
pub mod remote;
pub mod revision;
pub mod table;
pub mod traversal;

use std::sync::Arc;

use backend::Backend;
use error::Result;
use metadata::Metadata;

/// One Knot instance serves one namespace.
pub struct Knot {
    pub(crate) backend: Arc<dyn Backend>,
    pub(crate) namespace: String,
    pub(crate) meta: Metadata,
}

impl Knot {
    /// Open or create a Knot namespace with a given backend.
    pub fn open(backend: Arc<dyn Backend>, namespace: &str) -> Result<Self> {
        error::validate_name(namespace)?;
        let meta_ns = format!("knot.{namespace}.meta");
        backend.ensure_namespace(&meta_ns)?;
        let meta = Metadata::load(&*backend, namespace)?;
        Ok(Self {
            backend,
            namespace: namespace.to_owned(),
            meta,
        })
    }

    /// Open with an embedded rKV database (convenience).
    ///
    /// # Safety
    /// The DB must outlive this Knot instance.
    pub fn new(db: &rkv::DB, namespace: &str) -> Result<Self> {
        let backend = Arc::new(unsafe { embedded::EmbeddedBackend::new(db) });
        Self::open(backend, namespace)
    }

    /// Returns the namespace name.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the backend.
    pub fn backend(&self) -> &dyn Backend {
        &*self.backend
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
