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
}
