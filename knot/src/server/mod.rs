pub mod routes;

use std::sync::{Arc, RwLock};

use axum::Router;
use tower_http::cors::CorsLayer;

use crate::engine::Knot;

/// Shared application state for the HTTP server.
pub struct AppState<'db> {
    pub db: &'db rkv::DB,
    /// Map of namespace name → Knot instance. Created on first access.
    pub namespaces: RwLock<std::collections::HashMap<String, Knot<'db>>>,
}

impl<'db> AppState<'db> {
    pub fn new(db: &'db rkv::DB) -> Self {
        Self {
            db,
            namespaces: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Get or create a Knot instance for the given namespace.
    pub fn get_knot(&self, ns: &str) -> Result<(), crate::Error> {
        {
            let read = self.namespaces.read().unwrap();
            if read.contains_key(ns) {
                return Ok(());
            }
        }
        let knot = Knot::new(self.db, ns)?;
        let mut write = self.namespaces.write().unwrap();
        write.insert(ns.to_owned(), knot);
        Ok(())
    }
}

/// Build the Axum router for the Knot HTTP API.
pub fn build_router(state: Arc<AppState<'static>>) -> Router {
    Router::new()
        .nest("/api", routes::api_routes())
        .route("/health", axum::routing::get(routes::health::health))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
