pub mod routes;

use std::sync::{Arc, RwLock};

use axum::Router;
use tower_http::cors::CorsLayer;

use crate::engine::Knot;

/// Shared application state for the HTTP server.
pub struct AppState {
    pub backend: Arc<dyn crate::engine::backend::Backend>,
    pub namespaces: RwLock<std::collections::HashMap<String, Knot>>,
}

impl AppState {
    pub fn new(backend: Arc<dyn crate::engine::backend::Backend>) -> Self {
        Self {
            backend,
            namespaces: RwLock::new(std::collections::HashMap::new()),
        }
    }

    pub fn get_knot(&self, ns: &str) -> Result<(), crate::Error> {
        {
            let read = self.namespaces.read().unwrap();
            if read.contains_key(ns) {
                return Ok(());
            }
        }
        let knot = Knot::open(self.backend.clone(), ns)?;
        let mut write = self.namespaces.write().unwrap();
        write.insert(ns.to_owned(), knot);
        Ok(())
    }

    /// Discover all existing knot namespaces from the backend.
    /// Scans for rKV namespaces matching `knot.*.meta` pattern.
    pub fn discover_namespaces(&self) -> Vec<String> {
        let all = match self.backend.list_namespaces("knot.") {
            Ok(ns) => ns,
            Err(_) => return Vec::new(),
        };
        let mut names: std::collections::HashSet<String> = std::collections::HashSet::new();

        for ns_name in &all {
            // Parse knot.{namespace}.meta → extract {namespace}
            if let Some(rest) = ns_name.strip_prefix("knot.") {
                if let Some(name) = rest.strip_suffix(".meta") {
                    names.insert(name.to_owned());
                }
            }
        }

        // Also include any already-loaded namespaces
        let loaded = self.namespaces.read().unwrap();
        for key in loaded.keys() {
            names.insert(key.clone());
        }

        let mut result: Vec<String> = names.into_iter().collect();
        result.sort();
        result
    }
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .nest("/api", routes::api_routes())
        .route("/health", axum::routing::get(routes::health::health))
        .route("/", axum::routing::get(routes::ui::index))
        .route("/ui/app.js", axum::routing::get(routes::ui::app_js))
        .route("/ui/style.css", axum::routing::get(routes::ui::style_css))
        .route("/docs", axum::routing::get(routes::ui::docs))
        .route(
            "/api/openapi.yaml",
            axum::routing::get(routes::ui::openapi_yaml),
        )
        .layer(CorsLayer::permissive())
        .with_state(state)
}
