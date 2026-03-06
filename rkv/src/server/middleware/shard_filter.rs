use std::collections::HashSet;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use tower::{Layer, Service};

/// Shared shard ownership state.
#[derive(Clone)]
struct ShardState {
    /// Shard group ID for this node.
    shard_group: u16,
    /// Namespaces owned by this node. Empty = accept all (standalone).
    owned: HashSet<String>,
}

impl ShardState {
    fn is_cluster_mode(&self) -> bool {
        !self.owned.is_empty()
    }

    fn owns(&self, namespace: &str) -> bool {
        !self.is_cluster_mode() || self.owned.contains(namespace)
    }
}

/// Tower layer that returns 307 MOVED for namespaces not owned by this shard.
///
/// Only applies to `/api/{ns}/...` routes. Non-namespaced routes are passed through.
/// When `owned_namespaces` is empty, the filter is a no-op (standalone mode).
#[derive(Clone)]
pub struct ShardFilterLayer {
    state: Arc<ShardState>,
}

impl ShardFilterLayer {
    pub fn new(shard_group: u16, owned_namespaces: &[String]) -> Self {
        Self {
            state: Arc::new(ShardState {
                shard_group,
                owned: owned_namespaces.iter().cloned().collect(),
            }),
        }
    }
}

impl<S> Layer<S> for ShardFilterLayer {
    type Service = ShardFilterService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ShardFilterService {
            inner,
            state: Arc::clone(&self.state),
        }
    }
}

/// Tower service that enforces shard namespace ownership.
#[derive(Clone)]
pub struct ShardFilterService<S> {
    inner: S,
    state: Arc<ShardState>,
}

/// Extract namespace from a path like `/api/{ns}/...` or `/api/{ns}`.
/// Returns `None` for non-namespaced paths.
fn extract_namespace(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("/api/")?;
    let first = rest.split('/').next()?;
    // Skip admin and namespaces management routes
    if first == "admin" || first == "namespaces" {
        return None;
    }
    Some(first)
}

impl<S> Service<Request<Body>> for ShardFilterService<S>
where
    S: Service<Request<Body>, Response = Response> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        // Standalone mode — pass everything through
        if !self.state.is_cluster_mode() {
            let mut svc = self.inner.clone();
            return Box::pin(async move { svc.call(req).await });
        }

        // Check if this is a namespaced API request
        if let Some(ns) = extract_namespace(req.uri().path()) {
            if !self.state.owns(ns) {
                let shard_group = self.state.shard_group;
                let ns = ns.to_owned();
                return Box::pin(async move {
                    Ok((
                        StatusCode::TEMPORARY_REDIRECT,
                        [(
                            header::HeaderName::from_static("x-rkv-shard"),
                            shard_group.to_string(),
                        )],
                        format!("namespace '{ns}' is not owned by this shard"),
                    )
                        .into_response())
                });
            }
        }

        let mut svc = self.inner.clone();
        Box::pin(async move { svc.call(req).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_ns_from_key_path() {
        assert_eq!(extract_namespace("/api/users/keys/foo"), Some("users"));
    }

    #[test]
    fn extract_ns_from_namespace_root() {
        assert_eq!(extract_namespace("/api/orders"), Some("orders"));
    }

    #[test]
    fn extract_ns_skips_admin() {
        assert_eq!(extract_namespace("/api/admin/stats"), None);
    }

    #[test]
    fn extract_ns_skips_namespaces_endpoint() {
        assert_eq!(extract_namespace("/api/namespaces"), None);
    }

    #[test]
    fn extract_ns_non_api() {
        assert_eq!(extract_namespace("/health"), None);
        assert_eq!(extract_namespace("/metrics"), None);
    }

    #[test]
    fn extract_ns_does_not_match_namespace_prefixed_names() {
        // A namespace called "namespaces_data" should be treated as a namespace,
        // not confused with the /api/namespaces management endpoint
        assert_eq!(
            extract_namespace("/api/namespaces_data/keys/foo"),
            Some("namespaces_data")
        );
    }

    #[test]
    fn extract_ns_default() {
        assert_eq!(extract_namespace("/api/_/keys/foo"), Some("_"));
    }

    #[test]
    fn shard_state_standalone() {
        let state = ShardState {
            shard_group: 0,
            owned: HashSet::new(),
        };
        assert!(!state.is_cluster_mode());
        assert!(state.owns("anything"));
    }

    #[test]
    fn shard_state_cluster() {
        let state = ShardState {
            shard_group: 1,
            owned: ["users".to_string(), "sessions".to_string()]
                .into_iter()
                .collect(),
        };
        assert!(state.is_cluster_mode());
        assert!(state.owns("users"));
        assert!(state.owns("sessions"));
        assert!(!state.owns("orders"));
    }
}
