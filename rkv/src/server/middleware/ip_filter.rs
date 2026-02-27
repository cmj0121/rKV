use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use tower::{Layer, Service};

/// Shared allow-list state.
#[derive(Clone)]
struct AllowList {
    /// If true, skip IP checking entirely.
    allow_all: bool,
    /// Permitted source IPs.
    allowed: HashSet<IpAddr>,
}

/// Tower layer that rejects requests from IPs not in the allow list.
///
/// Applied only to `/api/*` routes. `/` and `/health` are exempt.
#[derive(Clone)]
pub struct IpFilterLayer {
    state: Arc<AllowList>,
}

impl IpFilterLayer {
    pub fn new(allow_all: bool, allow_ips: &[String]) -> Self {
        let mut allowed: HashSet<IpAddr> =
            allow_ips.iter().filter_map(|s| s.parse().ok()).collect();
        // Always allow loopback when explicit IPs are given
        if allowed.is_empty() && !allow_all {
            allowed.insert("127.0.0.1".parse().unwrap());
        }
        Self {
            state: Arc::new(AllowList { allow_all, allowed }),
        }
    }
}

impl<S> Layer<S> for IpFilterLayer {
    type Service = IpFilterService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        IpFilterService {
            inner,
            state: Arc::clone(&self.state),
        }
    }
}

/// Tower service that enforces the IP allow list.
#[derive(Clone)]
pub struct IpFilterService<S> {
    inner: S,
    state: Arc<AllowList>,
}

impl<S> Service<Request<Body>> for IpFilterService<S>
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
        let path = req.uri().path().to_owned();

        // Exempt non-API paths from IP filtering
        if !path.starts_with("/api") {
            let mut svc = self.inner.clone();
            return Box::pin(async move { svc.call(req).await });
        }

        // Check IP
        if !self.state.allow_all {
            let peer_ip = req
                .extensions()
                .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
                .map(|ci| ci.0.ip());

            match peer_ip {
                Some(ip) if self.state.allowed.contains(&ip) => {} // allowed
                _ => {
                    // No ConnectInfo or IP not in allow list → deny
                    return Box::pin(async move { Ok(StatusCode::FORBIDDEN.into_response()) });
                }
            }
        }

        let mut svc = self.inner.clone();
        Box::pin(async move { svc.call(req).await })
    }
}
