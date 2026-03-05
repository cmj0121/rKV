use std::sync::Arc;

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use axum::Json;

use crate::server::error::ServerError;
use crate::server::AppState;

/// GET /api/admin/stats
pub async fn get_stats(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let s = state.db.stats();
    Json(serde_json::json!({
        "total_keys": s.total_keys,
        "data_size_bytes": s.data_size_bytes,
        "namespace_count": s.namespace_count,
        "level_count": s.level_count,
        "sstable_count": s.sstable_count,
        "write_buffer_bytes": s.write_buffer_bytes,
        "pending_compactions": s.pending_compactions,
        "op_puts": s.op_puts,
        "op_gets": s.op_gets,
        "op_deletes": s.op_deletes,
        "cache_hits": s.cache_hits,
        "cache_misses": s.cache_misses,
        "uptime_secs": s.uptime.as_secs(),
        "role": s.role,
        "peer_count": s.peer_count,
        "conflicts_resolved": s.conflicts_resolved,
        "level_stats": s.level_stats.iter().map(|l| {
            serde_json::json!({"file_count": l.file_count, "size_bytes": l.size_bytes})
        }).collect::<Vec<_>>(),
    }))
}

/// POST /api/admin/analyze
pub async fn analyze(State(state): State<Arc<AppState>>) -> Json<&'static str> {
    state.db.analyze();
    Json("ok")
}

/// POST /api/admin/flush
pub async fn flush(State(state): State<Arc<AppState>>) -> Result<Json<&'static str>, ServerError> {
    let st = state.clone();
    tokio::task::spawn_blocking(move || st.db.flush())
        .await
        .map_err(|_| ServerError::Internal("flush task cancelled"))??;
    Ok(Json("ok"))
}

/// POST /api/admin/sync
pub async fn sync(State(state): State<Arc<AppState>>) -> Result<Json<&'static str>, ServerError> {
    let st = state.clone();
    tokio::task::spawn_blocking(move || st.db.sync())
        .await
        .map_err(|_| ServerError::Internal("sync task cancelled"))??;
    Ok(Json("ok"))
}

/// POST /api/admin/compact
pub async fn compact(
    State(state): State<Arc<AppState>>,
) -> Result<Json<&'static str>, ServerError> {
    let st = state.clone();
    tokio::task::spawn_blocking(move || st.db.compact())
        .await
        .map_err(|_| ServerError::Internal("compact task cancelled"))??;
    Ok(Json("ok"))
}

/// POST /api/admin/force-sync
pub async fn force_sync(
    State(state): State<Arc<AppState>>,
) -> Result<Json<&'static str>, ServerError> {
    if !state.db.is_replica() {
        return Err(crate::Error::ReadOnlyReplica.into());
    }
    state.db.force_sync()?;
    Ok(Json("ok"))
}

/// GET /metrics — Prometheus exposition format
pub async fn prometheus_metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let body = state.db.prometheus_metrics();
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

/// GET /api/admin/config
pub async fn get_config(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let c = state.db.config();
    Json(serde_json::json!({
        "path": "<redacted>",
        "create_if_missing": c.create_if_missing,
        "write_buffer_size": c.write_buffer_size,
        "max_levels": c.max_levels,
        "block_size": c.block_size,
        "cache_size": c.cache_size,
        "object_size": c.object_size,
        "compress": c.compress,
        "bloom_bits": c.bloom_bits,
        "bloom_prefix_len": c.bloom_prefix_len,
        "verify_checksums": c.verify_checksums,
        "compression": format!("{:?}", c.compression),
        "io_model": format!("{:?}", c.io_model),
        "cluster_id": c.cluster_id,
        "aol_buffer_size": c.aol_buffer_size,
        "l0_max_count": c.l0_max_count,
        "l0_max_size": c.l0_max_size,
        "l1_max_size": c.l1_max_size,
        "default_max_size": c.default_max_size,
        "shard_group": c.shard_group,
        "owned_namespaces": c.owned_namespaces,
    }))
}

/// GET /api/admin/cluster — cluster routing table and shard info
pub async fn get_cluster(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let c = state.db.config();
    let rt = state
        .routing_table
        .read()
        .unwrap_or_else(|e| e.into_inner());
    let routes: serde_json::Map<String, serde_json::Value> = rt
        .routes
        .iter()
        .map(|(ns, sg)| (ns.clone(), serde_json::json!(sg.id)))
        .collect();
    Json(serde_json::json!({
        "shard_group": c.shard_group,
        "owned_namespaces": c.owned_namespaces,
        "routing_table": {
            "version": rt.version,
            "routes": routes,
            "default_group": rt.default_group.id,
        },
    }))
}

/// Request body for `POST /api/admin/route`.
#[derive(serde::Deserialize)]
pub(crate) struct SetRouteRequest {
    namespace: String,
    shard_group: u16,
}

/// POST /api/admin/route — update a namespace-to-shard mapping
///
/// Request body: `{"namespace": "users", "shard_group": 2}`
pub async fn set_route(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SetRouteRequest>,
) -> Result<Json<serde_json::Value>, ServerError> {
    let namespace = body.namespace;
    let group_id = body.shard_group;

    let mut rt = state
        .routing_table
        .write()
        .unwrap_or_else(|e| e.into_inner());
    rt.set_route(namespace.clone(), crate::ShardGroup::new(group_id));
    let version = rt.version;
    drop(rt);

    Ok(Json(serde_json::json!({
        "ok": true,
        "namespace": namespace,
        "shard_group": group_id,
        "version": version,
    })))
}
