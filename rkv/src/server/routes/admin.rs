use std::sync::Arc;

use axum::extract::State;
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
        "level_stats": s.level_stats.iter().map(|l| {
            serde_json::json!({"file_count": l.file_count, "size_bytes": l.size_bytes})
        }).collect::<Vec<_>>(),
    }))
}

/// POST /api/admin/analyze
pub async fn analyze(State(state): State<Arc<AppState>>) -> Json<&'static str> {
    let _ = state.db.analyze();
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

/// GET /api/admin/config
pub async fn get_config(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let c = state.db.config();
    Json(serde_json::json!({
        "path": c.path.display().to_string(),
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
    }))
}
