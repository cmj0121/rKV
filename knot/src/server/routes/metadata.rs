use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;

use super::namespaces::ErrorBody;
use crate::server::AppState;

// --- Tables ---

#[derive(Deserialize)]
pub struct CreateTable {
    pub name: String,
    #[serde(default)]
    pub if_not_exists: bool,
}

pub async fn list_tables(
    State(state): State<Arc<AppState<'static>>>,
    Path(ns): Path<String>,
) -> Result<Json<Vec<String>>, (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let namespaces = state.namespaces.read().unwrap();
    let knot = namespaces.get(&ns).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody::new("namespace not found")),
        )
    })?;
    let mut tables = knot.tables();
    tables.sort();
    Ok(Json(tables))
}

pub async fn create_table(
    State(state): State<Arc<AppState<'static>>>,
    Path(ns): Path<String>,
    Json(body): Json<CreateTable>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let mut namespaces = state.namespaces.write().unwrap();
    let knot = namespaces.get_mut(&ns).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody::new("namespace not found")),
        )
    })?;

    let result = if body.if_not_exists {
        knot.create_table_if_not_exists(&body.name)
    } else {
        knot.create_table(&body.name)
    };

    result.map_err(|e| (StatusCode::CONFLICT, Json(ErrorBody::from(e))))?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({"name": body.name})),
    ))
}

pub async fn drop_table(
    State(state): State<Arc<AppState<'static>>>,
    Path((ns, table)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let mut namespaces = state.namespaces.write().unwrap();
    let knot = namespaces.get_mut(&ns).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody::new("namespace not found")),
        )
    })?;
    knot.drop_table(&table)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    Ok(StatusCode::OK)
}

// --- Link tables ---

#[derive(Deserialize)]
pub struct CreateLink {
    pub name: String,
    pub source: String,
    pub target: String,
    #[serde(default)]
    pub bidirectional: bool,
    #[serde(default)]
    pub cascade: bool,
    #[serde(default)]
    pub if_not_exists: bool,
}

#[derive(Deserialize)]
pub struct AlterLink {
    pub bidirectional: Option<bool>,
    pub cascade: Option<bool>,
}

pub async fn list_links(
    State(state): State<Arc<AppState<'static>>>,
    Path(ns): Path<String>,
) -> Result<Json<Vec<String>>, (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let namespaces = state.namespaces.read().unwrap();
    let knot = namespaces.get(&ns).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody::new("namespace not found")),
        )
    })?;
    let mut links = knot.links();
    links.sort();
    Ok(Json(links))
}

pub async fn create_link(
    State(state): State<Arc<AppState<'static>>>,
    Path(ns): Path<String>,
    Json(body): Json<CreateLink>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let mut namespaces = state.namespaces.write().unwrap();
    let knot = namespaces.get_mut(&ns).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody::new("namespace not found")),
        )
    })?;

    let result = if body.if_not_exists {
        knot.create_link_if_not_exists(
            &body.name,
            &body.source,
            &body.target,
            body.bidirectional,
            body.cascade,
        )
    } else {
        knot.create_link(
            &body.name,
            &body.source,
            &body.target,
            body.bidirectional,
            body.cascade,
        )
    };

    result.map_err(|e| (StatusCode::CONFLICT, Json(ErrorBody::from(e))))?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "name": body.name,
            "source": body.source,
            "target": body.target,
            "bidirectional": body.bidirectional,
            "cascade": body.cascade,
        })),
    ))
}

pub async fn drop_link(
    State(state): State<Arc<AppState<'static>>>,
    Path((ns, link)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let mut namespaces = state.namespaces.write().unwrap();
    let knot = namespaces.get_mut(&ns).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody::new("namespace not found")),
        )
    })?;
    knot.drop_link(&link)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    Ok(StatusCode::OK)
}

pub async fn alter_link(
    State(state): State<Arc<AppState<'static>>>,
    Path((ns, link)): Path<(String, String)>,
    Json(body): Json<AlterLink>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let mut namespaces = state.namespaces.write().unwrap();
    let knot = namespaces.get_mut(&ns).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody::new("namespace not found")),
        )
    })?;
    knot.alter_link(&link, body.bidirectional, body.cascade)
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorBody::from(e))))?;
    Ok(StatusCode::OK)
}

impl ErrorBody {
    pub fn new(msg: &str) -> Self {
        Self {
            error: msg.to_owned(),
        }
    }
}
