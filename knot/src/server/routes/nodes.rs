use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;

use super::namespaces::ErrorBody;
use crate::server::AppState;
use crate::PropertyValue;

#[derive(serde::Deserialize)]
pub struct ScanParams {
    pub prefix: Option<String>,
    pub limit: Option<usize>,
    pub count: Option<bool>,
    pub detail: Option<bool>,
}

pub async fn get_node(
    State(state): State<Arc<AppState>>,
    Path((ns, table, key)): Path<(String, String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let namespaces = state.namespaces.read().unwrap();
    let knot = namespaces.get(&ns).ok_or_else(|| not_found("namespace"))?;
    let tbl = knot
        .table(&table)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;

    match tbl.get(&key) {
        Ok(Some(node)) => {
            let props = node.properties.as_ref().map(props_to_json);
            Ok(Json(serde_json::json!({
                "key": node.key,
                "properties": props,
            })))
        }
        Ok(None) => Err(not_found("key")),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorBody::from(e)))),
    }
}

pub async fn put_node(
    State(state): State<Arc<AppState>>,
    Path((ns, table, key)): Path<(String, String, String)>,
    body: Option<Json<HashMap<String, serde_json::Value>>>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let namespaces = state.namespaces.read().unwrap();
    let knot = namespaces.get(&ns).ok_or_else(|| not_found("namespace"))?;
    let tbl = knot
        .table(&table)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;

    match body {
        Some(Json(map)) if !map.is_empty() => {
            let props = json_map_to_props(&map)?;
            tbl.insert(&key, &props)
                .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorBody::from(e))))?;
        }
        _ => {
            tbl.insert_set(&key)
                .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorBody::from(e))))?;
        }
    }
    Ok(StatusCode::CREATED)
}

pub async fn patch_node(
    State(state): State<Arc<AppState>>,
    Path((ns, table, key)): Path<(String, String, String)>,
    Json(map): Json<HashMap<String, serde_json::Value>>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let namespaces = state.namespaces.read().unwrap();
    let knot = namespaces.get(&ns).ok_or_else(|| not_found("namespace"))?;
    let tbl = knot
        .table(&table)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;

    let changes: HashMap<String, Option<PropertyValue>> = map
        .iter()
        .map(|(k, v)| {
            if v.is_null() {
                Ok((k.clone(), None))
            } else {
                Ok((k.clone(), Some(json_to_prop(v)?)))
            }
        })
        .collect::<Result<_, (StatusCode, Json<ErrorBody>)>>()?;

    tbl.update_with_nulls(&key, &changes)
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorBody::from(e))))?;
    Ok(StatusCode::OK)
}

pub async fn delete_node(
    State(state): State<Arc<AppState>>,
    Path((ns, table, key)): Path<(String, String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let namespaces = state.namespaces.read().unwrap();
    let knot = namespaces.get(&ns).ok_or_else(|| not_found("namespace"))?;
    let tbl = knot
        .table(&table)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;

    let cascade = params.get("cascade").is_some_and(|v| v == "true");
    if cascade {
        tbl.delete_cascade(&key, true)
    } else {
        tbl.delete(&key)
    }
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorBody::from(e))))?;
    Ok(StatusCode::OK)
}

pub async fn head_node(
    State(state): State<Arc<AppState>>,
    Path((ns, table, key)): Path<(String, String, String)>,
) -> StatusCode {
    let Ok(()) = state.get_knot(&ns) else {
        return StatusCode::NOT_FOUND;
    };
    let namespaces = state.namespaces.read().unwrap();
    let Some(knot) = namespaces.get(&ns) else {
        return StatusCode::NOT_FOUND;
    };
    let Ok(tbl) = knot.table(&table) else {
        return StatusCode::NOT_FOUND;
    };
    match tbl.exists(&key) {
        Ok(true) => StatusCode::OK,
        _ => StatusCode::NOT_FOUND,
    }
}

pub async fn scan_nodes(
    State(state): State<Arc<AppState>>,
    Path((ns, table)): Path<(String, String)>,
    Query(params): Query<ScanParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    state
        .get_knot(&ns)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;
    let namespaces = state.namespaces.read().unwrap();
    let knot = namespaces.get(&ns).ok_or_else(|| not_found("namespace"))?;
    let tbl = knot
        .table(&table)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorBody::from(e))))?;

    if params.count.unwrap_or(false) {
        let count = tbl
            .count(None)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorBody::from(e))))?;
        return Ok(Json(serde_json::json!({"count": count})));
    }

    let limit = params.limit.unwrap_or(40);
    let page = tbl
        .query(None, None, None, limit, None)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorBody::from(e))))?;

    if params.detail.unwrap_or(false) {
        let entries: Vec<serde_json::Value> = page
            .items
            .iter()
            .map(|n| {
                serde_json::json!({
                    "key": n.key,
                    "properties": n.properties.as_ref().map(props_to_json),
                })
            })
            .collect();
        Ok(Json(serde_json::json!({
            "entries": entries,
            "has_more": page.has_more,
            "cursor": page.cursor,
        })))
    } else {
        let keys: Vec<&str> = page.items.iter().map(|n| n.key.as_str()).collect();
        Ok(Json(serde_json::json!({
            "keys": keys,
            "has_more": page.has_more,
            "cursor": page.cursor,
        })))
    }
}

// --- Helpers ---

fn not_found(what: &str) -> (StatusCode, Json<ErrorBody>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorBody::new(&format!("{what} not found"))),
    )
}

pub fn props_to_json(props: &crate::Properties) -> serde_json::Value {
    let map: serde_json::Map<String, serde_json::Value> = props
        .iter()
        .map(|(k, v)| (k.clone(), prop_to_json(v)))
        .collect();
    serde_json::Value::Object(map)
}

fn prop_to_json(v: &PropertyValue) -> serde_json::Value {
    match v {
        PropertyValue::String(s) => serde_json::Value::String(s.clone()),
        PropertyValue::Integer(n) => serde_json::json!(n),
        PropertyValue::Float(f) => serde_json::json!(f),
        PropertyValue::Boolean(b) => serde_json::Value::Bool(*b),
        PropertyValue::Binary(b) => serde_json::json!(format!("<{} bytes>", b.len())),
        PropertyValue::Geo(lat, lon) => serde_json::json!([lat, lon]),
    }
}

pub fn json_to_prop(v: &serde_json::Value) -> Result<PropertyValue, (StatusCode, Json<ErrorBody>)> {
    match v {
        serde_json::Value::String(s) => Ok(PropertyValue::String(s.clone())),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(PropertyValue::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(PropertyValue::Float(f))
            } else {
                Err((
                    StatusCode::BAD_REQUEST,
                    Json(ErrorBody::new("unsupported number")),
                ))
            }
        }
        serde_json::Value::Bool(b) => Ok(PropertyValue::Boolean(*b)),
        serde_json::Value::Array(arr) if arr.len() == 2 => {
            let lat = arr[0].as_f64().ok_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorBody::new("geo lat must be number")),
                )
            })?;
            let lon = arr[1].as_f64().ok_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorBody::new("geo lon must be number")),
                )
            })?;
            Ok(PropertyValue::Geo(lat, lon))
        }
        _ => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody::new("unsupported property value type")),
        )),
    }
}

pub fn json_map_to_props(
    map: &HashMap<String, serde_json::Value>,
) -> Result<crate::Properties, (StatusCode, Json<ErrorBody>)> {
    map.iter()
        .map(|(k, v)| Ok((k.clone(), json_to_prop(v)?)))
        .collect()
}
