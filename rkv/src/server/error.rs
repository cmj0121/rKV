use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

pub enum ServerError {
    Engine(crate::Error),
    BadRequest(&'static str),
    Internal(&'static str),
}

impl From<crate::Error> for ServerError {
    fn from(err: crate::Error) -> Self {
        Self::Engine(err)
    }
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        use crate::Error;

        let (status, msg, detail) = match &self {
            Self::BadRequest(msg) => (StatusCode::BAD_REQUEST, *msg, msg.to_string()),
            Self::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, *msg, msg.to_string()),
            Self::Engine(err) => match err {
                Error::KeyNotFound => (StatusCode::NOT_FOUND, "key not found", String::new()),
                Error::InvalidKey(d) => (StatusCode::BAD_REQUEST, "invalid key", d.clone()),
                Error::InvalidNamespace(d) => {
                    (StatusCode::BAD_REQUEST, "invalid namespace", d.clone())
                }
                Error::Corruption(d) => {
                    (StatusCode::INTERNAL_SERVER_ERROR, "corruption", d.clone())
                }
                Error::EncryptionRequired(d) => {
                    (StatusCode::FORBIDDEN, "encryption required", d.clone())
                }
                Error::NotEncrypted(d) => (StatusCode::CONFLICT, "not encrypted", d.clone()),
                Error::InvalidConfig(d) => (StatusCode::BAD_REQUEST, "invalid config", d.clone()),
                Error::Io(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "io error",
                    format!("io error ({})", e.kind()),
                ),
                Error::NotImplemented(d) => {
                    (StatusCode::NOT_IMPLEMENTED, "not implemented", d.clone())
                }
            },
        };
        (status, Json(json!({"error": msg, "detail": detail}))).into_response()
    }
}
