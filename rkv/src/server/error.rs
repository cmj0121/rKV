use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

pub struct ServerError(pub crate::Error);

impl From<crate::Error> for ServerError {
    fn from(err: crate::Error) -> Self {
        Self(err)
    }
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        use crate::Error;

        let (status, msg) = match &self.0 {
            Error::KeyNotFound => (StatusCode::NOT_FOUND, "key not found"),
            Error::InvalidKey(_) => (StatusCode::BAD_REQUEST, "invalid key"),
            Error::InvalidNamespace(_) => (StatusCode::BAD_REQUEST, "invalid namespace"),
            Error::Corruption(_) => (StatusCode::INTERNAL_SERVER_ERROR, "corruption"),
            Error::EncryptionRequired(_) => (StatusCode::FORBIDDEN, "encryption required"),
            Error::NotEncrypted(_) => (StatusCode::CONFLICT, "not encrypted"),
            Error::InvalidConfig(_) => (StatusCode::BAD_REQUEST, "invalid config"),
            Error::Io(_) => (StatusCode::INTERNAL_SERVER_ERROR, "io error"),
            Error::NotImplemented(_) => (StatusCode::NOT_IMPLEMENTED, "not implemented"),
        };
        (status, Json(msg)).into_response()
    }
}
