use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};

pub async fn index() -> Html<&'static str> {
    Html(include_str!("ui/index.html"))
}

pub async fn app_js() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/javascript")],
        include_str!("ui/app.js"),
    )
        .into_response()
}

pub async fn style_css() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/css")],
        include_str!("ui/style.css"),
    )
        .into_response()
}

pub async fn docs() -> Html<&'static str> {
    Html(include_str!("ui/docs.html"))
}

pub async fn favicon() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "image/svg+xml")],
        r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><circle cx="12" cy="12" r="3" fill="none" stroke="#e8a84c" stroke-width="2"/><path d="M12 2v7M12 15v7M2 12h7M15 12h7" stroke="#e8a84c" stroke-width="1.5" stroke-linecap="round"/><circle cx="12" cy="2" r="1.5" fill="#e8a84c"/><circle cx="12" cy="22" r="1.5" fill="#e8a84c"/><circle cx="2" cy="12" r="1.5" fill="#e8a84c"/><circle cx="22" cy="12" r="1.5" fill="#e8a84c"/></svg>"##,
    )
        .into_response()
}

pub async fn openapi_yaml() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/yaml")],
        include_str!("ui/openapi.yaml"),
    )
        .into_response()
}
