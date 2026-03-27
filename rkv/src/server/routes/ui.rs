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
        r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 22 22"><rect x="2" y="3" width="18" height="4" rx="1.5" fill="none" stroke="#4fc3f7" stroke-width="1.5"/><rect x="2" y="9" width="18" height="4" rx="1.5" fill="none" stroke="#4fc3f7" stroke-width="1.5"/><rect x="2" y="15" width="18" height="4" rx="1.5" fill="none" stroke="#4fc3f7" stroke-width="1.5"/><circle cx="6" cy="5" r="1" fill="#4fc3f7"/><circle cx="6" cy="11" r="1" fill="#4fc3f7"/><circle cx="6" cy="17" r="1" fill="#4fc3f7"/></svg>"##,
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
