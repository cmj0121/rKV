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
