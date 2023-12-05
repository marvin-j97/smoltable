use crate::api::format_server_header;
use actix_web::{http::StatusCode, HttpResponse};
use serde_json::{json, Value};
use std::time::Instant;

pub fn build_response(
    before: Instant,
    status: StatusCode,
    message: &str,
    result: &Value,
) -> HttpResponse {
    let time_ms = before.elapsed().as_millis();

    let body = json!({
        "time_ms": time_ms,
        "status": status.as_u16(),
        "message": message,
        "result": result
    });
    let body = serde_json::to_string(&body).expect("should serialize");

    HttpResponse::build(status)
        .append_header(("x-server", format_server_header()))
        .append_header(("x-took-ms", time_ms.to_string()))
        .content_type("application/json; utf-8")
        .body(body)
}
