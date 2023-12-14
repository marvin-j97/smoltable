pub mod create_column_family;
pub mod create_table;
pub mod delete_row;
pub mod delete_table;
pub mod get_row;
pub mod list_tables;
pub mod metrics;
pub mod prefix;
pub mod system;
pub mod write;

use crate::{error::CustomRouteResult, response::build_response};
use actix_web::{http::StatusCode, HttpResponse};
use serde_json::json;
use std::time::Instant;

pub fn format_server_header() -> String {
    format!("{} {}", env!("CARGO_CRATE_NAME"), env!("CARGO_PKG_VERSION"))
}

pub fn bad_request(before: Instant, msg: &str) -> CustomRouteResult<HttpResponse> {
    Ok(build_response(
        before,
        StatusCode::BAD_REQUEST,
        msg,
        &json!(null),
    ))
}
