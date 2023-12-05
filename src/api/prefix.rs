use crate::app_state::AppState;
use crate::error::CustomRouteResult;
use crate::identifier::is_valid_identifier;
use crate::response::build_response;
use crate::table::QueryInput;
use actix_web::http::StatusCode;
use actix_web::{
    post,
    web::{self, Path},
    HttpResponse,
};
use serde_json::json;
use std::time::Instant;

fn bad_request(before: Instant, msg: &str) -> CustomRouteResult<HttpResponse> {
    Ok(build_response(
        before,
        StatusCode::BAD_REQUEST,
        msg,
        &json!(null),
    ))
}

#[post("/table/{name}/prefix")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
    req_body: web::Json<QueryInput>,
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let tables = app_state.user_tables.read().expect("lock is poisoned");

    let table_name = path.into_inner();

    if !is_valid_identifier(&req_body.row_key) {
        return bad_request(before, "Invalid row key");
    }

    if let Some(table) = tables.get(&table_name) {
        let rows = table.query(&req_body)?;

        let micros_per_item = if rows.0.is_empty() {
            None
        } else {
            Some(before.elapsed().as_micros() / rows.0.len() as u128)
        };

        Ok(build_response(
            before,
            StatusCode::OK,
            "Query successful",
            &json!({
                "micros_per_item": micros_per_item,
                "rows": rows
            }),
        ))
    } else {
        Ok(build_response(
            before,
            StatusCode::CONFLICT,
            "Table not found",
            &json!(null),
        ))
    }
}
