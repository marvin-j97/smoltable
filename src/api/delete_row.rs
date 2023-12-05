use crate::app_state::AppState;
use crate::error::CustomRouteResult;
use crate::identifier::is_valid_identifier;
use crate::response::build_response;
use actix_web::http::StatusCode;
use actix_web::{
    delete,
    web::{self, Path},
    HttpResponse,
};
use serde::Deserialize;
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

#[derive(Debug, Deserialize)]
pub struct Input {
    row_key: String,
}

#[delete("/table/{name}/row")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
    req_body: web::Json<Input>,
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let tables = app_state.user_tables.read().expect("lock is poisoned");

    let table_name = path.into_inner();

    if !is_valid_identifier(&req_body.row_key) {
        return bad_request(before, "Invalid row key");
    }

    if let Some(table) = tables.get(&table_name) {
        let count = table.delete_row(&req_body.row_key)?;

        let micros_per_item = if count == 0 {
            None
        } else {
            Some(before.elapsed().as_micros() / count as u128)
        };

        Ok(build_response(
            before,
            StatusCode::ACCEPTED,
            "Deletion completed successfully",
            &json!({
                "micros_per_item": micros_per_item,
                "deleted_cells_count": count
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
