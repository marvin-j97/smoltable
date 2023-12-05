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

#[post("/table/{name}/get-row")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
    mut req_body: web::Json<QueryInput>,
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let tables = app_state.user_tables.write().expect("lock is poisoned");

    let table_name = path.into_inner();

    if !is_valid_identifier(&req_body.row_key) {
        return bad_request(before, "Invalid row key");
    }

    if let Some(table) = tables.get(&table_name) {
        let rows = table.query(&req_body)?;

        let mut key = req_body.row_key.clone();

        if let Some(column_filter) = &req_body.column_filter {
            let cf = &column_filter.family;

            key.push_str(&format!(":cf:{cf}"));

            if let Some(cq) = &column_filter.qualifier {
                key.push_str(&format!(":c:{cq}"));
            }
        }

        // NOTE: Don't do filtering, we already built the query key correctly
        req_body.column_filter = None;

        Ok(build_response(
            before,
            StatusCode::OK,
            "Query successful",
            &json!({
                "result": {
                    "micros": before.elapsed().as_micros(),
                    "row": rows.0.get(0)
                }
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
