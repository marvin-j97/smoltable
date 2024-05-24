use crate::app_state::AppState;
use crate::error::CustomRouteResult;
use crate::identifier::is_valid_table_identifier;
use crate::response::build_response;
use actix_web::http::StatusCode;
use actix_web::{
    post,
    web::{self, Path},
    HttpResponse,
};
use serde_json::json;
use smoltable::query::count::Input as CountInput;

#[post("/v1/table/{name}/count")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
    req_body: web::Json<CountInput>,
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let table_name = path.into_inner();

    if table_name.starts_with('_') {
        return Ok(build_response(
            before.elapsed(),
            StatusCode::BAD_REQUEST,
            "Invalid table name",
            &json!(null),
        ));
    }

    if !is_valid_table_identifier(&table_name) {
        return Ok(build_response(
            before.elapsed(),
            StatusCode::BAD_REQUEST,
            "Invalid table name",
            &json!(null),
        ));
    }

    let tables = app_state.tables.read().await;

    if let Some(table) = tables.get(&table_name) {
        let result = {
            let table = table.clone();

            tokio::task::spawn_blocking(move || table.scan_count(req_body.0))
                .await
                .expect("should join")
        }?;

        let dur = before.elapsed();

        let micros_total = dur.as_micros();

        let micros_per_row = if result.row_count == 0 {
            None
        } else {
            Some(micros_total / result.row_count as u128)
        };

        Ok(build_response(
            dur,
            StatusCode::OK,
            "Count successful",
            &json!({
                "row_count": result.row_count,
                "cell_count": result.cell_count,
                "micros": micros_total,
                "micros_per_row": micros_per_row,
                "bytes_scanned": result.bytes_scanned_count,
            }),
        ))
    } else {
        Ok(build_response(
            before.elapsed(),
            StatusCode::NOT_FOUND,
            "Table not found",
            &json!(null),
        ))
    }
}
