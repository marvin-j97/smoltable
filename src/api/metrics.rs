use super::bad_request;
use crate::app_state::AppState;
use crate::error::CustomRouteResult;
use crate::identifier::is_valid_identifier;
use crate::response::build_response;
use crate::table::QueryInput;
use actix_web::http::StatusCode;
use actix_web::{
    get,
    web::{self, Path},
    HttpResponse,
};
use serde_json::json;

#[get("/v1/table/{name}/metrics")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
    req_body: web::Json<QueryInput>,
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let tables = app_state.tables.write().await;

    let table_name = path.into_inner();

    if !is_valid_identifier(&req_body.row_key) {
        return bad_request(before, "Invalid row key");
    }

    if tables.get(&table_name).is_some() {
        let rows = app_state
            .metrics_table
            .query_timeseries(&format!("t#{table_name}"), None)?;

        Ok(build_response(
            before,
            StatusCode::OK,
            "Metrics query successful",
            &json!(rows),
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
