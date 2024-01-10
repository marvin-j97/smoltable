use crate::app_state::AppState;
use crate::error::CustomRouteResult;
use crate::response::build_response;
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
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let tables = app_state.tables.write().await;

    let table_name = path.into_inner();

    let actual_name = format!("usr_{table_name}");

    if tables.get(&actual_name).is_some() {
        /* let rows = app_state
        .metrics_table
        .query_timeseries(&format!("t#{actual_name}"), None)?; */

        unimplemented!()

        /* Ok(build_response(
            before.elapsed(),
            StatusCode::OK,
            "Metrics query successful",
            &json!(rows),
        )) */
    } else {
        Ok(build_response(
            before.elapsed(),
            StatusCode::CONFLICT,
            "Table not found",
            &json!(null),
        ))
    }
}
