use crate::error::CustomRouteResult;
use crate::identifier::is_valid_table_identifier;
use crate::{app_state::AppState, response::build_response};
use actix_web::{
    http::StatusCode,
    put,
    web::{self, Path},
    HttpResponse,
};
use serde_json::json;

// TODO: allow setting dedicated cache size

#[put("/v1/table/{name}")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
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
    if tables.contains_key(&table_name) {
        return Ok(build_response(
            before.elapsed(),
            StatusCode::CONFLICT,
            "Conflict",
            &json!(null),
        ));
    }
    drop(tables);

    app_state.create_table(&table_name).await?;

    Ok(build_response(
        before.elapsed(),
        StatusCode::CREATED,
        "Table created successfully",
        &json!(null),
    ))
}
