use crate::error::CustomRouteResult;
use crate::identifier::is_valid_identifier;
use crate::{app_state::AppState, response::build_response};
use actix_web::{
    http::StatusCode,
    put,
    web::{self, Path},
    HttpResponse,
};
use serde_json::json;

#[put("/v1/table/{name}")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let table_name = path.into_inner();

    if !is_valid_identifier(&table_name) {
        return Ok(build_response(
            before,
            StatusCode::BAD_REQUEST,
            "Invalid table name",
            &json!(null),
        ));
    }

    let tables = app_state.user_tables.read().await;
    if tables.contains_key(&table_name) {
        return Ok(build_response(
            before,
            StatusCode::CONFLICT,
            "Conflict",
            &json!(null),
        ));
    }
    drop(tables);

    app_state.create_table(&table_name).await?;

    Ok(build_response(
        before,
        StatusCode::CREATED,
        "Table created successfully",
        &json!(null),
    ))
}
