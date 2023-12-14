use crate::app_state::AppState;
use crate::env::data_folder;
use crate::error::CustomRouteResult;
use crate::identifier::is_valid_identifier;
use crate::response::build_response;
use actix_web::http::StatusCode;
use actix_web::{
    delete,
    web::{self, Path},
    HttpResponse,
};
use serde_json::json;
use std::fs::remove_dir_all;

#[delete("/v1/table/{name}")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let mut tables = app_state.tables.write().await;

    let table_name = path.into_inner();

    if table_name.starts_with('_') {
        return Ok(build_response(
            before.elapsed(),
            StatusCode::BAD_REQUEST,
            "Invalid table name",
            &json!(null),
        ));
    }

    if !is_valid_identifier(&table_name) {
        return Ok(build_response(
            before.elapsed(),
            StatusCode::BAD_REQUEST,
            "Invalid table name",
            &json!(null),
        ));
    }

    if tables.get(&table_name).is_some() {
        app_state.manifest_table.delete_user_table(&table_name)?;
        remove_dir_all(data_folder().join("tables").join(&table_name))?;
        tables.remove(&table_name);

        let micros = before.elapsed().as_micros();

        Ok(build_response(
            before.elapsed(),
            StatusCode::ACCEPTED,
            "Deletion completed successfully",
            &json!({
                "micros": micros
            }),
        ))
    } else {
        Ok(build_response(
            before.elapsed(),
            StatusCode::CONFLICT,
            "Table not found",
            &json!(null),
        ))
    }
}
