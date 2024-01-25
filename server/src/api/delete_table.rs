use crate::app_state::AppState;
use crate::error::CustomRouteResult;
use crate::identifier::is_valid_table_identifier;
use crate::response::build_response;
use actix_web::http::StatusCode;
use actix_web::{
    delete,
    web::{self, Path},
    HttpResponse,
};
use serde_json::json;

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

    if !is_valid_table_identifier(&table_name) {
        return Ok(build_response(
            before.elapsed(),
            StatusCode::BAD_REQUEST,
            "Invalid table name",
            &json!(null),
        ));
    }

    if let Some(table) = tables.get(&table_name).cloned() {
        app_state.manifest_table.delete_user_table(&table_name)?;
        tables.remove(&table_name);

        app_state
            .keyspace
            .delete_partition(table.manifest.clone())
            .map_err(smoltable::Error::Storage)?;

        app_state
            .keyspace
            .delete_partition(table.metrics.manifest.clone())
            .map_err(smoltable::Error::Storage)?;

        app_state
            .keyspace
            .delete_partition(table.metrics.tree.clone())
            .map_err(smoltable::Error::Storage)?;

        for locality_group in &*table.locality_groups.read().expect("lock is poisoned") {
            app_state
                .keyspace
                .delete_partition(locality_group.tree.clone())
                .map_err(smoltable::Error::Storage)?;
        }

        app_state
            .keyspace
            .delete_partition(table.tree.clone())
            .map_err(smoltable::Error::Storage)?;

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
            StatusCode::NOT_FOUND,
            "Table not found",
            &json!(null),
        ))
    }
}
