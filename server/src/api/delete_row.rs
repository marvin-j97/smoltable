use crate::app_state::AppState;
use crate::data_point;
use crate::error::CustomRouteResult;
use crate::identifier::is_valid_table_identifier;
use crate::response::build_response;
use actix_web::http::StatusCode;
use actix_web::{
    delete,
    web::{self, Path},
    HttpResponse,
};
use serde::Deserialize;
use serde_json::json;
use smoltable::{ColumnFilter, TableWriter};

#[derive(Debug, Deserialize)]
pub struct Input {
    row_key: String,
    column_filter: Option<ColumnFilter>,
}

// TODO: change input format to Vec, atomic multi-row deletes...?

#[delete("/v1/table/{name}/row")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
    req_body: web::Json<Input>,
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

    let req_body = req_body.into_inner();

    let tables = app_state.tables.read().await;

    if let Some(table) = tables.get(&table_name) {
        let count = {
            let table = table.clone();

            tokio::task::spawn_blocking(move || {
                table.delete_row(req_body.row_key, req_body.column_filter)
            })
            .await
            .expect("should join")
        }?;

        let micros_total = before.elapsed().as_micros();

        let micros_per_item = if count == 0 {
            None
        } else {
            Some(micros_total / count as u128)
        }
        .unwrap_or_default();

        TableWriter::write_batch(
            table.metrics.clone(),
            &[
                smoltable::row!("lat#del#row", vec![data_point!(micros_total as f64)]),
                smoltable::row!("lat#del#cell", vec![data_point!(micros_per_item as f64)]),
            ],
        )
        .ok();

        Ok(build_response(
            before.elapsed(),
            StatusCode::ACCEPTED,
            "Deletion completed successfully",
            &json!({
                "micros_per_item": micros_per_item,
                "deleted_cells_count": count
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
