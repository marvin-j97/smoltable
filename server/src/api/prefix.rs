use crate::app_state::AppState;
use crate::data_point;
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
use smoltable::{query::prefix::Input as QueryPrefixInput, TableWriter};

#[post("/v1/table/{name}/prefix")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
    req_body: web::Json<QueryPrefixInput>,
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let tables = app_state.tables.read().await;

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

    if let Some(table) = tables.get(&table_name) {
        let result = table.query_prefix(req_body.0)?;

        let dur = before.elapsed();

        let micros_total = dur.as_micros();

        let micros_per_row = if result.rows.is_empty() {
            None
        } else {
            Some(micros_total / result.rows.len() as u128)
        };

        TableWriter::write_batch(
            table.metrics.clone(),
            &[smoltable::row!(
                "lat#read#pfx",
                vec![data_point!(micros_total as f64)]
            )],
        )
        .ok();

        let cell_count = result
            .rows
            .iter()
            .map(|x| x.columns.values().map(|x| x.len()).sum::<usize>())
            .sum::<usize>();

        Ok(build_response(
            dur,
            StatusCode::OK,
            "Query successful",
            &json!({
                "micros_per_row": micros_total,
                "micros_per_row": micros_per_row,
                "rows_scanned": result.rows_scanned_count,
                "cells_scanned": result.cells_scanned_count,
                "bytes_scanned": result.bytes_scanned_count,
                "row_count": result.rows.len(),
                "cell_count": cell_count,
                "rows": result.rows
            }),
        ))
    } else {
        let dur = before.elapsed();

        Ok(build_response(
            dur,
            StatusCode::CONFLICT,
            "Table not found",
            &json!(null),
        ))
    }
}
