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
use serde::{Deserialize, Serialize};
use serde_json::json;
use smoltable::{query::row::Input as QueryRowInput, TableWriter};

#[derive(Debug, Deserialize, Serialize)]
struct Input {
    items: Vec<QueryRowInput>,
}

#[post("/v1/table/{name}/rows")]
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

    let tables = app_state.tables.read().await;

    if let Some(table) = tables.get(&table_name) {
        let result = {
            let table = table.clone();

            tokio::task::spawn_blocking(move || table.multi_get(req_body.items.clone()))
                .await
                .expect("should join")
        }?;

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
                "lat#read#row",
                vec![data_point!(micros_per_row.unwrap_or_default() as f64)]
            )],
        )
        .ok();

        Ok(build_response(
            dur,
            StatusCode::OK,
            "Query successful",
            &json!({
                "affected_locality_groups": result.affected_locality_groups,
                "micros": micros_total,
                "micros_per_row": micros_per_row,
                "rows_scanned": result.rows_scanned_count,
                "cells_scanned": result.cells_scanned_count,
                "bytes_scanned": result.bytes_scanned_count,
                "rows": result.rows
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
