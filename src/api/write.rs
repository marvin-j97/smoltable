use std::ops::Deref;

use super::bad_request;
use crate::app_state::AppState;
use crate::column_key::ColumnKey;
use crate::error::CustomRouteResult;
use crate::identifier::is_valid_identifier;
use crate::response::build_response;
use crate::table::cell::Value as CellValue;
use crate::table::writer::{ColumnWriteItem, RowWriteItem, Writer as TableWriter};
use actix_web::http::StatusCode;
use actix_web::{
    post,
    web::{self, Path},
    HttpResponse,
};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
pub struct Input {
    items: Vec<RowWriteItem>,
}

#[post("/v1/table/{name}/write")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
    req_body: web::Json<Input>,
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    if req_body.items.is_empty() {
        return bad_request(before, "Items array should not be empty");
    }

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

    if !is_valid_identifier(&table_name) {
        return Ok(build_response(
            before.elapsed(),
            StatusCode::BAD_REQUEST,
            "Invalid table name",
            &json!(null),
        ));
    }

    if let Some(table) = tables.get(&table_name) {
        let mut writer = TableWriter::new(table.clone());

        drop(tables);

        for row in &req_body.items {
            // TODO:
            /* for cell in &row.cells {
                if !app_state
                    .manifest_table
                    .column_family_exists(&actual_name, &cell.column_key.family)?
                {
                    return bad_request(before, "Column family does not exist");
                }
            } */

            if let Err(write_error) = writer.write(row) {
                log::error!("Write error: {write_error:#?}");

                return Ok(build_response(
                    before.elapsed(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error",
                    &json!(null),
                ));
            };
        }

        writer.finalize()?;

        let dur = before.elapsed();

        let cell_count = req_body
            .items
            .iter()
            .map(|row| row.cells.len() as u128)
            .sum::<u128>();

        let micros_total = dur.as_micros();

        let micros_per_item = if cell_count == 0 {
            None
        } else {
            Some(micros_total / cell_count)
        };

        TableWriter::write_raw(
            app_state.metrics_table.deref().clone(),
            &RowWriteItem {
                row_key: format!("t#{table_name}"),
                cells: vec![ColumnWriteItem {
                    column_key: ColumnKey::try_from("lat:w").expect("should be column key"),
                    timestamp: None,
                    value: CellValue::F64(micros_total as f64),
                }],
            },
        )
        .ok();

        Ok(build_response(
            dur,
            StatusCode::OK,
            "Data ingestion successful",
            &json!({
                "micros_per_item": micros_per_item,
                "items": {
                    "row_count": req_body.items.len(),
                    "cell_count": cell_count
                }
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
