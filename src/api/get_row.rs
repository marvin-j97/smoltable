use super::bad_request;
use crate::app_state::AppState;
use crate::column_key::ColumnKey;
use crate::error::CustomRouteResult;
use crate::identifier::is_valid_identifier;
use crate::response::build_response;
use crate::table::writer::{ColumnWriteItem, RowWriteItem, Writer as TableWriter};
use crate::table::{cell::Value as CellValue, QueryInput};
use actix_web::http::StatusCode;
use actix_web::{
    post,
    web::{self, Path},
    HttpResponse,
};
use serde_json::json;

#[post("/v1/table/{name}/get-row")]
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

    if let Some(table) = tables.get(&table_name) {
        let key = match &req_body.column_filter {
            Some(filter) => filter.build_key(&req_body.row_key),
            None => format!("{}:", req_body.row_key),
        };

        let result = table.query(&QueryInput {
            row_key: key,
            column_filter: None,
            row_limit: req_body.row_limit,
            cell_limit: req_body.cell_limit,
        })?;

        let dur = before.elapsed();

        let micros_total = dur.as_micros();

        TableWriter::write_raw(
            &app_state.metrics_table,
            &RowWriteItem {
                row_key: format!("t#{table_name}"),
                cells: vec![ColumnWriteItem {
                    column_key: ColumnKey::try_from("lat:r#row").expect("should be column key"),
                    timestamp: None,
                    value: CellValue::F64(micros_total as f64),
                }],
            },
        )
        .ok();
        app_state.metrics_table.tree.flush().ok();

        Ok(build_response(
            dur,
            StatusCode::OK,
            "Query successful",
            &json!({
                "micros": micros_total,
                "rows_scanned": result.rows_scanned_count,
                "cells_scanned": result.cells_scanned_count,
                "row": result.rows.get(0)
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
