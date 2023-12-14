use crate::app_state::AppState;
use crate::column_key::ColumnKey;
use crate::error::CustomRouteResult;
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

#[post("/v1/table/{name}/prefix")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
    req_body: web::Json<QueryInput>,
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let tables = app_state.tables.read().await;

    let table_name = path.into_inner();

    if let Some(table) = tables.get(&table_name) {
        let result = table.query(&req_body)?;

        let micros_total = before.elapsed().as_micros();

        let micros_per_row = if result.rows.is_empty() {
            None
        } else {
            Some(micros_total / result.rows.len() as u128)
        };

        TableWriter::write_raw(
            &app_state.metrics_table,
            &RowWriteItem {
                row_key: format!("t#{table_name}"),
                cells: vec![ColumnWriteItem {
                    column_key: ColumnKey::try_from("lat:r#pfx").expect("should be column key"),
                    timestamp: None,
                    value: CellValue::U128(micros_total),
                }],
            },
        )
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "IO error"))?;

        Ok(build_response(
            before,
            StatusCode::OK,
            "Query successful",
            &json!({
                "micros_per_row": micros_total,
                "micros_per_row": micros_per_row,
                "rows_scanned": result.rows_scanned_count,
                "cells_scanned": result.cells_scanned_count,
                "rows": result.rows
            }),
        ))
    } else {
        Ok(build_response(
            before,
            StatusCode::CONFLICT,
            "Table not found",
            &json!(null),
        ))
    }
}
