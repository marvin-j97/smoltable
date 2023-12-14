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

    let tables = app_state.tables.read().await;

    let table_name = path.into_inner();

    if req_body.items.is_empty() {
        return bad_request(before, "Items array should not be empty");
    }

    if let Some(table) = tables.get(&table_name) {
        let mut writer = TableWriter::new(table);

        drop(tables);

        for row in &req_body.items {
            if !is_valid_identifier(&row.row_key) {
                return bad_request(before, "Invalid row key");
            }

            for cell in &row.cells {
                if !app_state
                    .manifest_table
                    .column_family_exists(&table_name, &cell.column_key.family)?
                {
                    return bad_request(before, "Column family does not exist");
                }
            }

            if let Err(write_error) = writer.write(row) {
                log::error!("Write error: {write_error:#?}");

                return Ok(build_response(
                    before,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error",
                    &json!(null),
                ));
            };
        }

        writer.finalize()?;

        let cell_count = req_body
            .items
            .iter()
            .map(|row| row.cells.len() as u128)
            .sum::<u128>();

        let micros_total = before.elapsed().as_micros();

        let micros_per_item = if cell_count == 0 {
            None
        } else {
            Some(micros_total / cell_count)
        };

        TableWriter::write_raw(
            &app_state.metrics_table,
            &RowWriteItem {
                row_key: format!("t#{table_name}"),
                cells: vec![ColumnWriteItem {
                    column_key: ColumnKey::try_from("lat:w").expect("should be column key"),
                    timestamp: None,
                    value: CellValue::U128(micros_total),
                }],
            },
        )
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "IO error"))?;

        Ok(build_response(
            before,
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
            before,
            StatusCode::CONFLICT,
            "Table not found",
            &json!(null),
        ))
    }
}
