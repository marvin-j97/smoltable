use crate::app_state::AppState;
use crate::column_key::ColumnKey;
use crate::error::CustomRouteResult;
use crate::response::build_response;
use crate::table::writer::{ColumnWriteItem, RowWriteItem, WriteError, Writer as TableWriter};
use crate::table::CellValue;
use actix_web::http::StatusCode;
use actix_web::{
    post,
    web::{self, Path},
    HttpResponse,
};
use serde::Deserialize;
use serde_json::json;
use std::time::Instant;

#[derive(Debug, Deserialize)]
pub struct Input {
    items: Vec<RowWriteItem>,
}

fn bad_request(before: Instant, msg: &str) -> CustomRouteResult<HttpResponse> {
    Ok(build_response(
        before,
        StatusCode::BAD_REQUEST,
        msg,
        &json!(null),
    ))
}

#[post("/v1/table/{name}/write")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
    req_body: web::Json<Input>,
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let tables = app_state.user_tables.read().await;

    let table_name = path.into_inner();

    if req_body.items.is_empty() {
        return bad_request(before, "Items array should not be empty");
    }

    if let Some(table) = tables.get(&table_name) {
        let mut writer =
            TableWriter::new(app_state.manifest_table.clone(), table.clone(), &table_name);

        drop(tables);

        for row in &req_body.items {
            if let Err(write_error) = writer.write(row) {
                use WriteError::{BadInput, Lsm};

                match write_error {
                    BadInput(msg) => return bad_request(before, msg),
                    Lsm(e) => return Err(e.into()),
                }
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
