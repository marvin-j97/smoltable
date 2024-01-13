use crate::app_state::AppState;
use crate::column_key::ColumnKey;
use crate::error::CustomRouteResult;
use crate::identifier::is_valid_identifier;
use crate::response::build_response;
use crate::table::cell::Value as CellValue;
use crate::table::writer::{ColumnWriteItem, RowWriteItem, Writer as TableWriter};
use actix_web::http::StatusCode;
use actix_web::{
    delete,
    web::{self, Path},
    HttpResponse,
};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
pub struct Input {
    row_key: String,
    // column_filter: Option<ColumnKey>,
}

// TODO: change input format to Vec, atomic multi-row deletes...?

#[delete("/v1/table/{name}/rows")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
    req_body: web::Json<Input>,
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

    if !is_valid_identifier(&table_name) {
        return Ok(build_response(
            before.elapsed(),
            StatusCode::BAD_REQUEST,
            "Invalid table name",
            &json!(null),
        ));
    }

    if let Some(table) = tables.get(&table_name) {
        let count = table.delete_row(req_body.row_key.clone())?;

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
                RowWriteItem {
                    row_key: "lat#del#row".to_string(),
                    cells: vec![ColumnWriteItem {
                        column_key: ColumnKey::try_from("value").expect("should be column key"),
                        timestamp: None,
                        value: CellValue::F64(micros_total as f64),
                    }],
                },
                RowWriteItem {
                    row_key: "lat#del#cell".to_string(),
                    cells: vec![ColumnWriteItem {
                        column_key: ColumnKey::try_from("value").expect("should be column key"),
                        timestamp: None,
                        value: CellValue::F64(micros_per_item as f64),
                    }],
                },
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
            StatusCode::CONFLICT,
            "Table not found",
            &json!(null),
        ))
    }
}
