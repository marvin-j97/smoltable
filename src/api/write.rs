use crate::app_state::AppState;
use crate::error::CustomRouteResult;
use crate::response::build_response;
use crate::table::writer::{RowWriteItem, WriteError, Writer as TableWriter};
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

#[post("/table/{name}/write")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
    req_body: web::Json<Input>,
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let tables = app_state.user_tables.write().expect("lock is poisoned");

    let table_name = path.into_inner();

    if req_body.items.is_empty() {
        return bad_request(before, "Items array should not be empty");
    }

    if let Some(table) = tables.get(&table_name) {
        let mut writer =
            TableWriter::new(app_state.manifest_table.clone(), table.clone(), table_name);

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

        let micros_per_item = if cell_count == 0 {
            None
        } else {
            Some(before.elapsed().as_micros() / cell_count)
        };

        Ok(build_response(
            before,
            StatusCode::OK,
            "Data ingestion successful",
            &json!({
                "micros_per_item": micros_per_item,
                "items": {
                    "count": req_body.items.len(),
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
