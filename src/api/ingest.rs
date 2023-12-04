use super::format_server_header;
use crate::error::CustomRouteResult;
use crate::{app_state::AppState, is_valid_identifier};
use actix_web::{
    post,
    web::{self, Path},
    HttpResponse,
};
use serde::Deserialize;
use serde_json::json;
use std::time::Instant;

#[derive(Debug, Deserialize)]
pub struct InputEntry {
    row_key: String,
    timestamp: Option<u128>,
    column_family: String,
    column_qualifier: Option<String>,
    value: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub struct Input {
    items: Vec<InputEntry>,
}

fn bad_request(before: Instant, msg: &str) -> CustomRouteResult<HttpResponse> {
    let body = json!({
        "status": 400,
        "message": msg,
        "result": null
    });

    let body = serde_json::to_string(&body).expect("should serialize");

    Ok(HttpResponse::BadRequest()
        .append_header(("x-server", format_server_header()))
        .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
        .content_type("application/json; utf-8")
        .body(body))
}

fn timestamp_nano() -> u128 {
    std::time::SystemTime::UNIX_EPOCH
        .elapsed()
        .unwrap()
        .as_nanos()
}

#[post("/table/{name}/ingest")]
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
        let mut batch = table.batch();

        for item in &req_body.items {
            if !is_valid_identifier(&item.row_key)
                || !is_valid_identifier(&item.column_family)
                || !item
                    .column_qualifier
                    .as_deref()
                    .map(is_valid_identifier)
                    .unwrap_or(true)
            {
                return bad_request(before, "Invalid item definition");
            }

            if !app_state
                .manifest_table
                .column_family_exists(&table_name, &item.column_family)?
            {
                return bad_request(before, "Column family does not exist");
            }

            let mut key = format!(
                "r:{}:cf:{}:c:{}:",
                item.row_key,
                item.column_family,
                item.column_qualifier
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| String::from("")),
            )
            .as_bytes()
            .to_vec();

            key.extend_from_slice(&(!item.timestamp.unwrap_or_else(timestamp_nano)).to_be_bytes());

            batch.insert(key, bincode::serialize(&item.value).unwrap());
        }

        batch.commit()?;

        let body = json!({
            "status": 200,
            "message": "Ingestion successful",
            "result": {
              "items": {
                "count": req_body.items.len(),
              }
            }
        });

        let body = serde_json::to_string(&body).expect("should serialize");

        Ok(HttpResponse::Ok()
            .append_header(("x-server", format_server_header()))
            .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
            .content_type("application/json; utf-8")
            .body(body))
    } else {
        let body = json!({
            "status": 409,
            "message": "Table not found",
            "result": null
        });

        let body = serde_json::to_string(&body).expect("should serialize");

        Ok(HttpResponse::Conflict()
            .append_header(("x-server", format_server_header()))
            .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
            .content_type("application/json; utf-8")
            .body(body))
    }
}
