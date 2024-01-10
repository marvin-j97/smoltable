use std::sync::Arc;

use crate::app_state::AppState;
use crate::error::CustomRouteResult;
use crate::response::build_response;
use crate::table::ColumnFamilyDefinition;
use actix_web::http::StatusCode;
use actix_web::{get, web, HttpResponse};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Deserialize, Serialize)]
struct CacheStats {
    block_count: usize,
    memory_usage_in_bytes: usize,
}

#[derive(Debug, Deserialize, Serialize)]
struct TableListEntry {
    name: String,
    column_families: Vec<ColumnFamilyDefinition>,
    disk_space_in_bytes: u64,
    locality_groups: Vec<Vec<Arc<str>>>,
}

#[get("/v1/table")]
pub async fn handler(app_state: web::Data<AppState>) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let tables = app_state.tables.read().await;

    let table_data = tables
        .iter()
        .map(|(table_name, table)| {
            Ok(TableListEntry {
                name: table_name.clone(),
                column_families: table.list_column_families()?,
                locality_groups: table
                    .locality_groups
                    .read()
                    .expect("lock is poisoned")
                    .iter()
                    .map(|x| x.column_families.clone())
                    .collect::<Vec<_>>(),
                disk_space_in_bytes: table.disk_space_usage(),
            })
        })
        .collect::<fjall::Result<Vec<_>>>()?;

    let metrics_table = tables.get("_metrics").expect("should exist");

    let cache_stats = CacheStats {
        block_count: metrics_table.cached_block_count(),
        memory_usage_in_bytes: metrics_table.cache_memory_usage(),
    };

    Ok(build_response(
        before.elapsed(),
        StatusCode::OK,
        "Tables retrieved successfully",
        &json!({
            "tables": {
                "count": table_data.len(),
                "items": table_data,
            },
            "cache_stats": cache_stats,
        }),
    ))
}
