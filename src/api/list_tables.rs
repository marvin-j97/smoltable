use super::format_server_header;
use crate::error::CustomRouteResult;
use crate::{app_state::AppState, manifest::ColumnFamilyDefinition};
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
    cache_stats: CacheStats,
}

#[get("/table")]
pub async fn handler(app_state: web::Data<AppState>) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let tables = app_state.user_tables.read().expect("lock is poisoned");

    let tables = tables
        .iter()
        .map(|(table_name, table)| {
            Ok(TableListEntry {
                name: table_name.clone(),
                column_families: app_state
                    .manifest_table
                    .get_user_table_column_families(table_name)?,
                disk_space_in_bytes: table.disk_space_usage(),
                cache_stats: CacheStats {
                    block_count: table.cached_block_count(),
                    memory_usage_in_bytes: table.cache_memory_usage(),
                },
            })
        })
        .collect::<lsm_tree::Result<Vec<_>>>()?;

    let body = json!({
        "status": 200,
        "message": "Tables retrieved successfully",
        "result": {
            "tables": {
                "count": tables.len(),
                "items": tables
            }
        }
    });
    let body = serde_json::to_string(&body).expect("should serialize");

    Ok(HttpResponse::Ok()
        .append_header(("x-server", format_server_header()))
        .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
        .content_type("application/json; utf-8")
        .body(body))
}
