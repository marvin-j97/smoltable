use crate::error::CustomRouteResult;
use crate::response::build_response;
use crate::{app_state::AppState, manifest::ColumnFamilyDefinition};
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
    cache_stats: CacheStats,
}

#[get("/v1/table")]
pub async fn handler(app_state: web::Data<AppState>) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let tables = app_state.tables.read().await;

    let tables = tables
        .iter()
        .map(|(table_name, table)| {
            Ok(TableListEntry {
                name: table_name.clone(),
                column_families: app_state
                    .manifest_table
                    .get_user_table_column_families(table_name)?,
                disk_space_in_bytes: table.disk_space_usage()?,
                cache_stats: CacheStats {
                    block_count: table.cached_block_count(),
                    memory_usage_in_bytes: table.cache_memory_usage(),
                },
            })
        })
        .collect::<lsm_tree::Result<Vec<_>>>()?;

    Ok(build_response(
        before,
        StatusCode::OK,
        "Tables retrieved successfully",
        &json!({
            "tables": {
                "count": tables.len(),
                "items": tables
            }
        }),
    ))
}
