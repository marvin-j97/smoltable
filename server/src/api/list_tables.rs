use crate::app_state::AppState;
use crate::error::CustomRouteResult;
use crate::response::build_response;
use actix_web::http::StatusCode;
use actix_web::{get, web, HttpResponse};
use serde::{Deserialize, Serialize};
use serde_json::json;
use smoltable::{ColumnFamilyDefinition, BLOCK_SIZE};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Deserialize, Serialize)]
struct CacheStats {
    block_count: usize,
    memory_usage_in_bytes: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct LocalityGroupListEntry {
    id: Arc<str>,
    column_families: Vec<Arc<str>>,
}

#[derive(Debug, Deserialize, Serialize)]
struct PartitionListEntry {
    name: Arc<str>,
    path: PathBuf,
}

#[derive(Debug, Deserialize, Serialize)]
struct TableListEntry {
    name: String,
    column_families: Vec<ColumnFamilyDefinition>,
    disk_space_in_bytes: u64,
    locality_groups: Vec<LocalityGroupListEntry>,
    partitions: Vec<PartitionListEntry>,
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
                    .map(|x| LocalityGroupListEntry {
                        id: x.id.clone(),
                        column_families: x.column_families.clone(),
                    })
                    .collect::<Vec<_>>(),
                disk_space_in_bytes: table.disk_space_usage(),
                partitions: {
                    let mut v = vec![
                        PartitionListEntry {
                            name: table.manifest.name.clone(),
                            path: table.manifest.path(),
                        },
                        PartitionListEntry {
                            name: table.tree.name.clone(),
                            path: table.tree.path(),
                        },
                    ];

                    v.extend(
                        table
                            .locality_groups
                            .read()
                            .expect("lock is poisoned")
                            .iter()
                            .map(|x| PartitionListEntry {
                                name: x.tree.name.clone(),
                                path: x.tree.path(),
                            }),
                    );

                    v
                },
            })
        })
        .collect::<smoltable::Result<Vec<_>>>()?;

    let cached_block_count = app_state.block_cache.len();

    let cache_stats = CacheStats {
        block_count: cached_block_count,
        memory_usage_in_bytes: cached_block_count as u64 * u64::from(BLOCK_SIZE),
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
