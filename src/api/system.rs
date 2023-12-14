use crate::{app_state::AppState, error::CustomRouteResult, response::build_response};
use actix_web::{get, http::StatusCode, web, HttpResponse};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Deserialize, Serialize)]
struct SystemStats {
    cpu_usage_percent: f64,
    memory_used_in_bytes: u64,
    database_size_in_bytes: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct SystemInfo {
    os_name: String,
    cpu_name: String,
    memory_size_in_bytes: u64,
}

#[get("/v1/system")]
pub async fn handler(app_state: web::Data<AppState>) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let system_metrics = app_state.metrics_table.query_timeseries("sys", None)?;

    Ok(build_response(
        before.elapsed(),
        StatusCode::OK,
        "System info retrieved successfully",
        &json!({
            "system": {
                "metrics": system_metrics,
            }
        }),
    ))
}
