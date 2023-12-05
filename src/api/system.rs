use crate::{error::CustomRouteResult, response::build_response};
use actix_web::{get, http::StatusCode, HttpResponse};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::Path;
use sysinfo::{CpuExt, System, SystemExt};

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

#[get("/system")]
pub async fn handler() -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let sysinfo = System::new_all();

    let data_folder = crate::data_folder();
    let data_folder = Path::new(&data_folder);

    let data_folder_size = fs_extra::dir::get_size(data_folder)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "fs_extra error"))?;

    let info = SystemInfo {
        os_name: sysinfo.long_os_version().unwrap_or("Unknown OS".into()),
        cpu_name: sysinfo.global_cpu_info().brand().into(),
        memory_size_in_bytes: sysinfo.available_memory(),
    };

    let stats = SystemStats {
        cpu_usage_percent: sysinfo.load_average().one,
        memory_used_in_bytes: sysinfo.used_memory(),
        database_size_in_bytes: data_folder_size,
    };

    Ok(build_response(
        before,
        StatusCode::OK,
        "System info retrieved successfully",
        &json!({
            "system": {
              "info": info,
              "stats": stats
            }
        }),
    ))
}
