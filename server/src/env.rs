use std::path::PathBuf;

const DEFAULT_DATA_FOLDER: &str = ".smoltable_data";
const DEFAULT_HTTP_PORT: &str = "9876";
const DEFAULT_METRICS_CAP_MB: &str = "1";
const DEFAULT_WRITE_BUFFER_SIZE_MB: &str = "64";

/// Gets data folder
pub fn data_folder() -> PathBuf {
    let data_folder = std::env::var("SMOLTABLE_DATA").unwrap_or(DEFAULT_DATA_FOLDER.into());
    PathBuf::from(&data_folder)
}

/// Gets HTTP port
pub fn get_port() -> u16 {
    let port = std::env::var("SMOLTABLE_HTTP_PORT")
        .or_else(|_| std::env::var("SMOLTABLE_PORT"))
        .or_else(|_| std::env::var("HTTP_PORT"))
        .or_else(|_| std::env::var("PORT"))
        .unwrap_or(DEFAULT_HTTP_PORT.into());

    port.parse::<u16>().expect("invalid port")
}

/// Metrics data cap *per metrics table*
pub fn metrics_cap_mb() -> u16 {
    let port = std::env::var("SMOLTABLE_METRICS_CAP_MB").unwrap_or(DEFAULT_METRICS_CAP_MB.into());

    port.parse::<u16>()
        .expect("invalid metrics cap MB setting, can be up to 65536")
}

/// Global write buffer size
pub fn write_buffer_size() -> u16 {
    let port = std::env::var("SMOLTABLE_WRITE_BUFFER_SIZE_MB")
        .unwrap_or(DEFAULT_WRITE_BUFFER_SIZE_MB.into());

    port.parse::<u16>().expect("invalid metrics cap MB setting")
}
