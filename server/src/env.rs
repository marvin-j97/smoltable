use std::path::PathBuf;

/// Gets data folder
pub fn data_folder() -> PathBuf {
    let data_folder = std::env::var("SMOLTABLE_DATA").unwrap_or(".smoltable_data".into());
    PathBuf::from(&data_folder)
}

/// Gets HTTP port
pub fn get_port() -> u16 {
    let port = std::env::var("SMOLTABLE_HTTP_PORT")
        .or_else(|_| std::env::var("SMOLTABLE_PORT"))
        .or_else(|_| std::env::var("PORT"))
        .unwrap_or("9876".into());

    port.parse::<u16>().expect("invalid port")
}

/// Metrics data cap *per metrics table*
pub fn metrics_cap_mb() -> u16 {
    let port = std::env::var("SMOLTABLE_METRICS_CAP_MB").unwrap_or("1".into());

    port.parse::<u16>()
        .expect("invalid metrics cap MB setting, can be up to 65536")
}

/// Global write buffer size
pub fn write_buffer_size() -> u32 {
    let port = std::env::var("SMOLTABLE_WRITE_BUFFER_SIZE").unwrap_or("67108864".into());

    port.parse::<u32>().expect("invalid metrics cap MB setting")
}
