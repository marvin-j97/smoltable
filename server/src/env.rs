use std::path::PathBuf;

pub fn data_folder() -> PathBuf {
    let data_folder = std::env::var("SMOLTABLE_DATA").unwrap_or(".smoltable_data".into());
    PathBuf::from(&data_folder)
}

pub fn get_port() -> u16 {
    let port = std::env::var("PORT")
        .or_else(|_| std::env::var("SMOLTABLE_PORT"))
        .unwrap_or("9876".into());

    port.parse::<u16>().expect("invalid port")
}

pub fn metrics_cap_mb() -> u16 {
    let port = std::env::var("SMOLTABLE_METRICS_CAP_MB").unwrap_or("10".into());

    port.parse::<u16>()
        .expect("invalid metrics cap MB setting, can be up to 65536")
}
