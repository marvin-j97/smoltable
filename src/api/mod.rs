pub mod create_column_family;
pub mod create_table;
pub mod ingest;
pub mod list_tables;
pub mod system;

pub fn format_server_header() -> String {
    format!("{} {}", env!("CARGO_CRATE_NAME"), env!("CARGO_PKG_VERSION"))
}
