pub mod create_column_family;
pub mod create_table;
pub mod delete_row;
pub mod get_row;
pub mod list_tables;
pub mod prefix;
pub mod system;
pub mod write;

pub fn format_server_header() -> String {
    format!("{} {}", env!("CARGO_CRATE_NAME"), env!("CARGO_PKG_VERSION"))
}
