use super::scan::ScanMode;
use crate::ColumnFilter;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RowOptions {
    #[serde(flatten)]
    pub scan: ScanMode,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ColumnOptions {
    #[serde(flatten)]
    pub filter: Option<ColumnFilter>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Input {
    pub row: RowOptions,
    pub column: Option<ColumnOptions>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Output {
    pub affected_locality_groups: usize,
    pub cell_count: u64,
    pub row_count: u64,
    pub bytes_scanned_count: u64,
}
