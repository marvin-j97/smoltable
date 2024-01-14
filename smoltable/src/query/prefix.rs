use crate::{ColumnFilter, Row};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RowOptions {
    pub limit: Option<u32>,
    pub cell_limit: Option<u32>,
    pub sample: Option<f32>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ColumnOptions {
    pub cell_limit: Option<u32>,

    // TODO: column limit
    #[serde(flatten)]
    pub filter: Option<ColumnFilter>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CellOptions {
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Input {
    pub prefix: String, // TODO: should be row.prefix
    pub column: Option<ColumnOptions>,
    pub row: Option<RowOptions>,
    pub cell: Option<CellOptions>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Output {
    pub rows: Vec<Row>,
    pub cells_scanned_count: u64,
    pub rows_scanned_count: u64,
    pub bytes_scanned_count: u64,
}
