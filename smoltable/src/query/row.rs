use crate::{ColumnFilter, Row};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RowOptions {
    pub key: String,
    pub cell_limit: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ColumnOptions {
    pub cell_limit: Option<u32>,

    // pub start, end: Option<Range>, // TODO: .......

    // TODO: column limit
    #[serde(flatten)]
    pub filter: Option<ColumnFilter>,
}

// TODO:
/* #[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CellOptions {
    // pub time: Option<Range>, // TODO:
} */

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Input {
    pub row: RowOptions,
    pub column: Option<ColumnOptions>,
    // pub cell: Option<CellOptions>, // TODO:
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Output {
    pub row: Option<Row>,
    pub cells_scanned_count: u64,
    pub bytes_scanned_count: u64,
}
