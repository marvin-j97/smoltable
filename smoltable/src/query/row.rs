use crate::{ColumnFilter, Row};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RowOptions {
    pub key: String,
    // TODO: row-wide cell_limit
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ColumnOptions {
    pub cell_limit: Option<u32>,

    // TODO: column limit
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
    pub row: Option<Row>,
    pub cells_scanned_count: u64,
    pub bytes_scanned_count: u64,
}
