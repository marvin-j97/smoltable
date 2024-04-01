use crate::{ColumnFilter, Row};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Range {
    pub start: String,
    pub end: String,
    pub inclusive: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ScanMode {
    #[serde(rename = "prefix")]
    Prefix(String),

    #[serde(rename = "range")]
    Range(Range),
    // TODO:
    /*  #[serde(rename = "ranges")]
    Ranges(Vec<Range>), */
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RowOptions {
    #[serde(flatten)]
    pub scan: ScanMode,

    pub offset: Option<u32>,
    pub limit: Option<u32>,
    pub cell_limit: Option<u32>,
    pub sample: Option<f32>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ColumnOptions {
    pub cell_limit: Option<u32>,

    // TODO: column limit

    // pub start, end: Option<Range>, // TODO: .......
    #[serde(flatten)]
    pub filter: Option<ColumnFilter>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CellOptions {
    pub limit: Option<u32>,
    // pub time: Option<Range>, // TODO:
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Input {
    pub column: Option<ColumnOptions>,
    pub row: RowOptions,
    pub cell: Option<CellOptions>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Output {
    pub rows: Vec<Row>,
    pub affected_locality_groups: usize,
    pub cells_scanned_count: u64,
    pub rows_scanned_count: u64,
    pub bytes_scanned_count: u64,
}
