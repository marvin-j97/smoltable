use crate::ColumnKey;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ColumnFilter {
    #[serde(rename = "key")]
    Key(ColumnKey),

    #[serde(rename = "multi_key")]
    Multi(Vec<ColumnKey>),

    #[serde(rename = "prefix")]
    Prefix(ColumnKey),
}
