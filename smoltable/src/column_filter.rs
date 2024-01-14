use crate::ColumnKey;
use serde::{Deserialize, Serialize};

/// The column filter allows querying specific columns or column families
///
/// If possible, the column filter will be used
/// to minimize the amount of locality groups that
/// need to be visited to retrieve the data.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ColumnFilter {
    #[serde(rename = "key")]
    Key(ColumnKey),

    #[serde(rename = "multi_key")]
    Multi(Vec<ColumnKey>),

    #[serde(rename = "prefix")]
    Prefix(ColumnKey),
}
