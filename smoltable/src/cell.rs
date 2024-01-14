use crate::{column_key::ColumnKey, ColumnFilter};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Cell value
///
/// In Bigtable, a cell value is just an unstructured byte array.
///
/// Smoltable supports various data types for better DX.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum Value {
    /// UTF-8 encoded string
    String(String),

    /// like Byte, but is unmarshalled as boolean
    Boolean(bool),

    /// unsigned integer, 1 byte
    Byte(u8),

    /// signed integer, 4 bytes
    I32(i32),

    /// signed integer, 8 bytes
    I64(i64),

    /// floating point, 4 bytes
    F32(f32),

    /// floating point, 8 bytes
    F64(f64),
}

/// A cell and its meta information
#[derive(Clone, Debug)]
pub struct VisitedCell {
    /// The raw cell key, which is `row_key:cf:cq:!ts`
    pub raw_key: Arc<[u8]>,

    /// User row key
    pub row_key: String,

    /// Column key
    pub column_key: ColumnKey,

    /// Timestamp
    ///
    /// Will be restored in negated form to order by descending
    pub timestamp: u128,

    /// Cell value
    pub value: Value,
}

impl std::fmt::Display for VisitedCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{:?}",
            self.row_key, self.column_key, self.timestamp
        )
    }
}

impl VisitedCell {
    pub fn parse(key: Arc<[u8]>, value: &[u8]) -> VisitedCell {
        let mut buf = [0; std::mem::size_of::<u128>()];
        buf.clone_from_slice(&key[(key.len() - std::mem::size_of::<u128>())..key.len()]);
        let ts = !u128::from_be_bytes(buf);

        // NOTE: + 1 because of : delimiter
        let key_without_ts = &key[0..(key.len() - std::mem::size_of::<u128>() - 1)];
        let mut parsed_key = key_without_ts.rsplitn(3, |&e| e == b':');

        let last = parsed_key.next().unwrap();
        let cq = std::str::from_utf8(last).ok().map(Into::into);

        let last = parsed_key.next().unwrap();
        let cf = std::str::from_utf8(last).unwrap();

        let last = parsed_key.next().unwrap();
        let row_key = std::str::from_utf8(last).unwrap();

        VisitedCell {
            raw_key: key.clone(),
            row_key: row_key.into(),
            timestamp: ts,
            column_key: ColumnKey {
                family: cf.to_owned(),
                qualifier: cq,
            },
            value: bincode::deserialize::<Value>(value).expect("should deserialize"),
        }
    }

    pub fn satisfies_column_filter(&self, filter: &ColumnFilter) -> bool {
        match filter {
            ColumnFilter::Key(key) => {
                if self.column_key.family != key.family {
                    return false;
                }

                if let Some(cq_filter) = &key.qualifier {
                    if self.column_key.qualifier.as_deref().unwrap_or("") != cq_filter {
                        return false;
                    }
                }

                true
            }
            ColumnFilter::Multi(keys) => {
                for key in keys {
                    if self.column_key.family != key.family {
                        continue;
                    }

                    if let Some(cq_filter) = &key.qualifier {
                        if self.column_key.qualifier.as_deref().unwrap_or("") == cq_filter {
                            return true;
                        }
                    } else {
                        return true;
                    }
                }

                false
            }
            ColumnFilter::Prefix(key) => {
                if self.column_key.family != key.family {
                    return false;
                }

                if let Some(cq_filter) = &key.qualifier {
                    if !self
                        .column_key
                        .qualifier
                        .as_deref()
                        .unwrap_or("")
                        .starts_with(cq_filter)
                    {
                        return false;
                    }
                }

                true
            }
        }
    }
}

/// User-facing cell content
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Cell {
    pub timestamp: u128,
    pub value: Value,
}
