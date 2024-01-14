use crate::{column_key::ColumnKey, ColumnFilter};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum Value {
    String(String),
    Boolean(bool),
    Byte(u8),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
}

#[derive(Clone, Debug)]
pub struct VisitedCell {
    pub raw_key: Arc<[u8]>,
    pub row_key: String,
    pub column_key: ColumnKey,
    pub timestamp: u128,
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

// TODO: move to server?
/// User-facing cell
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Cell {
    pub timestamp: u128,
    pub value: Value,
}
