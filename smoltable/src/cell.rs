use crate::{column_key::ColumnKey, ColumnFilter};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Cell value
///
/// In Bigtable, a cell value is just an unstructured byte array.
///
/// Smoltable supports various data types for better DX.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type", content = "value")]
pub enum Value {
    #[serde(rename = "string")]
    /// UTF-8 encoded string
    String(String),

    #[serde(rename = "boolean")]
    /// like Byte, but is unmarshalled as boolean
    Boolean(bool),

    #[serde(rename = "byte")]
    /// unsigned integer, 1 byte
    Byte(u8),

    #[serde(rename = "i32")]
    /// signed integer, 4 bytes
    I32(i32),

    #[serde(rename = "i64")]
    /// signed integer, 8 bytes
    I64(i64),

    #[serde(rename = "f32")]
    /// floating point, 4 bytes
    F32(f32),

    #[serde(rename = "f64")]
    /// floating point, 8 bytes
    F64(f64),
}

impl Value {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::String(s) => {
                let mut bytes = vec![0u8; 1 + s.len()];
                bytes[0] = 0;
                bytes[1..].copy_from_slice(s.as_bytes());
                bytes
            }
            Value::Boolean(b) => vec![1, *b as u8],
            Value::Byte(byte) => vec![2, *byte],
            Value::I32(i) => {
                let bytes: [u8; 4] = i.to_be_bytes();
                vec![3, bytes[0], bytes[1], bytes[2], bytes[3]]
            }
            Value::I64(i) => {
                let bytes: [u8; 8] = i.to_be_bytes();
                vec![
                    4, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                    bytes[7],
                ]
            }
            Value::F32(f) => {
                let bytes: [u8; 4] = f.to_be_bytes();
                vec![5, bytes[0], bytes[1], bytes[2], bytes[3]]
            }
            Value::F64(f) => {
                let bytes: [u8; 8] = f.to_be_bytes();
                vec![
                    6, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                    bytes[7],
                ]
            }
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }

        match bytes[0] {
            0 => {
                let string_len = bytes.len() - 1;
                let string_bytes = &bytes[1..];

                if string_len > 0 {
                    let string =
                        String::from_utf8(string_bytes.to_vec()).expect("should be utf-8 string");

                    Some(Value::String(string))
                } else {
                    Some(Value::String(String::new()))
                }
            }
            1 => Some(Value::Boolean(bytes[1] != 0)),
            2 => Some(Value::Byte(bytes[1])),
            3 => {
                if bytes.len() < 5 {
                    None
                } else {
                    let i = i32::from_be_bytes(bytes[1..5].try_into().expect("should deserialize"));
                    Some(Value::I32(i))
                }
            }
            4 => {
                if bytes.len() < 9 {
                    None
                } else {
                    let i = i64::from_be_bytes(bytes[1..9].try_into().expect("should deserialize"));
                    Some(Value::I64(i))
                }
            }
            5 => {
                if bytes.len() < 5 {
                    None
                } else {
                    let f = f32::from_be_bytes(bytes[1..5].try_into().expect("should deserialize"));
                    Some(Value::F32(f))
                }
            }
            6 => {
                if bytes.len() < 9 {
                    None
                } else {
                    let f = f64::from_be_bytes(bytes[1..9].try_into().expect("should deserialize"));
                    Some(Value::F64(f))
                }
            }
            _ => None,
        }
    }
}

/// A cell and its meta information visited by an iterator
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
    pub fn format_key(row_key: &str, column_key: &ColumnKey, timestamp: u128) -> Vec<u8> {
        let mut key = format!(
            "{}:{}:{}:",
            row_key,
            column_key.family,
            column_key.qualifier.as_ref().cloned().unwrap_or_default(),
        )
        .as_bytes()
        .to_vec();

        // NOTE: Reverse the timestamp to store it in descending order
        key.extend_from_slice(&(!timestamp).to_be_bytes());

        key
    }

    pub fn parse(key: Arc<[u8]>, value: &[u8]) -> VisitedCell {
        let mut buf = [0; std::mem::size_of::<u128>()];
        buf.clone_from_slice(&key[(key.len() - std::mem::size_of::<u128>())..key.len()]);
        let ts = !u128::from_be_bytes(buf);

        // NOTE: -1 because of : delimiter
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
            row_key: row_key.to_owned(),
            timestamp: ts,
            column_key: ColumnKey {
                family: cf.to_owned(),
                qualifier: cq,
            },
            value: Value::from_bytes(value).expect("should deserialize"),
        }
    }

    pub fn satisfies_column_filter(&self, filter: &ColumnFilter) -> bool {
        match filter {
            ColumnFilter::Key(key) => {
                if self.column_key.family != key.family {
                    return false;
                }

                if let Some(cq_filter) = &key.qualifier {
                    if self.column_key.qualifier.as_deref().unwrap_or_default() != cq_filter {
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
                        if self.column_key.qualifier.as_deref().unwrap_or_default() == cq_filter {
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
                        .unwrap_or_default()
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
    #[serde(rename = "time")]
    pub timestamp: u128,

    #[serde(flatten)]
    pub value: Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CellValue;
    use test_log::test;

    #[test]
    fn cell_format_key() {
        let key = VisitedCell::format_key("test", &ColumnKey::try_from("value:").unwrap(), 0);

        let cell = VisitedCell::parse(key.clone().into(), &CellValue::Byte(0).to_bytes());

        assert_eq!(cell.raw_key, key.into());
        assert_eq!(cell.value, CellValue::Byte(0));
    }

    #[test]
    fn cell_serde() {
        let cell = Cell {
            timestamp: 0,
            value: Value::String("test".into()),
        };

        let s = serde_json::to_string(&cell).unwrap();
        let p: Cell = serde_json::from_str(&s).unwrap();

        assert_eq!(
            p,
            serde_json::from_str(
                r#"
        {"time":0,"type":"string","value":"test"}
        "#
            )
            .unwrap()
        );
    }
}
