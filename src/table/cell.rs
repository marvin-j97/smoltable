use crate::column_key::ColumnKey;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Cell {
    pub timestamp: u128,
    pub value: Value,
}

impl Cell {
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
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Row {
    pub row_key: String,
    pub columns: HashMap<String, HashMap<String, Vec<Cell>>>,
}

impl Row {
    pub fn family_count(&self) -> usize {
        self.columns.len()
    }

    pub fn column_count(&self) -> usize {
        self.columns.values().map(HashMap::len).sum::<usize>()
    }

    pub fn cell_count(&self) -> usize {
        self.columns
            .values()
            .map(|family| family.values().map(|column| column.len()).sum::<usize>())
            .sum::<usize>()
    }
}
