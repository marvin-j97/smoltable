use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum Value {
    String(String),
    Boolean(bool),
    U8(u8),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    // U128(u128),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Cell {
    pub timestamp: u128,
    pub value: Value,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Row {
    pub row_key: String,
    pub columns: HashMap<String, HashMap<String, Vec<Cell>>>,
}
