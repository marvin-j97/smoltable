use crate::Cell;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
