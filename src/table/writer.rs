use super::{CellValue, Smoltable};
use crate::column_key::ColumnKey;
use lsm_tree::Batch;
use serde::Deserialize;

pub struct Writer {
    batch: Batch,
}

#[derive(Debug, Deserialize)]
pub struct ColumnWriteItem {
    pub column_key: ColumnKey,
    pub timestamp: Option<u128>,
    pub value: CellValue,
}

#[derive(Debug, Deserialize)]
pub struct RowWriteItem {
    pub row_key: String,
    pub cells: Vec<ColumnWriteItem>,
}

fn timestamp_nano() -> u128 {
    std::time::SystemTime::UNIX_EPOCH
        .elapsed()
        .unwrap()
        .as_nanos()
}

impl Writer {
    pub fn new(target_table: &Smoltable) -> Self {
        let batch = target_table.batch();
        Self { batch }
    }

    pub fn write_raw(table: &Smoltable, item: &RowWriteItem) -> lsm_tree::Result<()> {
        for cell in &item.cells {
            let mut key = format!(
                "{}:cf:{}:c:{}:",
                item.row_key,
                cell.column_key.family,
                cell.column_key
                    .qualifier
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| String::from("")),
            )
            .as_bytes()
            .to_vec();

            // NOTE: Reverse the timestamp to store it in descending order
            key.extend_from_slice(&(!cell.timestamp.unwrap_or_else(timestamp_nano)).to_be_bytes());

            let encoded_value = bincode::serialize(&cell.value).expect("should serialize");
            table.tree.insert(key, encoded_value)?;
        }

        Ok(())
    }

    pub fn write(&mut self, item: &RowWriteItem) -> lsm_tree::Result<()> {
        for cell in &item.cells {
            let mut key = format!(
                "{}:cf:{}:c:{}:",
                item.row_key,
                cell.column_key.family,
                cell.column_key
                    .qualifier
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| String::from("")),
            )
            .as_bytes()
            .to_vec();

            // NOTE: Reverse the timestamp to store it in descending order
            key.extend_from_slice(&(!cell.timestamp.unwrap_or_else(timestamp_nano)).to_be_bytes());

            let encoded_value = bincode::serialize(&cell.value).expect("should serialize");
            self.batch.insert(key, encoded_value);
        }

        Ok(())
    }

    pub fn finalize(self) -> lsm_tree::Result<()> {
        self.batch.commit()
    }
}
