use super::Smoltable;
use crate::{CellValue, ColumnKey};
use fjall::Batch;
use serde::Deserialize;

pub struct Writer {
    table: Smoltable,
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

pub fn timestamp_nano() -> u128 {
    std::time::SystemTime::UNIX_EPOCH
        .elapsed()
        .unwrap()
        .as_nanos()
}

impl Writer {
    pub fn new(target_table: Smoltable) -> Self {
        let batch = target_table.batch();

        Self {
            table: target_table,
            batch,
        }
    }

    pub fn write_batch(table: Smoltable, items: &[RowWriteItem]) -> fjall::Result<()> {
        let mut writer = Self::new(table);
        for item in items {
            writer.write(item)?;
        }
        writer.finalize()?;
        Ok(())
    }

    pub fn write(&mut self, item: &RowWriteItem) -> fjall::Result<()> {
        for cell in &item.cells {
            let mut key = format!(
                "{}:{}:{}:",
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

            let partition = self
                .table
                .get_partition_for_column_family(&cell.column_key.family)?;

            self.batch.insert(&partition, key, encoded_value);
        }

        Ok(())
    }

    pub fn finalize(self) -> fjall::Result<()> {
        self.batch.commit()
    }
}
