use super::Smoltable;
use crate::{CellValue, ColumnKey, VisitedCell};
use fjall::Batch;
use serde::Deserialize;

pub struct Writer {
    table: Smoltable,
    batch: Batch,
}

#[derive(Debug, Deserialize)]
pub struct ColumnWriteItem {
    pub column_key: ColumnKey, // TODO: rename "column"?

    #[serde(rename = "time")]
    pub timestamp: Option<u128>,

    #[serde(flatten)]
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

    /// Convenience function for opening and committing a write batch
    pub fn write_batch(table: Smoltable, items: &[RowWriteItem]) -> crate::Result<()> {
        let mut writer = Self::new(table);
        for item in items {
            writer.write(item)?;
        }
        writer.finalize()?;
        Ok(())
    }

    /// Appends to the write batch
    pub fn write(&mut self, item: &RowWriteItem) -> crate::Result<()> {
        for cell in &item.cells {
            let key = VisitedCell::format_key(
                &item.row_key,
                &cell.column_key,
                cell.timestamp.unwrap_or_else(timestamp_nano),
            );

            let partition = self
                .table
                .get_partition_for_column_family(&cell.column_key.family)?;

            let encoded_value = cell.value.to_bytes();
            self.batch.insert(&partition, key, encoded_value);
        }

        Ok(())
    }

    pub fn finalize(self) -> crate::Result<()> {
        self.batch.commit()?;
        self.table.keyspace.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }
}
