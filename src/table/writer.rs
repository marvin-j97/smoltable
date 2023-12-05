use std::sync::Arc;

use super::SmolTable;
use crate::{column_key::ColumnKey, identifier::is_valid_identifier, manifest::ManifestTable};
use base64::{engine::general_purpose, Engine as _};
use lsm_tree::Batch;
use serde::Deserialize;

pub struct Writer {
    manifest_table: Arc<ManifestTable>,
    user_table: SmolTable,
    batch: Batch,
    table_name: String,
}

#[derive(Debug, Deserialize)]
pub struct ColumnWriteItem {
    column_key: ColumnKey,
    timestamp: Option<u128>,
    value: String, // base64-encoded
}

#[derive(Debug, Deserialize)]
pub struct RowWriteItem {
    pub row_key: String,
    pub cells: Vec<ColumnWriteItem>,
}

pub enum WriteError {
    Lsm(lsm_tree::Error),
    BadInput(&'static str),
}

impl From<lsm_tree::Error> for WriteError {
    fn from(value: lsm_tree::Error) -> Self {
        Self::Lsm(value)
    }
}

fn timestamp_nano() -> u128 {
    std::time::SystemTime::UNIX_EPOCH
        .elapsed()
        .unwrap()
        .as_nanos()
}

impl Writer {
    pub fn new(
        manifest_table: Arc<ManifestTable>,
        user_table: SmolTable,
        table_name: String,
    ) -> Self {
        let batch = user_table.batch();

        Self {
            manifest_table,
            user_table,
            batch,
            table_name,
        }
    }

    pub fn write(&mut self, item: &RowWriteItem) -> Result<(), WriteError> {
        if !is_valid_identifier(&item.row_key) {
            return Err(WriteError::BadInput("Invalid item definition"));
        }

        for cell in &item.cells {
            if !self
                .manifest_table
                .column_family_exists(&self.table_name, &cell.column_key.family)?
            {
                return Err(WriteError::BadInput("Column family does not exist"));
            }

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

            let Ok(decoded_value) = general_purpose::STANDARD.decode(&cell.value) else {
                return Err(WriteError::BadInput(
                    "Invalid value: could not be base64-decoded",
                ));
            };

            self.batch.insert(key, decoded_value);
        }

        Ok(())
    }

    pub fn finalize(self) -> lsm_tree::Result<()> {
        self.batch.commit()
    }
}
