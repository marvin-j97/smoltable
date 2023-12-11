use crate::{
    column_key::ColumnKey,
    data_folder,
    table::{
        writer::{ColumnWriteItem, RowWriteItem, WriteError, Writer as TableWriter},
        QueryInput, Row, SmolTable,
    },
};
use base64::{engine::general_purpose, Engine as _};
use std::sync::Arc;

#[derive(Clone)]
pub struct MetricsTable(pub SmolTable);

impl MetricsTable {
    pub fn new(block_cache: Arc<lsm_tree::BlockCache>) -> lsm_tree::Result<Self> {
        let metrics_table_path = data_folder().join("metrics");
        log::info!("Opening metrics table at {}", metrics_table_path.display());

        let tree = lsm_tree::Config::new(metrics_table_path.clone())
            .level_count(2)
            .block_cache(block_cache)
            .max_memtable_size(/* 512 KiB */ 512 * 1_024)
            .compaction_strategy(lsm_tree::compaction::Fifo::new(
                /* 10 MiB */ 10 * 1_024 * 1_024,
            ))
            .open()?;

        log::info!("Recovered metrics table");

        Ok(Self(SmolTable::from_tree(tree)?))
    }

    pub fn push_value(&self, name: &str, value: f64) -> Result<(), WriteError> {
        TableWriter::write_raw(
            &self.0,
            &RowWriteItem {
                row_key: name.into(),
                cells: vec![ColumnWriteItem {
                    column_key: ColumnKey::try_from("v:").expect("should be column key"),
                    timestamp: None,
                    value: general_purpose::STANDARD.encode(value.to_string()),
                }],
            },
        )?;

        Ok(())
    }

    pub fn query_timeseries(&self, name: &str) -> lsm_tree::Result<Vec<Row>> {
        let data = self
            .0
            .query(&QueryInput {
                row_key: name.to_owned(),
                cell_limit: Some(/* 12 hours*/ 1_440 / 2),
                column_filter: None,
                limit: None,
            })?
            .0;

        Ok(data)
    }
}
