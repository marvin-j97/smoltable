use crate::{
    column_key::ColumnKey,
    data_folder,
    table::{cell::Row, QueryInput, Smoltable},
};
use std::sync::Arc;

#[derive(Clone)]
pub struct MetricsTable(Smoltable);

impl std::ops::Deref for MetricsTable {
    type Target = Smoltable;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl MetricsTable {
    pub fn from_table(table: Smoltable) -> Self {
        Self(table)
    }

    pub fn create_new(block_cache: Arc<lsm_tree::BlockCache>) -> lsm_tree::Result<Self> {
        let metrics_table_path = data_folder().join("tables").join("_metrics");
        log::info!("Opening metrics table at {}", metrics_table_path.display());

        let tree = lsm_tree::Config::new(metrics_table_path.clone())
            .fsync_ms(None)
            .level_count(2)
            .block_cache(block_cache)
            .max_memtable_size(/* 1 MiB */ 1_024 * 1_024)
            .compaction_strategy(lsm_tree::compaction::Fifo::new(
                /* 100 MiB */ 100 * 1_024 * 1_024,
            ))
            .flush_threads(1)
            .open()?;

        log::info!("Recovered metrics table");

        Ok(Self(Smoltable::from_tree(tree)?))
    }

    pub fn query_timeseries(
        &self,
        name: &str,
        column_filter: Option<ColumnKey>,
    ) -> lsm_tree::Result<Vec<Row>> {
        let data = self
            .0
            .query(&QueryInput {
                row_key: name.to_owned(),
                cell_limit: Some(/* 12 hours*/ 1_440 / 2), // TODO: use timestamp gt filter instead of cell_limit
                column_filter,
                row_limit: None,
            })?
            .rows;

        Ok(data)
    }
}
