use crate::{
    column_key::ColumnKey,
    table::{cell::Row, Smoltable},
};
use fjall::Keyspace;

#[derive(Clone)]
pub struct MetricsTable(Smoltable);

impl std::ops::Deref for MetricsTable {
    type Target = Smoltable;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// TODO: store metrics per User Table
// TODO: if table is deleted -> Drop metrics table as well etc etc

impl MetricsTable {
    pub async fn create_new(keyspace: Keyspace) -> fjall::Result<Self> {
        /* let max_mb = u64::from(metrics_cap_mb());

        let tree = keyspace.open_partition(
            "_metrics",
            fjall::PartitionCreateOptions::default()
                .level_count(7)
                .block_size(/* 32 MiB */ 32 * 1_024 * 1_024),
        )?;

        tree.set_compaction_strategy(Arc::new(fjall::compaction::Fifo::new(
            /* N MiB */ max_mb * 1_000 * 1_000,
        )));

        log::info!("Recovered metrics table"); */

        // TODO: Smoltable::with_tree
        let table = Self(Smoltable::open("_metrics", keyspace)?);

        Ok(table)
    }

    /* pub fn query_timeseries(
        &self,
        name: &str,
        column_filter: Option<ColumnKey>,
    ) -> fjall::Result<Vec<Row>> {
        let data = self
            .0
            .query_prefix(crate::table::QueryRowInput {
                row_key: name.to_owned(),
                cell_limit: Some(/* 12 hours */ 1_440 / 2), // TODO: use timestamp gt filter instead of cell_limit
                column_filter,
                row_limit: None,
            })?
            .rows;

        Ok(data)
    } */
}
