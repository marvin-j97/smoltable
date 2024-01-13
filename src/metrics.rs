use crate::{
    env::metrics_cap_mb,
    table::{ColumnFamilyDefinition, CreateColumnFamilyInput, GarbageCollectionOptions, Smoltable},
};
use fjall::Keyspace;
use std::sync::Arc;

pub struct MetricsTable;

impl MetricsTable {
    pub async fn open(keyspace: Keyspace, name: &str) -> fjall::Result<Smoltable> {
        let max_mb = u64::from(metrics_cap_mb());

        let table = Smoltable::with_strategy(
            name,
            keyspace,
            Arc::new(fjall::compaction::Fifo::new(
                /* N MiB */ max_mb * 1_000 * 1_000,
            )),
        )?;

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "value".into(),
                gc_settings: GarbageCollectionOptions {
                    ttl_secs: None,
                    version_limit: None,
                },
            }],
            locality_group: None,
        })?;

        Ok(table)
    }
}
