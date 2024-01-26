use crate::{app_state::MonitoredSmoltable, data_point};
use fjall::Keyspace;
use smoltable::{Smoltable, TableWriter};
use std::{collections::HashMap, sync::Arc, time::Duration};
use sysinfo::SystemExt;
use tokio::sync::RwLock;

pub async fn start(
    keyspace: Keyspace,
    system_metrics_table: Smoltable,
    tables: Arc<RwLock<HashMap<String, MonitoredSmoltable>>>,
) {
    loop {
        log::debug!("Saving system metrics");

        let sysinfo = sysinfo::System::new_all();

        let tables_lock = tables.read().await;
        let tables = tables_lock.clone();
        drop(tables_lock);

        for (_, table) in tables {
            let folder_size = table.disk_space_usage();
            let segment_count = table.segment_count();

            TableWriter::write_batch(
                table.metrics.clone(),
                &[
                    smoltable::row!("stats#seg_cnt", vec![data_point!(segment_count as f64)]),
                    smoltable::row!("stats#du", vec![data_point!(folder_size as f64)]),
                ],
            )
            .ok();
        }

        let journal_count = keyspace.journal_count();
        let write_buffer_size = keyspace.write_buffer_size();
        // TODO: let block_cache_size = keyspace.block_cache_size();

        TableWriter::write_batch(
            system_metrics_table.clone(),
            &[
                smoltable::row!("sys#cpu", vec![data_point!(sysinfo.load_average().one)]),
                smoltable::row!("sys#mem", vec![data_point!(sysinfo.used_memory() as f64)]),
                smoltable::row!("wal#len", vec![data_point!(journal_count as f64)]),
                smoltable::row!("wbuf#size", vec![data_point!(write_buffer_size as f64)]),
            ],
        )
        .ok();

        log::info!("System metrics worker done");
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}
