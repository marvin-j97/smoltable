use crate::{app_state::MonitoredSmoltable, data_point};
use smoltable::TableWriter;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;

pub async fn start(tables: Arc<RwLock<HashMap<String, MonitoredSmoltable>>>) {
    loop {
        let tables_lock = tables.read().await;
        let tables = tables_lock.clone();
        drop(tables_lock);

        tokio::task::spawn_blocking(move || {
            for (table_name, table) in tables {
                log::debug!("Running TTL worker on {table_name:?}");

                match table.run_version_gc() {
                    Ok(deleted_count) => {
                        log::info!("Cell GC deleted {deleted_count} cells in {table_name:?}");

                        TableWriter::write_batch(
                            table.metrics.clone(),
                            &[smoltable::row!(
                                "gc#del_cnt",
                                vec![data_point!(deleted_count as f64)]
                            )],
                        )
                        .ok();
                    }
                    Err(e) => {
                        log::error!("Error during cell GC: {e:?}");
                    }
                };
            }
        });

        log::info!("TTL worker done");
        tokio::time::sleep(Duration::from_secs(/* 24 hours*/ 21_600 * 4)).await;
    }
}
