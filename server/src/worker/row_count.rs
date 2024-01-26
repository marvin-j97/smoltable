use crate::{app_state::MonitoredSmoltable, data_point};
use smoltable::TableWriter;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;

pub async fn start(tables: Arc<RwLock<HashMap<String, MonitoredSmoltable>>>) {
    loop {
        let tables_lock = tables.read().await;
        let tables = tables_lock.clone();
        drop(tables_lock);

        let before = std::time::Instant::now();

        tokio::task::spawn_blocking(move || {
            for (table_name, table) in tables {
                log::debug!("Counting {table_name}");

                if let Ok((row_count, cell_count)) = table.count() {
                    TableWriter::write_batch(
                        table.metrics.clone(),
                        &[
                            smoltable::row!("stats#row_cnt", vec![data_point!(row_count as f64)]),
                            smoltable::row!("stats#cell_cnt", vec![data_point!(cell_count as f64)]),
                        ],
                    )
                    .ok();
                }

                log::debug!("Counted {table_name}");
            }
        })
        .await
        .expect("should join task");

        let time_s = before.elapsed().as_secs();

        log::info!("Counting worker done in {time_s}s");

        let sleep_time = match time_s {
            _ if time_s < 5 => 60,
            _ if time_s < 60 => 3_600,
            _ => 21_600, // 6 hours
        };
        tokio::time::sleep(Duration::from_secs(sleep_time)).await;
    }
}
