pub mod gc;
pub mod metrics;
pub mod row_count;

use crate::app_state::MonitoredSmoltable;
use fjall::Keyspace;
use smoltable::Smoltable;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;

pub fn start_all(
    keyspace: &Keyspace,
    system_metrics_table: &Smoltable,
    tables: &Arc<RwLock<HashMap<String, MonitoredSmoltable>>>,
) {
    // Start TTL worker
    let tables_copy = tables.clone();

    log::info!("Starting TTL worker");
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(15)).await;
        gc::start(tables_copy).await;
    });

    // Start row counting worker
    let tables_copy = tables.clone();

    log::info!("Starting row counting worker");
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(15)).await;
        row_count::start(tables_copy).await;
    });

    // Start metrics worker
    let keyspace = keyspace.clone();
    let system_metrics_table = system_metrics_table.clone();
    let tables_copy = tables.clone();

    log::info!("Starting system metrics worker");
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(15)).await;
        metrics::start(keyspace, system_metrics_table, tables_copy).await;
    });
}
