use crate::{app_state::MonitoredSmoltable, manifest::ManifestTable, metrics::MetricsTable};
use smoltable::Smoltable;
use std::collections::HashMap;

pub async fn recover_tables(
    manifest_table: &ManifestTable,
) -> smoltable::Result<HashMap<String, MonitoredSmoltable>> {
    log::info!("Recovering user tables");

    let mut tables = HashMap::default();

    for table_name in manifest_table
        .get_user_table_names()?
        .into_iter()
        .filter(|x| !x.starts_with('_'))
    {
        log::debug!("Recovering user table {table_name}");

        let recovered_table = Smoltable::open(&table_name, manifest_table.keyspace.clone())?;
        let metrics_table = MetricsTable::open(
            manifest_table.keyspace.clone(),
            &format!("_mtx_{table_name}"),
        )
        .await?;

        tables.insert(
            table_name,
            MonitoredSmoltable {
                inner: recovered_table,
                metrics: metrics_table,
            },
        );
    }

    log::info!("Recovered {} tables", tables.len());

    Ok(tables)
}
