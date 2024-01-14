use crate::{manifest::ManifestTable, metrics::MetricsTable};
use fjall::{BlockCache, Keyspace};
use smoltable::Smoltable;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct MonitoredSmoltable {
    pub(crate) inner: Smoltable,
    pub(crate) metrics: Smoltable,
}

impl std::ops::Deref for MonitoredSmoltable {
    type Target = Smoltable;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct AppState {
    pub keyspace: Keyspace,
    pub manifest_table: Arc<ManifestTable>,
    pub tables: Arc<RwLock<HashMap<String, MonitoredSmoltable>>>,
    pub block_cache: Arc<BlockCache>,
    pub system_metrics_table: Smoltable,
}

impl AppState {
    pub async fn create_table(&self, table_name: &str) -> fjall::Result<MonitoredSmoltable> {
        let mut tables = self.tables.write().await;

        self.manifest_table.persist_user_table(table_name)?;

        let inner = Smoltable::open(table_name, self.keyspace.clone())?;

        let metrics =
            MetricsTable::open(self.keyspace.clone(), &format!("_mtx_{table_name}")).await?;

        let table = MonitoredSmoltable { inner, metrics };

        tables.insert(table_name.into(), table.clone());

        Ok(table)
    }
}
