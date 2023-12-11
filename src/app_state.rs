use crate::{manifest::ManifestTable, metrics::MetricsTable, table::SmolTable};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

pub struct AppState {
    pub manifest_table: Arc<ManifestTable>,
    pub metrics_table: MetricsTable,
    pub user_tables: Arc<RwLock<HashMap<String, SmolTable>>>,
}

impl AppState {
    pub fn create_table(&self, table_name: &str) -> lsm_tree::Result<SmolTable> {
        let path = crate::data_folder().join("user_tables").join(table_name);
        let table = SmolTable::new(path, self.manifest_table.config().block_cache.clone())?;

        self.manifest_table.persist_user_table(table_name)?;

        let mut user_tables = self.user_tables.write().expect("lock is poisoned");
        user_tables.insert(table_name.into(), table.clone());

        Ok(table)
    }
}
