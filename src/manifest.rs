use crate::data_folder;
use lsm_tree::Tree as LsmTree;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub struct ManifestTable {
    data: LsmTree,
}

impl std::ops::Deref for ManifestTable {
    type Target = LsmTree;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ColumnFamilyDefinition {
    pub name: String,
    pub row_limit: Option<u64>,
}

impl ManifestTable {
    pub fn open(block_cache: Arc<lsm_tree::BlockCache>) -> lsm_tree::Result<Self> {
        let manifest_table_path = data_folder().join("manifest");
        log::info!(
            "Opening manifest table at {}",
            manifest_table_path.display()
        );

        let tree = Self {
            data: lsm_tree::Config::new(manifest_table_path)
                .level_ratio(2)
                .level_count(2)
                .block_cache(block_cache)
                .max_memtable_size(/* 512 KiB */ 512 * 1_024)
                .flush_threads(1)
                .compaction_strategy(Arc::new(lsm_tree::compaction::Levelled {
                    l0_threshold: 1,
                    target_size: 512 * 1_024,
                }))
                .open()?,
        };

        #[cfg(debug_assertions)]
        {
            eprintln!("= MANIFEST =");
            for item in &tree.iter() {
                let (key, value) = item?;
                let key = std::str::from_utf8(&key).expect("should be utf-8");
                let value = std::str::from_utf8(&value).expect("should be utf-8");

                eprintln!("{key} => {value}");
            }
            eprintln!("= MANIFEST OVER =");
        }

        log::info!("Recovered manifest table");

        Ok(tree)
    }

    pub fn get_user_table_names(&self) -> lsm_tree::Result<Vec<String>> {
        self.data
            .prefix("n:")
            .into_iter()
            .map(|item| {
                let (_, table_name) = item?;
                let table_name =
                    String::from_utf8(table_name.to_vec()).expect("table name should be utf-8");
                Ok(table_name)
            })
            .collect()
    }

    pub fn persist_user_table(&self, table_name: &str) -> lsm_tree::Result<()> {
        self.data.insert(format!("n:{table_name}"), table_name)?;
        self.data.flush()?;
        Ok(())
    }

    pub fn persist_column_family(
        &self,
        table_name: &str,
        column_family_definition: &ColumnFamilyDefinition,
    ) -> lsm_tree::Result<()> {
        let str = serde_json::to_string(&column_family_definition).expect("should serialize");

        self.data.insert(
            format!("t:{table_name}:cf:{}", column_family_definition.name),
            str,
        )?;
        self.data.flush()?;

        Ok(())
    }

    pub fn get_user_table_column_families(
        &self,
        table_name: &str,
    ) -> lsm_tree::Result<Vec<ColumnFamilyDefinition>> {
        self.data
            .prefix(format!("t:{table_name}:cf:"))
            .into_iter()
            .map(|item| {
                let (_, value) = item?;
                let value = std::str::from_utf8(&value).expect("column definition should be utf-8");
                let value = serde_json::from_str::<ColumnFamilyDefinition>(value)
                    .expect("column definition should be json");
                Ok(value)
            })
            .collect()
    }

    pub fn column_family_exists(
        &self,
        table_name: &str,
        column_family_name: &str,
    ) -> lsm_tree::Result<bool> {
        Ok(self
            .get_user_table_column_families(table_name)?
            .iter()
            .any(|cf| cf.name == column_family_name))
    }

    pub fn delete_user_table(&self, table_name: &str) -> lsm_tree::Result<()> {
        let mut batch = self.data.batch();

        batch.remove(format!("n:{table_name}"));

        for item in self.get_user_table_column_families(table_name)? {
            batch.remove(format!("t:{table_name}:cf:{}", item.name));
        }

        batch.commit()?;

        Ok(())
    }
}
