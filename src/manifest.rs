use crate::{
    column_key::ColumnKey,
    data_folder,
    table::{
        cell::Value as CellValue,
        writer::{ColumnWriteItem, Writer as TableWriter},
        QueryInput, Smoltable,
    },
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub struct ManifestTable {
    data: Smoltable,
}

impl std::ops::Deref for ManifestTable {
    type Target = Smoltable;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ColumnFamilyDefinition {
    pub name: String,
    pub row_limit: Option<u64>, // TODO: rename cell limit
}

impl ManifestTable {
    pub fn open(block_cache: Arc<lsm_tree::BlockCache>) -> lsm_tree::Result<Self> {
        let manifest_table_path = data_folder().join("manifest");
        log::info!(
            "Opening manifest table at {}",
            manifest_table_path.display()
        );

        let tree = lsm_tree::Config::new(manifest_table_path)
            .level_ratio(2)
            .level_count(2)
            .block_cache(block_cache)
            .max_memtable_size(/* 512 KiB */ 512 * 1_024)
            .flush_threads(1)
            .compaction_strategy(Arc::new(lsm_tree::compaction::Levelled {
                l0_threshold: 1,
                target_size: 512 * 1_024,
            }))
            .open()?;

        #[cfg(debug_assertions)]
        {
            eprintln!("= MANIFEST =");
            for item in &tree.iter() {
                let (key, value) = item?;
                let key = String::from_utf8_lossy(&key);
                let value = String::from_utf8_lossy(&value);

                eprintln!("{key} => {value}");
            }
            eprintln!("= MANIFEST OVER =");
        }

        log::info!("Recovered manifest table");

        Ok(Self {
            data: Smoltable::from_tree(tree)?,
        })
    }

    pub fn get_user_table_names(&self) -> lsm_tree::Result<Vec<String>> {
        let result = self.data.query(&QueryInput {
            row_key: "".into(),
            cell_limit: None,
            column_filter: None,
            row_limit: None,
        })?;

        let names = result.rows.into_iter().map(|x| x.key).collect();

        Ok(names)
    }

    pub fn persist_user_table(&self, table_name: &str) -> lsm_tree::Result<()> {
        TableWriter::write_raw(
            &self.data,
            &crate::table::writer::RowWriteItem {
                row_key: table_name.into(),
                cells: vec![ColumnWriteItem {
                    column_key: ColumnKey::try_from("name:").unwrap(),
                    timestamp: Some(0),
                    value: CellValue::String(table_name.into()),
                }],
            },
        )?;
        self.data.tree.flush()?;

        Ok(())
    }

    pub fn persist_column_family(
        &self,
        table_name: &str,
        column_family_definition: &ColumnFamilyDefinition,
    ) -> lsm_tree::Result<()> {
        let str = serde_json::to_string(&column_family_definition).expect("should serialize");

        TableWriter::write_raw(
            &self.data,
            &crate::table::writer::RowWriteItem {
                row_key: table_name.into(),
                cells: vec![ColumnWriteItem {
                    column_key: ColumnKey {
                        family: "family".into(),
                        qualifier: Some(column_family_definition.name.clone()),
                    },
                    timestamp: Some(0),
                    value: CellValue::String(str),
                }],
            },
        )?;
        self.data.tree.flush()?;

        Ok(())
    }

    pub fn get_user_table_column_families(
        &self,
        table_name: &str,
    ) -> lsm_tree::Result<Vec<ColumnFamilyDefinition>> {
        let result = self.data.query(&QueryInput {
            row_key: table_name.into(),
            cell_limit: None,
            column_filter: Some(ColumnKey::try_from("family:").unwrap()),
            row_limit: None,
        })?;

        let Some(row) = result.rows.get(0) else {
            return Ok(vec![]);
        };

        let Some(col_family) = row.columns.get("family") else {
            return Ok(vec![]);
        };

        let names = col_family
            .iter()
            .map(|(key, cells)| (key, &cells[0]))
            .collect::<Vec<_>>();

        let Some(jsons) = names
            .iter()
            .map(|(_, cell)| match &cell.value {
                CellValue::String(str) => Some(str),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(vec![]);
        };

        let families = jsons
            .iter()
            .map(|value| serde_json::from_str(value).expect("should be valid json"))
            .collect();

        Ok(families)
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
        self.data.delete_cells(&format!("{table_name}:"))?;
        Ok(())
    }
}
