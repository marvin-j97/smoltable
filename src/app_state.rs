use crate::{manifest::ManifestTable, metrics::MetricsTable, table::Smoltable};
use fjall::{BlockCache, Keyspace};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

pub struct AppState {
    pub keyspace: Keyspace,
    pub manifest_table: Arc<ManifestTable>,
    pub tables: Arc<RwLock<HashMap<String, Smoltable>>>,
    pub block_cache: Arc<BlockCache>,
    pub metrics_table: MetricsTable,
}

impl AppState {
    pub async fn create_table(&self, table_name: &str) -> fjall::Result<Smoltable> {
        let mut tables = self.tables.write().await;

        self.manifest_table.persist_user_table(table_name)?;
        let table = Smoltable::open(table_name, self.keyspace.clone())?;

        tables.insert(table_name.into(), table.clone());

        Ok(table)
    }

    /* pub async fn create_column_families(
        &self,
        table_name: &str,
        defs: &crate::api::create_column_family::Input,
    ) -> fjall::Result<bool> {
        let mut tables = self.tables.write().await;

        let Some(table) = tables.get_mut(table_name) else {
            return Ok(false);
        };

        let mut writer = TableWriter::new(self.manifest_table.data.clone());

        for item in &defs.column_families {
            let str = serde_json::to_string(&item).expect("should serialize");

            writer.write(&RowWriteItem {
                row_key: table_name.into(),
                cells: vec![ColumnWriteItem {
                    column_key: ColumnKey {
                        family: "family".into(),
                        qualifier: Some(item.name.clone()),
                    },
                    timestamp: Some(0),
                    value: CellValue::String(str),
                }],
            })?;
        }

        if defs.locality_group.unwrap_or_default() {
            /* writer.write(&RowWriteItem {
                row_key: table_name.into(),
                cells: vec![ColumnWriteItem {
                    column_key: ColumnKey {
                        family: "family".into(),
                        qualifier: Some(item.name.clone()),
                    },
                    timestamp: Some(0),
                    value: CellValue::String(str),
                }],
            })?; */

            table.locality_groups.push(LocalityGroup {
                tree: self.keyspace.open_partition(
                    &format!("_lg_{table_name}"),
                    PartitionCreateOptions::default().block_size(BLOCK_SIZE),
                )?,
                column_families: defs
                    .column_families
                    .iter()
                    .map(|x| x.name.clone().into())
                    .collect(),
            });
        }

        writer.finalize()?;

        Ok(true)
    } */
}
