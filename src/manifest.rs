use fjall::{Keyspace, PartitionHandle};
use std::sync::Arc;

pub struct ManifestTable {
    pub keyspace: Keyspace,
    tree: PartitionHandle,
}

impl ManifestTable {
    pub fn open(keyspace: Keyspace) -> fjall::Result<Self> {
        log::debug!("Loading manifest table");

        let tree = keyspace.open_partition(
            "_manifest",
            fjall::PartitionCreateOptions::default()
                .level_ratio(2)
                .level_count(2),
        )?;

        tree.set_max_memtable_size(/* 512 KiB */ 512 * 1_024);

        tree.set_compaction_strategy(Arc::new(fjall::compaction::Levelled {
            l0_threshold: 1,
            target_size: 512 * 1_024,
        }));

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

        Ok(Self { tree, keyspace })
    }

    pub fn get_user_table_names(&self) -> fjall::Result<Vec<String>> {
        let items = self
            .tree
            .iter()
            .into_iter()
            .collect::<Result<Vec<_>, fjall::LsmError>>()?;

        let names = items
            .into_iter()
            .map(|(_k, v)| {
                let str = std::str::from_utf8(&v).expect("should be utf-8");
                str.to_owned()
            })
            .collect();

        Ok(names)
    }

    pub fn persist_user_table(&self, table_name: &str) -> fjall::Result<()> {
        self.tree
            .insert(format!("table:{table_name}:name"), table_name)?;

        self.keyspace.persist()?;

        Ok(())
    }

    /* pub fn get_user_table_column_families(
        &self,
        table_name: &str,
    ) -> fjall::Result<Vec<ColumnFamilyDefinition>> {
        let result = self.data.query(crate::table::reader::Input {
            row_key: table_name.into(),
            cell_limit: None,
            column_filter: Some(ColumnKey::try_from("family:").unwrap()),
            row_limit: None,
        })?;

        let Some(row) = result.rows.first() else {
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
    } */

    pub fn delete_user_table(&self, table_name: &str) -> fjall::Result<()> {
        self.tree.remove(format!("table:{table_name}:name"))?;
        Ok(())
    }
}
