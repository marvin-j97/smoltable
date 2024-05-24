use fjall::{Keyspace, PartitionHandle};
use std::sync::Arc;

pub struct ManifestTable {
    pub keyspace: Keyspace,
    tree: PartitionHandle,
}

impl ManifestTable {
    pub fn open(keyspace: Keyspace) -> smoltable::Result<Self> {
        log::debug!("Loading manifest table");

        let tree = keyspace.open_partition(
            "_manifest",
            fjall::PartitionCreateOptions::default()
                .level_ratio(2)
                .level_count(2),
        )?;

        tree.set_max_memtable_size(/* 512 KiB */ 512 * 1_024);

        tree.set_compaction_strategy(Arc::new(fjall::compaction::Levelled {
            target_size: /* 512 KiB */ 512 * 1_024,
            l0_threshold: 2,
        }));

        /* #[cfg(debug_assertions)]
        {
            eprintln!("= MANIFEST =");
            for item in &tree.iter() {
                let (key, value) = item?;
                let key = String::from_utf8_lossy(&key);
                let value = String::from_utf8_lossy(&value);

                eprintln!("{key} => {value}");
            }
            eprintln!("= MANIFEST OVER =");
        } */

        log::info!("Recovered manifest table");

        Ok(Self { tree, keyspace })
    }

    pub fn get_user_table_names(&self) -> smoltable::Result<Vec<String>> {
        let items = self.tree.iter().collect::<Result<Vec<_>, _>>()?;

        let names = items
            .into_iter()
            .map(|(_k, v)| {
                let str = std::str::from_utf8(&v).expect("should be utf-8");
                str.to_owned()
            })
            .collect();

        Ok(names)
    }

    pub fn persist_user_table(&self, table_name: &str) -> smoltable::Result<()> {
        self.tree
            .insert(format!("table#{table_name}#name"), table_name)?;

        self.keyspace.persist(fjall::PersistMode::SyncAll)?;

        Ok(())
    }

    pub fn delete_user_table(&self, table_name: &str) -> smoltable::Result<()> {
        for item in self.tree.prefix(format!("table#{table_name}#")) {
            let (k, _) = item?;
            self.tree.remove(k)?;
        }
        Ok(())
    }
}
