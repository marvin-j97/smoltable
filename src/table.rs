use lsm_tree::{Batch, Tree as LsmTree};
use std::{path::Path, sync::Arc};

const MEMTABLE_SIZE: u32 = /* 32 MiB */ 32 * 1024 * 1024;

#[derive(Clone)]
pub struct SmolTable {
    tree: LsmTree,
}

impl SmolTable {
    pub fn new<P: AsRef<Path>>(path: P) -> lsm_tree::Result<SmolTable> {
        let path = path.as_ref();

        let tree = lsm_tree::Config::new(path)
            .max_memtable_size(MEMTABLE_SIZE)
            .compaction_strategy(Arc::new(lsm_tree::compaction::Levelled {
                target_size: MEMTABLE_SIZE.into(),
                l0_threshold: 2,
                ratio: 4,
            }))
            .block_cache_capacity(/* 50 MiB*/ 12_800)
            .open()?;

        #[cfg(debug_assertions)]
        {
            eprintln!("= USER TABLE {} =", path.display());
            for item in &tree.iter()? {
                let (key, value) = item?;
                let key = String::from_utf8_lossy(&key);

                eprintln!("{key} => {value:?}");
            }
        }

        Ok(Self { tree })
    }

    pub fn batch(&self) -> Batch {
        self.tree.batch()
    }

    pub fn disk_space_usage(&self) -> u64 {
        self.tree.disk_space()
    }

    pub fn cached_block_count(&self) -> usize {
        self.tree.block_cache_size()
    }

    pub fn cache_memory_usage(&self) -> usize {
        self.tree.block_cache_size() * (self.tree.config().block_size as usize)
    }
}
