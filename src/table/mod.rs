pub mod writer;

use crate::column_key::ColumnKey;
use base64::{engine::general_purpose, Engine as _};
use lsm_tree::{Batch, Tree as LsmTree};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::Path, sync::Arc};

const MEMTABLE_SIZE: u32 = /* 32 MiB */ 32 * 1024 * 1024;

#[derive(Clone)]
pub struct SmolTable {
    tree: LsmTree,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QueryInput {
    pub row_key: String,
    pub column_filter: Option<ColumnKey>,
    pub limit: Option<u16>,
    pub cell_limit: Option<u16>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Cell {
    pub timestamp: u128,
    pub value: String, // base64-encoded
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Row {
    pub key: String,
    pub columns: HashMap<String, HashMap<String, Vec<Cell>>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QueryOutput(pub Vec<Row>);

#[derive(Debug)]
pub struct VisitedCell {
    pub row_key: String,
    pub column_key: ColumnKey,
    pub timestamp: u128,
    pub value: String, // base64-encoded
}

impl SmolTable {
    pub fn new<P: AsRef<Path>>(path: P) -> lsm_tree::Result<SmolTable> {
        let path = path.as_ref();

        let tree = lsm_tree::Config::new(path)
            .level_count(4)
            .max_memtable_size(MEMTABLE_SIZE)
            .compaction_strategy(Arc::new(lsm_tree::compaction::Levelled {
                target_size: MEMTABLE_SIZE.into(),
                l0_threshold: 2,
                ratio: 4,
            }))
            .block_cache_capacity(/* 50 MiB*/ 12_800)
            .open()?;

        Self::from_tree(path, tree)
    }

    pub fn from_tree<P: AsRef<Path>>(path: P, tree: LsmTree) -> lsm_tree::Result<SmolTable> {
        let path = path.as_ref();

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

    pub fn query(&self, input: &QueryInput) -> lsm_tree::Result<QueryOutput> {
        let key = input.row_key.clone();

        let iter = self.tree.prefix(key)?;

        let mut iter = iter
            .into_iter()
            .map(|item| {
                let (key, value) = item?;

                let parsed_key = key.splitn(7, |&e| e == b':');

                let chunks = parsed_key.collect::<Vec<_>>();
                let row_key = std::str::from_utf8(chunks[0]).unwrap();
                let cf = std::str::from_utf8(chunks[2]).unwrap();
                let cq = std::str::from_utf8(chunks[4]).unwrap();

                let mut buf = [0; std::mem::size_of::<u128>()];
                buf.clone_from_slice(&chunks[5][..std::mem::size_of::<u128>()]);
                let ts = !u128::from_be_bytes(buf);

                Ok::<_, lsm_tree::Error>(VisitedCell {
                    row_key: row_key.into(),
                    timestamp: ts,
                    column_key: ColumnKey {
                        family: cf.to_owned(),
                        qualifier: Some(cq.to_owned()),
                    },
                    value: general_purpose::STANDARD.encode(value),
                })
            })
            .take(input.limit.unwrap_or(1_000).into());

        let mut rows = vec![];

        let Some(first_cell) = iter.next().transpose()? else {
            return Ok(QueryOutput(rows));
        };

        let mut row = Row {
            key: first_cell.row_key,
            columns: {
                let mut map = HashMap::<String, HashMap<String, Vec<Cell>>>::default();

                map.entry(first_cell.column_key.family).or_default().insert(
                    first_cell.column_key.qualifier.unwrap_or(String::from("")),
                    vec![Cell {
                        timestamp: first_cell.timestamp,
                        value: first_cell.value,
                    }],
                );

                map
            },
        };

        for item in iter {
            let cell = item?;

            if let Some(col_filter) = &input.column_filter {
                if cell.column_key.family != col_filter.family {
                    continue;
                }

                if let Some(cq_filter) = &col_filter.qualifier {
                    if cell.column_key.qualifier.as_deref().unwrap_or("") != cq_filter {
                        continue;
                    }
                }
            }

            if cell.row_key != row.key {
                // Rotate over to new row
                rows.push(row);
                row = Row {
                    key: cell.row_key,
                    columns: HashMap::default(),
                }
            }

            // Append cell
            let version_history = row
                .columns
                .entry(cell.column_key.family)
                .or_default()
                .entry(cell.column_key.qualifier.unwrap_or(String::from("_")))
                .or_default();

            if version_history.len() < input.cell_limit.unwrap_or(u16::MAX) as usize {
                version_history.push(Cell {
                    timestamp: cell.timestamp,
                    value: cell.value,
                });
            }
        }

        if !row.columns.is_empty() {
            rows.push(row);
        }

        Ok(QueryOutput(rows))
    }

    pub fn batch(&self) -> Batch {
        self.tree.batch()
    }

    pub fn disk_space_usage(&self) -> lsm_tree::Result<u64> {
        self.tree.disk_space()
    }

    pub fn cached_block_count(&self) -> usize {
        self.tree.block_cache_size()
    }

    pub fn cache_memory_usage(&self) -> usize {
        self.tree.block_cache_size() * (self.tree.config().block_size as usize)
    }
}
