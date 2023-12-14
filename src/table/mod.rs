pub mod cell;
pub mod writer;

use self::cell::{Cell, Row, Value as CellValue};
use crate::column_key::ColumnKey;
use lsm_tree::{Batch, BlockCache, Tree as LsmTree};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ops::Bound, path::Path, sync::Arc};

const BLOCK_SIZE: u32 = /* 16 KiB */ 16 * 1024;

#[derive(Clone)]
pub struct Smoltable {
    pub tree: LsmTree,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QueryInput {
    pub row_key: String,
    pub column_filter: Option<ColumnKey>,
    pub row_limit: Option<u16>,
    pub cell_limit: Option<u16>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QueryOutput {
    pub rows: Vec<Row>,
    pub cells_scanned_count: u64,
    pub rows_scanned_count: u64,
}

#[derive(Debug)]
pub struct VisitedCell {
    pub row_key: String,
    pub column_key: ColumnKey,
    pub timestamp: u128,
    pub value: CellValue,
}

impl Smoltable {
    pub fn new<P: AsRef<Path>>(
        path: P,
        block_cache: Arc<BlockCache>,
    ) -> lsm_tree::Result<Smoltable> {
        let path = path.as_ref();

        let tree = lsm_tree::Config::new(path)
            .fsync_ms(None)
            .level_count(4)
            .compaction_strategy(Arc::new(lsm_tree::compaction::Levelled {
                target_size: 64 * 1_024 * 1_024,
                l0_threshold: 2,
            }))
            .flush_threads(1)
            .block_size(BLOCK_SIZE)
            .block_cache(block_cache)
            .open()?;

        Self::from_tree(tree)
    }

    pub fn from_tree(tree: LsmTree) -> lsm_tree::Result<Smoltable> {
        Ok(Self { tree })
    }

    // TODO: use approximate_len in Tree and set to 1 min
    pub fn cell_count(&self) -> lsm_tree::Result<usize> {
        use std::ops::Bound::{Excluded, Unbounded};

        let snapshot = self.tree.snapshot();

        let mut count = 0;

        let mut range: (Bound<Vec<u8>>, Bound<Vec<u8>>) = (Unbounded, Unbounded);

        loop {
            let chunk = snapshot
                .range(range.clone())
                .into_iter()
                .take(10_000)
                .collect::<lsm_tree::Result<Vec<_>>>()?;

            if chunk.is_empty() {
                break;
            }

            count += chunk.len();

            let (key, _) = chunk.last().unwrap();
            range = (Excluded(key.to_vec()), Unbounded);
        }

        Ok(count)
    }

    pub fn delete_cells(&self, key: &str) -> lsm_tree::Result<u64> {
        use std::ops::Bound::{Excluded, Included, Unbounded};

        let prefix_key = key;
        let mut count = 0;

        let Some((first_key, _)) = self.tree.first_key_value()? else {
            return Ok(count);
        };

        let mut range: (Bound<Vec<u8>>, Bound<Vec<u8>>) = (Included(first_key.to_vec()), Unbounded);

        loop {
            let chunk = self
                .tree
                .range(range.clone())
                .into_iter()
                .take(1_000)
                .collect::<lsm_tree::Result<Vec<_>>>()?;

            if chunk.is_empty() {
                break;
            }

            for (key, _) in &chunk {
                if !key.starts_with(prefix_key.as_bytes()) {
                    break;
                }

                self.tree.remove(key.clone())?;
                count += 1;
            }

            let (key, _) = chunk.last().unwrap();
            range = (Excluded(key.to_vec()), Unbounded);
        }

        self.tree.flush()?;
        Ok(count)
    }

    pub fn query(&self, input: &QueryInput) -> lsm_tree::Result<QueryOutput> {
        use std::ops::Bound::{Excluded, Included, Unbounded};

        let snapshot = self.tree.snapshot();

        let prefix_key = input.row_key.as_bytes();
        let mut cells_scanned_count = 0;
        let mut rows_scanned_count = 0;

        let mut rows = vec![];
        let mut current_row = Row {
            key: "".into(),
            columns: Default::default(),
        };

        let Some((first_key, _)) = snapshot.prefix(prefix_key).into_iter().next().transpose()?
        else {
            return Ok(QueryOutput {
                rows,
                cells_scanned_count,
                rows_scanned_count,
            });
        };

        let mut range: (Bound<Vec<u8>>, Bound<Vec<u8>>) = (Included(first_key.to_vec()), Unbounded);

        loop {
            let chunk = snapshot
                .range(range.clone())
                .into_iter()
                .take(1_000)
                .filter(|x| match x {
                    Ok((key, _)) => key.starts_with(prefix_key),
                    Err(_) => true,
                })
                .collect::<lsm_tree::Result<Vec<_>>>()?;

            if chunk.is_empty() {
                break;
            }

            if rows.len() >= input.row_limit.unwrap_or(u16::MAX) as usize {
                break;
            }

            let cells = chunk
                .iter()
                .map(|(key, value)| {
                    let parsed_key = key.splitn(7, |&e| e == b':');

                    let chunks = parsed_key.collect::<Vec<_>>();

                    let row_key = std::str::from_utf8(chunks[0]).unwrap();
                    let cf = std::str::from_utf8(chunks[2]).unwrap();
                    let cq = std::str::from_utf8(chunks[4]).ok().map(Into::into);

                    let mut buf = [0; std::mem::size_of::<u128>()];
                    buf.clone_from_slice(&key[key.len() - std::mem::size_of::<u128>()..key.len()]);
                    let ts = !u128::from_be_bytes(buf);

                    cells_scanned_count += 1;

                    Ok::<_, lsm_tree::Error>(VisitedCell {
                        row_key: row_key.into(),
                        timestamp: ts,
                        column_key: ColumnKey {
                            family: cf.to_owned(),
                            qualifier: cq,
                        },
                        value: bincode::deserialize::<CellValue>(value)
                            .expect("should deserialize"),
                    })
                })
                .collect::<lsm_tree::Result<Vec<_>>>()?;

            for cell in cells {
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

                if current_row.key.is_empty() {
                    current_row.key = cell.row_key.clone();
                }

                if cell.row_key != current_row.key {
                    // Rotate over to new row
                    rows.push(current_row);
                    rows_scanned_count += 1;

                    current_row = Row {
                        key: cell.row_key,
                        columns: HashMap::default(),
                    };
                }

                // Append cell
                let version_history = current_row
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

            let (last_key, _) = chunk.last().unwrap();
            range = (Excluded(last_key.to_vec()), Unbounded);
        }

        if !current_row.columns.is_empty() {
            rows.push(current_row);
            rows_scanned_count += 1;
        }

        Ok(QueryOutput {
            rows,
            rows_scanned_count,
            cells_scanned_count,
        })

        // let iter = snapshot.prefix(key);

        /* let iter = iter.into_iter().map(|item| {
            let (key, value) = item?;

            let parsed_key = key.splitn(7, |&e| e == b':');

            let chunks = parsed_key.collect::<Vec<_>>();

            let row_key = std::str::from_utf8(chunks[0]).unwrap();
            let cf = std::str::from_utf8(chunks[2]).unwrap();
            let cq = std::str::from_utf8(chunks[4]).ok().map(Into::into);

            let mut buf = [0; std::mem::size_of::<u128>()];
            buf.clone_from_slice(&key[key.len() - std::mem::size_of::<u128>()..key.len()]);
            let ts = !u128::from_be_bytes(buf);

            cells_scanned_count += 1;

            Ok::<_, lsm_tree::Error>(VisitedCell {
                row_key: row_key.into(),
                timestamp: ts,
                column_key: ColumnKey {
                    family: cf.to_owned(),
                    qualifier: cq,
                },
                value: bincode::deserialize::<CellValue>(&value).expect("should deserialize"),
            })
        }); */

        /* let mut rows = vec![];

        let mut current_row = Row {
            key: "".into(),
            columns: Default::default(),
        };

        let mut rows_scanned_count = 0;

        for (_, item) in iter.enumerate() {
            if rows.len() >= input.row_limit.unwrap_or(u16::MAX) as usize {
                break;
            }

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

            if current_row.key.is_empty() {
                current_row.key = cell.row_key.clone();
            }

            if cell.row_key != current_row.key {
                // Rotate over to new row
                rows.push(current_row);
                rows_scanned_count += 1;

                current_row = Row {
                    key: cell.row_key,
                    columns: HashMap::default(),
                };
            }

            // Append cell
            let version_history = current_row
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

        if !current_row.columns.is_empty() {
            rows.push(current_row);
            rows_scanned_count += 1;
        }

        Ok(QueryOutput {
            rows,
            rows_scanned_count,
            cells_scanned_count,
        }) */
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
