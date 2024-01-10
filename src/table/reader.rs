use super::CellValue;
use crate::column_key::ColumnKey;
use fjall::{PartitionHandle, Snapshot};
use std::{collections::VecDeque, ops::Bound, sync::Arc};

#[derive(Debug)]
pub struct VisitedCell {
    pub raw_key: Arc<[u8]>,
    pub row_key: String,
    pub column_key: ColumnKey,
    pub timestamp: u128,
    pub value: CellValue,
}

// TODO: i think : doesn't work in row keys right now

fn deserialize_cell((key, value): (Arc<[u8]>, Arc<[u8]>)) -> VisitedCell {
    let mut buf = [0; std::mem::size_of::<u128>()];
    buf.clone_from_slice(&key[(key.len() - std::mem::size_of::<u128>())..key.len()]);
    let ts = !u128::from_be_bytes(buf);

    // NOTE: + 1 because of : delimiter
    let key_without_ts = &key[0..(key.len() - std::mem::size_of::<u128>() - 1)];
    let mut parsed_key = key_without_ts.rsplitn(4, |&e| e == b':');

    let last = parsed_key.next().unwrap();
    let cq = std::str::from_utf8(last).ok().map(Into::into);

    let last = parsed_key.next().unwrap();
    let cf = std::str::from_utf8(last).unwrap();

    let last = parsed_key.next().unwrap();
    let row_key = std::str::from_utf8(last).unwrap();

    VisitedCell {
        raw_key: key.clone(),
        row_key: row_key.into(),
        timestamp: ts,
        column_key: ColumnKey {
            family: cf.to_owned(),
            qualifier: cq,
        },
        value: bincode::deserialize::<CellValue>(&value).expect("should deserialize"),
    }
}

/// Stupidly iterates through a prefixed set of cells
pub struct Reader {
    snapshot: Snapshot,
    prefix: String,
    range: Option<(Bound<Vec<u8>>, Bound<Vec<u8>>)>,

    buffer: VecDeque<(Arc<[u8]>, Arc<[u8]>)>,

    pub cells_scanned_count: u64,
    pub bytes_scanned_count: u64,

    chunk_size: usize,
}

impl Reader {
    pub fn new(instant: fjall::Instant, locality_group: PartitionHandle, prefix: String) -> Self {
        let snapshot = locality_group.snapshot_at(instant);

        Self {
            snapshot,
            prefix,
            range: None,
            buffer: VecDeque::with_capacity(1_000),
            cells_scanned_count: 0,
            bytes_scanned_count: 0,
            chunk_size: 1_000,
        }
    }

    pub fn chunk_size(mut self, n: usize) -> Self {
        self.chunk_size = n;
        self
    }
}

impl Iterator for &mut Reader {
    type Item = fjall::Result<VisitedCell>;

    fn next(&mut self) -> Option<Self::Item> {
        use std::ops::Bound::{Excluded, Included, Unbounded};

        // First, consume buffer, if filled
        if let Some(item) = self.buffer.pop_front() {
            let cell = deserialize_cell(item);
            return Some(Ok(cell));
        }

        // Get initial range start
        if self.range.is_none() {
            let item = self
                .snapshot
                .prefix(self.prefix.as_bytes())
                .into_iter()
                .next()?;

            match item {
                Ok((first_key, _)) => {
                    self.range = Some((Included(first_key.to_vec()), Unbounded));
                }
                Err(e) => return Some(Err(fjall::Error::Storage(e))),
            }
        }

        let mut range = self.range.clone().unwrap();

        loop {
            // Advance range by querying chunks
            match self
                .snapshot
                .range(range.clone())
                .into_iter()
                .take(self.chunk_size)
                .filter(|x| match x {
                    Ok((key, _)) => key.starts_with(self.prefix.as_bytes()),
                    Err(_) => true,
                })
                .collect::<Result<Vec<_>, fjall::LsmError>>()
            {
                Ok(chunk) => {
                    if chunk.is_empty() {
                        return None;
                    }

                    self.cells_scanned_count += chunk.len() as u64;
                    self.bytes_scanned_count += chunk
                        .iter()
                        .map(|(k, v)| k.len() as u64 + v.len() as u64)
                        .sum::<u64>();

                    let (last_key, _) = chunk.last().unwrap();
                    range = (Excluded(last_key.to_vec()), Unbounded);

                    self.buffer.extend(chunk);

                    if let Some(item) = self.buffer.pop_front() {
                        let cell = deserialize_cell(item);
                        self.range = Some(range);
                        return Some(Ok(cell));
                    }
                }
                Err(e) => return Some(Err(fjall::Error::Storage(e))),
            }
        }
    }
}
