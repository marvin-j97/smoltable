use super::cell::VisitedCell;
use crate::table::cell::Cell;
use fjall::{PartitionHandle, Snapshot};
use std::{collections::VecDeque, ops::Bound};

/// Stupidly iterates through a prefixed set of cells
pub struct Reader {
    pub partition: PartitionHandle,
    snapshot: Snapshot,
    prefix: String,
    range: Option<(Bound<Vec<u8>>, Bound<Vec<u8>>)>,

    buffer: VecDeque<VisitedCell>,

    pub cells_scanned_count: u64,
    pub bytes_scanned_count: u64,

    chunk_size: usize,
}

impl Reader {
    pub fn new(instant: fjall::Instant, locality_group: PartitionHandle, prefix: String) -> Self {
        let snapshot = locality_group.snapshot_at(instant);

        Self {
            partition: locality_group,
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

    fn initialize_range(&mut self) -> Option<fjall::Result<()>> {
        use std::ops::Bound::{Included, Unbounded};

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

        Some(Ok(()))
    }

    // TODO: try to make Peek return a &VisitedCell
    pub fn peek(&mut self) -> Option<fjall::Result<VisitedCell>> {
        use std::ops::Bound::{Excluded, Unbounded};

        // First, consume buffer, if filled
        if let Some(cell) = self.buffer.front().cloned() {
            return Some(Ok(cell));
        }

        // Get initial range start
        if self.range.is_none() {
            if let Err(e) = self.initialize_range()? {
                return Some(Err(e));
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

                    self.buffer.extend(
                        chunk
                            .into_iter()
                            .map(|(k, v)| Cell::parse(k, &v))
                            .collect::<Vec<_>>(),
                    );

                    if let Some(cell) = self.buffer.front().cloned() {
                        self.range = Some(range);
                        return Some(Ok(cell));
                    }
                }
                Err(e) => return Some(Err(fjall::Error::Storage(e))),
            }
        }
    }
}

impl Iterator for &mut Reader {
    type Item = fjall::Result<VisitedCell>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.peek()? {
            Err(e) => Some(Err(e)),
            Ok(_) => Some(Ok(self.buffer.pop_front().unwrap())),
        }

        /*   use std::ops::Bound::{Excluded, Unbounded};

        // First, consume buffer, if filled
        if let Some(cell) = self.buffer.pop_front() {
            return Some(Ok(cell));
        }

        // Get initial range start
        if self.range.is_none() {
            if let Err(e) = self.initialize_range()? {
                return Some(Err(e));
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

                    self.buffer.extend(
                        chunk
                            .into_iter()
                            .map(|(k, v)| Cell::parse(k, &v))
                            .collect::<Vec<_>>(),
                    );

                    if let Some(cell) = self.buffer.pop_front() {
                        self.range = Some(range);
                        return Some(Ok(cell));
                    }
                }
                Err(e) => return Some(Err(fjall::Error::Storage(e))),
            }
        } */
    }
}
