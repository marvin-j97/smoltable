use crate::VisitedCell;
use fjall::{PartitionHandle, Snapshot};
use std::{collections::VecDeque, ops::Bound, sync::Arc};

/// Stupidly iterates through cells
pub struct Reader {
    pub partition: PartitionHandle,
    snapshot: Snapshot,
    current_range_start: Bound<Arc<[u8]>>,

    buffer: VecDeque<VisitedCell>,

    pub cells_scanned_count: u64,
    pub bytes_scanned_count: u64,

    pub chunk_size: usize,
}

impl Reader {
    pub fn new(
        instant: fjall::Instant,
        locality_group: PartitionHandle,
        range: Bound<Arc<[u8]>>,
    ) -> Self {
        let snapshot = locality_group.snapshot_at(instant);

        Self {
            partition: locality_group,
            snapshot,
            current_range_start: range,
            buffer: VecDeque::with_capacity(1_000),
            cells_scanned_count: 0,
            bytes_scanned_count: 0,
            chunk_size: 10,
        }
    }

    pub fn chunk_size(mut self, n: usize) -> Self {
        self.chunk_size = n;
        self
    }

    pub fn from_prefix(
        instant: fjall::Instant,
        locality_group: PartitionHandle,
        prefix: &str,
    ) -> fjall::Result<Option<Self>> {
        let Some(range) = Self::get_range_start_from_prefix(instant, &locality_group, prefix)?
        else {
            return Ok(None);
        };

        let reader = Self::new(instant, locality_group, std::ops::Bound::Included(range));

        Ok(Some(reader))
    }

    pub fn get_range_start_from_prefix(
        instant: fjall::Instant,
        locality_group: &PartitionHandle,
        prefix: &str,
    ) -> fjall::Result<Option<Arc<[u8]>>> {
        let snapshot = locality_group.snapshot_at(instant);
        let item = snapshot.prefix(prefix.as_bytes()).next();

        match item {
            Some(item) => {
                let (key, _) = item?;
                Ok(Some(key))
            }
            None => Ok(None),
        }
    }

    // TODO: try to make Peek return a &smoltable::VisitedCell
    pub fn peek(&mut self) -> Option<fjall::Result<VisitedCell>> {
        use std::ops::Bound::{Excluded, Unbounded};

        // First, consume buffer, if filled
        if let Some(cell) = self.buffer.front().cloned() {
            return Some(Ok(cell));
        }

        let mut current_range_start = self.current_range_start.clone();

        loop {
            let collected = self
                .snapshot
                .range((current_range_start.clone(), Unbounded))
                .take(self.chunk_size)
                .collect::<Result<Vec<_>, fjall::LsmError>>();

            // Advance range by querying chunks
            match collected {
                Ok(chunk) => {
                    if chunk.is_empty() {
                        return None;
                    }

                    let chunk_memory = chunk.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>();

                    // TODO: ~20 mb for now
                    if chunk_memory <= 10_000_000 {
                        self.chunk_size = (self.chunk_size * 2).min(128_000);
                    }

                    self.cells_scanned_count += chunk.len() as u64;
                    self.bytes_scanned_count += chunk
                        .iter()
                        .map(|(k, v)| k.len() as u64 + v.len() as u64)
                        .sum::<u64>();

                    let (last_key, _) = chunk.last().unwrap();
                    current_range_start = Excluded(last_key.clone());

                    self.buffer.extend(
                        chunk
                            .into_iter()
                            .map(|(k, v)| VisitedCell::parse(k, &v))
                            .collect::<Vec<_>>(),
                    );

                    if let Some(cell) = self.buffer.front().cloned() {
                        self.current_range_start = current_range_start;
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
    }
}
