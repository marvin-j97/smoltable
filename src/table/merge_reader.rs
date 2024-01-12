use super::{cell::VisitedCell, reader::Reader as TableReader};

pub struct MergeReader {
    readers: Vec<TableReader>,
}

impl MergeReader {
    pub fn new(readers: Vec<TableReader>) -> Self {
        Self { readers }
    }

    pub fn cells_scanned_count(&self) -> u64 {
        self.readers.iter().map(|x| x.cells_scanned_count).sum()
    }

    pub fn bytes_scanned_count(&self) -> u64 {
        self.readers.iter().map(|x| x.bytes_scanned_count).sum()
    }
}

impl Iterator for &mut MergeReader {
    type Item = fjall::Result<VisitedCell>;

    fn next(&mut self) -> Option<Self::Item> {
        // Peek all readers
        let cells = self
            .readers
            .iter_mut()
            .map(TableReader::peek)
            .collect::<Vec<_>>();

        // Throw if error
        let cells = match cells
            .into_iter()
            .map(Option::transpose)
            .collect::<fjall::Result<Vec<Option<VisitedCell>>>>()
        {
            Ok(cells) => cells,
            Err(e) => return Some(Err(e)),
        };

        // Get index of reader that has lowest row
        let lowest_idx = cells
            .into_iter()
            .enumerate()
            .filter(|(_, cell)| Option::is_some(cell))
            .map(|(idx, cell)| (idx, cell.unwrap()))
            .max_by(|(_, a), (_, b)| a.raw_key.cmp(&b.raw_key));

        let Some((lowest_idx, _)) = lowest_idx else {
            // No more items
            return None;
        };

        // Consume from iterator with lowest item
        let cell = match self.readers.get_mut(lowest_idx).unwrap().next().transpose() {
            Ok(cell) => cell,
            Err(e) => return Some(Err(e)),
        };

        cell.map(Ok)
    }
}
