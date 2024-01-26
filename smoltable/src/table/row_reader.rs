use super::reader::Reader as TableReader;
use crate::query::row::Input;
use crate::{ColumnFilter, Smoltable, VisitedCell};
use fjall::PartitionHandle;

pub fn get_affected_locality_groups(
    table: &Smoltable,
    column_filter: &Option<ColumnFilter>,
) -> crate::Result<Vec<PartitionHandle>> {
    let mut locality_groups = vec![];

    if let Some(cf) = column_filter {
        match cf {
            ColumnFilter::Key(key) | ColumnFilter::Prefix(key) => {
                let lock = table.locality_groups.read().expect("lock is poisoned");

                let column_family_name = &key.family;

                if table
                    .column_families_that_are_in_default_locality_group()?
                    .contains(column_family_name)
                {
                    locality_groups.push(table.tree.clone());
                } else {
                    let filtered_groups = lock
                        .iter()
                        .filter(|x| x.contains_column_family(column_family_name))
                        .map(|lg| lg.tree.clone());

                    locality_groups.extend(filtered_groups);
                }
            }
            ColumnFilter::Multi(keys) => {
                let lock = table.locality_groups.read().expect("lock is poisoned");

                let mut column_family_names = keys.iter().map(|x| &x.family).collect::<Vec<_>>();
                column_family_names.sort();

                let filtered_groups = lock
                    .iter()
                    .filter(|x| x.contains_column_families(&column_family_names))
                    .map(|lg| lg.tree.clone());

                {
                    let column_families_that_are_in_default_locality_group =
                        table.column_families_that_are_in_default_locality_group()?;

                    if column_family_names.iter().any(|column_family_name| {
                        column_families_that_are_in_default_locality_group
                            .contains(column_family_name)
                    }) {
                        locality_groups.push(table.tree.clone());
                    }
                }

                locality_groups.extend(filtered_groups);
            }
        }
    } else {
        // NOTE: Of course, add the default locality group
        locality_groups.push(table.tree.clone());

        // NOTE: Scan over all locality groups, because we have no column filter
        let lock = table.locality_groups.read().expect("lock is poisoned");
        let all_groups = lock.iter().map(|lg| lg.tree.clone());
        locality_groups.extend(all_groups);
    }

    Ok(locality_groups)
}

pub struct SingleRowReader {
    inner: Option<TableReader>,
    input: Input,
    instant: fjall::Instant,
    locality_groups: Vec<PartitionHandle>,
    cells_scanned_count: u64,
    bytes_scanned_count: u64,
}

impl SingleRowReader {
    pub fn new(table: &Smoltable, instant: fjall::Instant, input: Input) -> crate::Result<Self> {
        let column_filter = input.column.as_ref().and_then(|x| x.filter.clone());
        let locality_groups = get_affected_locality_groups(table, &column_filter)?;

        Ok(Self {
            inner: None,
            input,
            instant,
            locality_groups,
            bytes_scanned_count: 0,
            cells_scanned_count: 0,
        })
    }

    pub fn locality_group_count(&self) -> usize {
        self.locality_groups.len()
    }

    pub fn cells_scanned_count(&self) -> u64 {
        self.cells_scanned_count
            + self
                .inner
                .as_ref()
                .map(|x| x.cells_scanned_count)
                .unwrap_or_default()
    }

    pub fn bytes_scanned_count(&self) -> u64 {
        self.bytes_scanned_count
            + self
                .inner
                .as_ref()
                .map(|x| x.bytes_scanned_count)
                .unwrap_or_default()
    }

    fn take_next_locality_group(&mut self) -> fjall::Result<bool> {
        let column_filter = self.input.column.as_ref().and_then(|x| x.filter.as_ref());

        let locality_group = self.locality_groups.remove(0);

        // TODO: optimize Multi Column filter to only scan columns, not entire column family
        let prefix = match column_filter {
            Some(ColumnFilter::Key(filter)) => filter.build_key(&self.input.row.key),
            _ => format!("{}:", self.input.row.key),
        };

        let Some(reader) = TableReader::from_prefix(self.instant, locality_group, &prefix)? else {
            return Ok(false);
        };
        self.inner = Some(reader);

        Ok(true)
    }
}

impl Iterator for &mut SingleRowReader {
    type Item = fjall::Result<VisitedCell>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.inner.is_none() {
            // Initialize reader
            match self.take_next_locality_group() {
                Ok(has_range) => {
                    if !has_range {
                        return None;
                    }
                }
                Err(e) => return Some(Err(e)),
            };
        }

        loop {
            let mut reader = self.inner.as_mut().unwrap();
            let cell = reader.next();

            match cell {
                Some(cell) => {
                    let cell = match cell {
                        Ok(cell) => cell,
                        Err(e) => return Some(Err(e)),
                    };

                    let column_filter = self.input.column.as_ref().and_then(|x| x.filter.as_ref());

                    if let Some(filter) = column_filter {
                        if !cell.satisfies_column_filter(filter) {
                            continue;
                        }
                    }

                    return Some(Ok(cell));
                }
                None => {
                    self.bytes_scanned_count += reader.bytes_scanned_count;
                    self.cells_scanned_count += reader.cells_scanned_count;
                    self.inner = None;

                    // Iterator is empty
                    if !self.locality_groups.is_empty() {
                        // Load next one
                        match self.take_next_locality_group() {
                            Ok(has_range) => {
                                if !has_range {
                                    return None;
                                }
                            }
                            Err(e) => return Some(Err(e)),
                        };
                    } else {
                        // It's over
                        return None;
                    }
                }
            }
        }
    }
}
