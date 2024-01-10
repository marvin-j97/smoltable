use super::{
    reader::{Reader as TableReader, VisitedCell},
    satisfies_column_filter, ColumnFilter, Smoltable,
};
use fjall::PartitionHandle;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QueryRowInputRowOptions {
    pub key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(transparent)]
pub struct QueryRowInputColumnOptions {
    #[serde(flatten)]
    pub filter: Option<ColumnFilter>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QueryRowInputCellOptions {
    pub limit: Option<u16>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QueryRowInput {
    pub row: QueryRowInputRowOptions,
    pub column: Option<QueryRowInputColumnOptions>,
    pub cell: Option<QueryRowInputCellOptions>,
}

pub fn get_affected_locality_groups(
    table: &Smoltable,
    column_filter: &Option<ColumnFilter>,
) -> fjall::Result<Vec<PartitionHandle>> {
    let mut locality_groups = vec![];

    if let Some(cf) = column_filter {
        match cf {
            ColumnFilter::Key(key) | ColumnFilter::Prefix(key) => {
                let lock = table.locality_groups.read().expect("lock is poisoned");

                let column_family_name = &key.family;

                let filtered_groups = lock
                    .iter()
                    .filter(|x| x.contains_column_family(column_family_name))
                    .map(|lg| lg.tree.clone());

                locality_groups.extend(filtered_groups);

                if table
                    .column_families_that_are_in_default_locality_group()?
                    .contains(column_family_name)
                {
                    locality_groups.push(table.tree.clone());
                }
            }
            ColumnFilter::Multi(keys) => {
                let lock = table.locality_groups.read().expect("lock is poisoned");

                let column_family_names = keys.iter().map(|x| &x.family).collect::<Vec<_>>();

                let filtered_groups = lock
                    .iter()
                    .filter(|x| x.contains_column_families(&column_family_names))
                    .map(|lg| lg.tree.clone());

                locality_groups.extend(filtered_groups);

                let column_families_that_are_in_default_locality_group =
                    table.column_families_that_are_in_default_locality_group()?;

                if column_family_names.iter().any(|column_family_name| {
                    column_families_that_are_in_default_locality_group.contains(column_family_name)
                }) {
                    locality_groups.push(table.tree.clone());
                }
            }
        }
    } else {
        // NOTE: Scan over all locality groups, because we have no column filter
        let lock = table.locality_groups.read().expect("lock is poisoned");
        let all_groups = lock.iter().map(|lg| lg.tree.clone());
        locality_groups.extend(all_groups);

        // NOTE: Of course, also add the default locality group
        locality_groups.push(table.tree.clone());
    }

    Ok(locality_groups)
}

pub struct SingleRowReader {
    inner: Option<TableReader>,
    input: QueryRowInput,
    instant: fjall::Instant,
    locality_groups: Vec<PartitionHandle>,
    pub cells_scanned_count: u64,
    pub bytes_scanned_count: u64,
}

impl SingleRowReader {
    pub fn new(
        table: &Smoltable,
        instant: fjall::Instant,
        input: QueryRowInput,
    ) -> fjall::Result<Self> {
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

    fn take_next_locality_group(&mut self) {
        let column_filter = self.input.column.as_ref().and_then(|x| x.filter.as_ref());

        let locality_group = self.locality_groups.remove(0);

        // TODO: optimize Multi Column filter to only scan columns, not entire column family
        let prefix = match column_filter {
            Some(ColumnFilter::Key(filter)) => filter.build_key(&self.input.row.key),
            _ => format!("{}:", self.input.row.key),
        };

        log::debug!(
            "Performing cell scan over {:?} with prefix {prefix:?}",
            locality_group.name
        );

        self.inner = Some(TableReader::new(self.instant, locality_group, prefix));
    }
}

impl Iterator for &mut SingleRowReader {
    type Item = fjall::Result<VisitedCell>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.inner.is_none() {
            // Initialize reader
            self.take_next_locality_group();
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
                        if !satisfies_column_filter(&cell, filter) {
                            continue;
                        }
                    }

                    return Some(Ok(cell));
                }
                None => {
                    self.bytes_scanned_count += reader.bytes_scanned_count;
                    self.cells_scanned_count += reader.cells_scanned_count;

                    // Iterator is empty
                    if !self.locality_groups.is_empty() {
                        // Load next one
                        self.take_next_locality_group();
                    } else {
                        // It's over
                        return None;
                    }
                }
            }
        }
    }
}
