pub mod merge_reader;
pub mod reader;
pub mod row_reader;
pub mod writer;

use self::row_reader::SingleRowReader;
use crate::{
    query::{
        prefix::{Input as QueryPrefixInput, Output as QueryPrefixOutput},
        row::{
            Input as QueryRowInput, Output as QueryRowOutput, RowOptions as QueryRowInputRowOptions,
        },
    },
    table::{
        merge_reader::MergeReader, row_reader::get_affected_locality_groups, writer::timestamp_nano,
    },
    Cell, ColumnFilter, ColumnKey, Row,
};
use fjall::{Batch, Keyspace, PartitionHandle};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, RwLock},
};

// NOTE: Bigger block size is advantageous for Smoltable, because:
// - better compression ratio when block is larger
// - workload is dominated by prefix & range searches
/// The block size used by Smoltable (64 KiB)
pub const BLOCK_SIZE: u32 = /* 64 KiB */ 64 * 1024;

#[derive(Clone)]
pub struct LocalityGroup {
    pub id: Arc<str>,
    pub column_families: Vec<Arc<str>>,
    pub tree: PartitionHandle,
}

impl LocalityGroup {
    pub fn contains_column_family(&self, name: &str) -> bool {
        self.column_families.iter().any(|cf| &**cf == name)
    }

    pub fn contains_column_families(&self, names: &[&String]) -> bool {
        names
            .iter()
            .any(|&name| self.column_families.iter().any(|cf| &**cf == name))
    }
}

pub struct SmoltableInner {
    /// Name
    pub name: Arc<str>,

    /// Keyspace
    pub keyspace: Keyspace,

    /// Manifest partition
    pub manifest: PartitionHandle,

    /// Default locality group
    pub tree: PartitionHandle,

    /// User-defined locality groups
    pub locality_groups: RwLock<Vec<LocalityGroup>>,
}

/// A single smoltable
#[derive(Clone)]
pub struct Smoltable(Arc<SmoltableInner>);

impl std::ops::Deref for Smoltable {
    type Target = SmoltableInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GarbageCollectionOptions {
    pub version_limit: Option<u64>,
    pub ttl_secs: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ColumnFamilyDefinition {
    pub name: String,
    pub gc_settings: GarbageCollectionOptions,
}

#[derive(Debug, Deserialize)]
pub struct CreateColumnFamilyInput {
    pub column_families: Vec<ColumnFamilyDefinition>,
    pub locality_group: Option<bool>,
}

impl Smoltable {
    /// Creates a Smoltable, setting the compaction strategy of its
    /// main partition to the given compaction strategy
    pub fn with_strategy(
        name: &str,
        keyspace: Keyspace,
        strategy: Arc<dyn fjall::compaction::Strategy + Send + Sync>,
    ) -> fjall::Result<Smoltable> {
        let manifest = {
            let config = fjall::PartitionCreateOptions::default()
                .level_count(2)
                .level_ratio(2);

            let tree = keyspace.open_partition(&format!("_man_{name}"), config)?;

            tree.set_max_memtable_size(/* 512 KiB */ 512 * 1_024);

            tree.set_compaction_strategy(Arc::new(fjall::compaction::Levelled {
                target_size: /* 512 KiB */ 512 * 1_024,
                l0_threshold: 2,
            }));

            tree
        };

        let tree = {
            let config = fjall::PartitionCreateOptions::default().block_size(BLOCK_SIZE);
            let tree = keyspace.open_partition(&format!("_dat_{name}"), config)?;
            tree.set_compaction_strategy(strategy);

            tree
        };

        let table = SmoltableInner {
            name: name.into(),
            keyspace,
            tree,
            manifest,
            locality_groups: RwLock::default(),
        };
        let table = Self(Arc::new(table));

        table.load_locality_groups()?;

        // TODO: set block cache(s) if defined

        Ok(table)
    }

    pub fn open(name: &str, keyspace: Keyspace) -> fjall::Result<Smoltable> {
        Self::with_strategy(
            name,
            keyspace,
            Arc::new(fjall::compaction::Levelled {
                target_size: 64 * 1_024 * 1_024,
                l0_threshold: 8,
            }),
        )
    }

    pub(crate) fn get_partition_for_column_family(
        &self,
        cf_name: &str,
    ) -> fjall::Result<PartitionHandle> {
        let locality_groups = self.locality_groups.read().expect("lock is poisoned");

        Ok(locality_groups
            .iter()
            .find(|x| x.column_families.iter().any(|x| &**x == cf_name))
            .map(|x| x.tree.clone())
            .unwrap_or_else(|| self.tree.clone()))
    }

    pub fn list_column_families(&self) -> fjall::Result<Vec<ColumnFamilyDefinition>> {
        let items = self
            .manifest
            .prefix("cf#")
            .into_iter()
            .collect::<Result<Vec<_>, fjall::LsmError>>()?;

        let items = items
            .into_iter()
            .map(|(_k, value)| {
                let value = std::str::from_utf8(&value).expect("should be utf-8");
                serde_json::from_str(value).expect("should deserialize")
            })
            .collect::<Vec<_>>();

        Ok(items)
    }

    fn load_locality_groups(&self) -> fjall::Result<()> {
        let items = self
            .manifest
            .prefix("lg#")
            .into_iter()
            .collect::<Result<Vec<_>, fjall::LsmError>>()?;

        let items = items
            .into_iter()
            .map(|(key, value)| {
                let key = std::str::from_utf8(&key).expect("should be utf-8");
                let id = key.split('#').nth(1).expect("should have ID");

                let value = std::str::from_utf8(&value).expect("should be utf-8");

                let column_families = serde_json::from_str(value).expect("should deserialize");

                log::debug!("Loading locality group {id} <= {:?}", column_families);

                Ok(LocalityGroup {
                    id: id.into(),
                    column_families,
                    tree: {
                        let tree = self.keyspace.open_partition(
                            &format!("_lg_{id}"),
                            fjall::PartitionCreateOptions::default().block_size(BLOCK_SIZE),
                        )?;

                        tree.set_compaction_strategy(Arc::new(fjall::compaction::Levelled {
                            target_size: 64 * 1_024 * 1_024,
                            l0_threshold: 8,
                        }));

                        tree
                    },
                })
            })
            .collect::<fjall::Result<Vec<_>>>()?;

        *self.locality_groups.write().expect("lock is poisoned") = items;

        Ok(())
    }

    /// Creates a dedicated block cache for the table.
    ///
    /// Will be applied after restart automatically, no need to call after every start.
    pub fn set_cache_size(&self, bytes: u64) -> fjall::Result<()> {
        log::debug!("Setting block cache with {bytes}B table {:?}", self.name);

        self.manifest.insert("cache#bytes", bytes.to_be_bytes())?;

        // TODO: create block cache and apply to locality group...or all partitions...

        self.keyspace.persist()?;

        Ok(())
    }

    /// Creates column families.
    ///
    /// Will be persisted, no need to call after every restart.
    pub fn create_column_families(&self, input: &CreateColumnFamilyInput) -> fjall::Result<()> {
        log::debug!(
            "Creating {} column families (locality: {}) for table {:?}",
            input.column_families.len(),
            input.locality_group.unwrap_or_default(),
            self.name
        );

        let mut batch = self.keyspace.batch();

        for item in &input.column_families {
            let str = serde_json::to_string(&item).expect("should serialize");
            batch.insert(&self.manifest, format!("cf#{}", item.name), str);
        }

        let locality_group_id = nanoid::nanoid!();

        if input.locality_group.unwrap_or_default() {
            let names: Vec<Arc<str>> = input
                .column_families
                .iter()
                .map(|x| x.name.clone().into())
                .collect();
            let str = serde_json::to_string(&names).expect("should serialize");

            batch.insert(&self.manifest, format!("lg#{locality_group_id}"), str);
        }

        batch.commit()?;
        self.keyspace.persist()?;

        self.load_locality_groups()?;

        Ok(())
    }

    // TODO: count thrashes block cache

    // TODO: unit test
    pub fn count(&self) -> fjall::Result<(usize, usize)> {
        use reader::Reader as TableReader;

        let mut cell_count = 0;
        let mut row_count = 0;

        // TODO: ideally, we should get counts per locality group
        // TODO: store in table-wide _metrics

        let locality_groups_to_scan = get_affected_locality_groups(self, &None)?;
        let instant = self.keyspace.instant();

        let readers = locality_groups_to_scan
            .into_iter()
            .map(|x| TableReader::new(instant, x, "".into()).chunk_size(100_000))
            .collect::<Vec<_>>();

        let mut current_row_key = None;

        let mut reader = MergeReader::new(readers);
        loop {
            let Some(cell) = (&mut reader).next() else {
                break;
            };

            let cell = cell?;

            cell_count += 1;

            if current_row_key.is_none() || current_row_key.clone().unwrap() != cell.row_key {
                current_row_key = Some(cell.row_key);
                row_count += 1;
            }
        }

        Ok((row_count, cell_count))
    }

    // TODO: GC thrashes block cache

    // TODO: unit test
    pub fn run_version_gc(&self) -> fjall::Result<u64> {
        use reader::Reader as TableReader;

        let gc_options_map = self
            .list_column_families()?
            .into_iter()
            .map(|x| (x.name, x.gc_settings))
            .collect::<HashMap<_, _>>();

        if gc_options_map
            .iter()
            .all(|(_, x)| x.ttl_secs.is_none() && x.version_limit.is_none())
        {
            // NOTE: Short circuit because no GC defined for any column family
            log::info!("{} has no column families with GC, skipping", self.name);
            return Ok(0);
        }

        let mut deleted_cell_count = 0;

        // TODO: ideally, we should get count per column family
        // TODO: store in table-wide _metrics

        let locality_groups_to_scan = get_affected_locality_groups(
            self,
            &Some(ColumnFilter::Multi(
                gc_options_map
                    .keys()
                    .map(|cf| {
                        ColumnKey::try_from(cf.as_str())
                            .expect("should be valid column family name")
                    })
                    .collect(),
            )),
        )?;
        let instant = self.keyspace.instant();

        let mut readers = locality_groups_to_scan
            .into_iter()
            .map(|x| TableReader::new(instant, x, "".into()))
            .collect::<Vec<_>>();

        let mut current_row_key = None;
        let mut current_column_key = None;
        let mut cell_count_in_column = 0;

        // IMPORTANT: Can't use MergeReader because we may need to access
        // a specific partition (locality group)
        for mut reader in &mut readers {
            loop {
                let Some(cell) = reader.next() else {
                    break;
                };

                let cell = cell?;

                if current_row_key.is_none() || current_row_key.clone().unwrap() != cell.row_key {
                    current_row_key = Some(cell.row_key.clone());
                    cell_count_in_column = 0;
                }

                if current_column_key.is_none()
                    || current_column_key.clone().unwrap() != cell.column_key
                {
                    current_column_key = Some(cell.column_key.clone());
                    cell_count_in_column = 0;
                }

                cell_count_in_column += 1;

                let Some(gc_opts) = gc_options_map.get(&cell.column_key.family) else {
                    continue;
                };

                if let Some(version_limit) = gc_opts.version_limit {
                    if version_limit > 0 && cell_count_in_column > version_limit {
                        reader.partition.remove(&cell.raw_key)?;
                        deleted_cell_count += 1;
                    }
                }

                if let Some(ttl_secs) = gc_opts.ttl_secs {
                    if ttl_secs > 0 && cell.timestamp > 0 {
                        let timestamp_secs = cell.timestamp / 1_000 / 1_000 / 1_000;
                        let timestamp_now = timestamp_nano() / 1_000 / 1_000 / 1_000;

                        let lifetime = timestamp_now - timestamp_secs;

                        if lifetime > u128::from(ttl_secs) {
                            reader.partition.remove(&cell.raw_key)?;
                            deleted_cell_count += 1;
                        }
                    }
                }
            }
        }

        Ok(deleted_cell_count)
    }

    // TODO: delete row thrashes block cache

    // TODO: allow deleting specific columns -> DeleteRowInput, also batch + limit it?
    pub fn delete_row(&self, row_key: String) -> fjall::Result<u64> {
        let mut count = 0;

        let mut reader = SingleRowReader::new(
            self,
            self.keyspace.instant(),
            QueryRowInput {
                row: QueryRowInputRowOptions { key: row_key },
                column: None,
            },
        )?;

        for cell in &mut reader {
            let cell = cell?;
            self.tree.remove(&cell.raw_key)?;

            log::trace!("Deleted cell {:?}", cell.raw_key);
            count += 1;
        }

        Ok(count)
    }

    pub fn multi_get(&self, inputs: Vec<QueryRowInput>) -> fjall::Result<QueryPrefixOutput> {
        let mut cells_scanned_count = 0;
        let mut rows_scanned_count = 0;
        let mut bytes_scanned_count = 0;

        let mut rows = Vec::with_capacity(inputs.len());

        for input in inputs {
            let query_result = self.query_row(input)?;
            rows.extend(query_result.row);
            cells_scanned_count += query_result.cells_scanned_count;
            bytes_scanned_count += query_result.bytes_scanned_count;
            rows_scanned_count += 1;
        }

        Ok(QueryPrefixOutput {
            rows,
            rows_scanned_count,
            cells_scanned_count,
            bytes_scanned_count,
        })
    }

    pub fn query_prefix(&self, input: QueryPrefixInput) -> fjall::Result<QueryPrefixOutput> {
        use reader::Reader as TableReader;

        let column_filter = &input.column.as_ref().and_then(|x| x.filter.clone());
        let row_limit = input
            .row
            .as_ref()
            .and_then(|x| x.limit)
            .unwrap_or(u32::from(u16::MAX)) as usize;

        let column_cell_limit = input
            .column
            .as_ref()
            .and_then(|x| x.cell_limit)
            .unwrap_or(u32::from(u16::MAX)) as usize;

        let row_cell_limit = input
            .row
            .as_ref()
            .and_then(|x| x.cell_limit)
            .unwrap_or(u32::from(u16::MAX)) as usize;

        let global_cell_limit = input
            .cell
            .as_ref()
            .and_then(|x| x.limit)
            .unwrap_or(u32::from(u16::MAX)) as usize;

        let locality_groups_to_scan = get_affected_locality_groups(self, column_filter)?;
        let instant = self.keyspace.instant();

        let mut rows_scanned_count = 0;
        let mut cells_scanned_count = 0;
        let mut bytes_scanned_count = 0;
        let mut cell_count = 0; // Cell count over all aggregated rows

        let mut row_sample_counter = 1.0_f32;

        let mut rows: BTreeMap<String, Row> = BTreeMap::new();

        let readers = locality_groups_to_scan
            .into_iter()
            .map(|x| TableReader::new(instant, x, input.prefix.clone()).chunk_size(16_000))
            .collect::<Vec<_>>();

        let mut reader = MergeReader::new(readers);

        loop {
            // We are gonna visit another cell, if the global cell limit is reached
            // we can short circuit out of the loop
            if cell_count >= global_cell_limit {
                break;
            }

            let Some(cell) = (&mut reader).next() else {
                break;
            };

            let cell = cell?;

            if let Some(filter) = column_filter {
                if !cell.satisfies_column_filter(filter) {
                    continue;
                }
            }

            if !rows.contains_key(&cell.row_key) {
                // We are visiting a new row
                rows_scanned_count += 1;

                rows.retain(|_, row| row.column_count() > 0);

                // If the row limit is reached
                // we can short circuit out of the loop
                if rows.len() == row_limit {
                    break;
                }

                if let Some(sample_rate) = input.row.as_ref().and_then(|x| x.sample) {
                    if sample_rate < 1.0 {
                        row_sample_counter += sample_rate;

                        if row_sample_counter < 1.0 {
                            continue;
                        } else {
                            row_sample_counter -= 1.0;
                        }
                    }
                }
            }

            // IMPORTANT: Even if the row has no matching columns, we need to temporarily add it to
            // the buffer, so we can track in which row we are currently in (to increment `rows_scanned_count`)
            // After that it gets removed, if the column count stays 0
            let row = rows.entry(cell.row_key).or_insert_with_key(|key| Row {
                row_key: key.clone(),
                columns: HashMap::default(),
            });

            if row.cell_count() >= row_cell_limit {
                continue;
            }

            let version_history = row
                .columns
                .entry(cell.column_key.family)
                .or_default()
                .entry(cell.column_key.qualifier.unwrap_or(String::from("")))
                .or_default();

            if version_history.len() >= column_cell_limit {
                continue;
            }

            version_history.push(Cell {
                timestamp: cell.timestamp,
                value: cell.value,
            });

            cell_count += 1;
        }

        cells_scanned_count += reader.cells_scanned_count();
        bytes_scanned_count += reader.bytes_scanned_count();

        rows.retain(|_, row| row.column_count() > 0);

        Ok(QueryPrefixOutput {
            rows: rows.into_values().collect(),
            cells_scanned_count,
            rows_scanned_count,
            bytes_scanned_count,
        })
    }

    fn column_families_that_are_in_default_locality_group(&self) -> fjall::Result<Vec<String>> {
        let mut fams = self
            .list_column_families()?
            .into_iter()
            .map(|x| x.name.clone())
            .collect::<Vec<_>>();

        let fams_in_non_default_locality_groups = self
            .locality_groups
            .read()
            .expect("lock is poisoned")
            .iter()
            .flat_map(|x| &x.column_families)
            .map(|x| x.to_string())
            .collect::<HashSet<_>>();

        fams.retain(|x| !fams_in_non_default_locality_groups.contains(x));

        Ok(fams)
    }

    pub fn query_row(&self, input: QueryRowInput) -> fjall::Result<QueryRowOutput> {
        let column_cell_limit = input
            .column
            .as_ref()
            .and_then(|x| x.cell_limit)
            .unwrap_or(u32::from(u16::MAX));

        let row_key = input.row.key.clone();
        let mut columns: HashMap<String, HashMap<String, Vec<Cell>>> = HashMap::new();

        let mut reader = SingleRowReader::new(self, self.keyspace.instant(), input)?;

        for cell in &mut reader {
            let cell = cell?;

            // Append cell
            let version_history = columns
                .entry(cell.column_key.family)
                .or_default()
                .entry(cell.column_key.qualifier.unwrap_or(String::from("_")))
                .or_default();

            if version_history.len() < column_cell_limit as usize {
                version_history.push(Cell {
                    timestamp: cell.timestamp,
                    value: cell.value,
                });
            }

            // TODO: row cell limit

            // TODO: unit test cell limit with multiple columns etc
        }

        let row = if columns.is_empty() {
            None
        } else {
            Some(Row { row_key, columns })
        };

        Ok(QueryRowOutput {
            row,
            cells_scanned_count: reader.cells_scanned_count,
            bytes_scanned_count: reader.bytes_scanned_count,
        })
    }

    fn batch(&self) -> Batch {
        self.keyspace.batch()
    }

    pub fn segment_count(&self) -> usize {
        let mut bytes = self.tree.segment_count();

        for lg_size in self
            .locality_groups
            .read()
            .expect("lock is poisoned")
            .iter()
            .map(|x| x.tree.segment_count())
        {
            bytes += lg_size;
        }

        // TODO: add meta partitions sizes

        bytes
    }

    pub fn disk_space_usage(&self) -> u64 {
        let mut bytes = self.tree.disk_space();

        for lg_size in self
            .locality_groups
            .read()
            .expect("lock is poisoned")
            .iter()
            .map(|x| x.tree.disk_space())
        {
            bytes += lg_size;
        }

        // TODO: add meta partitions sizes

        bytes
    }
}

#[cfg(test)]
mod tests {
    /*  #[test]
    pub fn smoltable_row_order() -> fjall::Result<()> {
        let folder = tempfile::tempdir()?;

        let keyspace = fjall::Config::new(folder.path()).open()?;
        let table = open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "value".to_owned(),
                gc_settings: GarbageCollectionOptions {
                    ttl_secs: None,
                    version_limit: None,
                },
            }],
            locality_group: None,
        })?;

        assert_eq!(1, table.list_column_families()?.len());

        let mut writer = TableWriter::new(table.clone());

        writer.write(&writer::RowWriteItem {
            row_key: "item#1".to_owned(),
            cells: vec![writer::ColumnWriteItem {
                column_key: ColumnKey::try_from("value:")
                    .expect("should be valid column key"),
                timestamp: Some(0),
                value: CellValue::U8(1),
            }],
        })?;
        writer.write(&writer::RowWriteItem {
            row_key: "item#2".to_owned(),
            cells: vec![writer::ColumnWriteItem {
                column_key: ColumnKey::try_from("value:")
                    .expect("should be valid column key"),
                timestamp: Some(0),
                value: CellValue::U8(2),
            }],
        })?;
        writer.write(&writer::RowWriteItem {
            row_key: "item#3".to_owned(),
            cells: vec![writer::ColumnWriteItem {
                column_key: ColumnKey::try_from("value:")
                    .expect("should be valid column key"),
                timestamp: Some(0),
                value: CellValue::U8(3),
            }],
        })?;

        writer.finalize()?;

        let query_result = table.query_row(QueryInput {
            cell_limit: None,
            row_limit: None,
            column_filter: None,
            row_key: "".to_owned(),
        })?;

        assert_eq!(query_result.rows_scanned_count, 3);
        assert_eq!(query_result.cells_scanned_count, 3);

        assert_eq!(
            serde_json::to_value(query_result.rows).unwrap(),
            serde_json::json!([
                {
                    "row_key": "item#1",
                    "columns": {
                        "value": {
                            "": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 1
                                    }
                                }
                            ]
                        }
                    }
                },
                {
                    "row_key": "item#2",
                    "columns": {
                        "value": {
                            "": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 2
                                    }
                                }
                            ]
                        }
                    }
                },
                {
                    "row_key": "item#3",
                    "columns": {
                        "value": {
                            "": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 3
                                    }
                                }
                            ]
                        }
                    }
                }
            ])
        );

        Ok(())
    } */

    /*   #[test]
    pub fn smoltable_row_limit() -> fjall::Result<()> {
        let folder = tempfile::tempdir()?;

        let keyspace = fjall::Config::new(folder.path()).open()?;
        let table = open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "value".to_owned(),
                gc_settings: GarbageCollectionOptions {
                    ttl_secs: None,
                    version_limit: None,
                },
            }],
            locality_group: None,
        })?;

        assert_eq!(1, table.list_column_families()?.len());

        let mut writer = TableWriter::new(table.clone());

        writer.write(&writer::RowWriteItem {
            row_key: "item#1".to_owned(),
            cells: vec![writer::ColumnWriteItem {
                column_key: ColumnKey::try_from("value:")
                    .expect("should be valid column key"),
                timestamp: Some(0),
                value: CellValue::U8(1),
            }],
        })?;
        writer.write(&writer::RowWriteItem {
            row_key: "item#2".to_owned(),
            cells: vec![writer::ColumnWriteItem {
                column_key: ColumnKey::try_from("value:")
                    .expect("should be valid column key"),
                timestamp: Some(0),
                value: CellValue::U8(2),
            }],
        })?;
        writer.write(&writer::RowWriteItem {
            row_key: "item#3".to_owned(),
            cells: vec![writer::ColumnWriteItem {
                column_key: ColumnKey::try_from("value:")
                    .expect("should be valid column key"),
                timestamp: Some(0),
                value: CellValue::U8(3),
            }],
        })?;

        writer.finalize()?;

        let query_result = table.query(QueryInput {
            cell_limit: None,
            row_limit: Some(1),
            column_filter: None,
            row_key: "item#".to_owned(),
        })?;

        assert_eq!(
            serde_json::to_value(query_result.rows).unwrap(),
            serde_json::json!([
                {
                    "row_key": "item#1",
                    "columns": {
                        "value": {
                            "": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 1
                                    }
                                }
                            ]
                        }
                    }
                }
            ])
        );

        Ok(())
    } */

    /* #[test]
    pub fn smoltable_simple_column_filter() -> fjall::Result<()> {
        let folder = tempfile::tempdir()?;

        let keyspace = fjall::Config::new(folder.path()).open()?;
        let table = open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![
                ColumnFamilyDefinition {
                    name: "value".to_owned(),
                    gc_settings: GarbageCollectionOptions {
                    ttl_secs: None,
                    version_limit: None,
                },
                },
                ColumnFamilyDefinition {
                    name: "other".to_owned(),
                    gc_settings: GarbageCollectionOptions {
                    ttl_secs: None,
                    version_limit: None,
                },
                },
            ],
            locality_group: None,
        })?;

        assert_eq!(2, table.list_column_families()?.len());

        let mut writer = TableWriter::new(table.clone());

        writer.write(&writer::RowWriteItem {
            row_key: "item#1".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(1),
                },
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("other:asd")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(44),
                },
            ],
        })?;
        writer.write(&writer::RowWriteItem {
            row_key: "item#2".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(2),
                },
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("other:asd")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(45),
                },
            ],
        })?;
        writer.write(&writer::RowWriteItem {
            row_key: "item#3".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(3),
                },
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("other:asd")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(46),
                },
            ],
        })?;

        writer.finalize()?;

        let query_result = table.query(QueryInput {
            cell_limit: None,
            row_limit: None,
            column_filter: Some(
                ColumnKey::try_from("other:").expect("should be valid column key"),
            ),
            row_key: "item#".to_owned(),
        })?;

        assert_eq!(
            serde_json::to_value(query_result.rows).unwrap(),
            serde_json::json!([
                {
                    "row_key": "item#1",
                    "columns": {
                        "other": {
                            "asd": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 44
                                    }
                                }
                            ]
                        }
                    }
                },
                {
                    "row_key": "item#2",
                    "columns": {
                        "other": {
                            "asd": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 45
                                    }
                                }
                            ]
                        }
                    }
                },
                {
                    "row_key": "item#3",
                    "columns": {
                        "other": {
                            "asd": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 46
                                    }
                                }
                            ]
                        }
                    }
                }
            ])
        );

        let query_result = table.query(QueryInput {
            cell_limit: None,
            row_limit: None,
            column_filter: Some(
                ColumnKey::try_from("other:asd").expect("should be valid column key"),
            ),
            row_key: "item#".to_owned(),
        })?;

        assert_eq!(
            serde_json::to_value(query_result.rows).unwrap(),
            serde_json::json!([
                {
                    "row_key": "item#1",
                    "columns": {
                        "other": {
                            "asd": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 44
                                    }
                                }
                            ]
                        }
                    }
                },
                {
                    "row_key": "item#2",
                    "columns": {
                        "other": {
                            "asd": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 45
                                    }
                                }
                            ]
                        }
                    }
                },
                {
                    "row_key": "item#3",
                    "columns": {
                        "other": {
                            "asd": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 46
                                    }
                                }
                            ]
                        }
                    }
                }
            ])
        );

        Ok(())
    } */

    /* #[test]
    pub fn smoltable_multiple_column_families() -> fjall::Result<()> {
        let folder = tempfile::tempdir()?;

        let keyspace = fjall::Config::new(folder.path()).open()?;
        let table = open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![
                ColumnFamilyDefinition {
                    name: "value".to_owned(),
                    gc_settings: GarbageCollectionOptions {
                    ttl_secs: None,
                    version_limit: None,
                },
                },
                ColumnFamilyDefinition {
                    name: "other".to_owned(),
                    gc_settings: GarbageCollectionOptions {
                    ttl_secs: None,
                    version_limit: None,
                },
                },
            ],
            locality_group: None,
        })?;

        assert_eq!(2, table.list_column_families()?.len());

        let mut writer = TableWriter::new(table.clone());

        writer.write(&writer::RowWriteItem {
            row_key: "item#1".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(1),
                },
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("other:asd")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(44),
                },
            ],
        })?;
        writer.write(&writer::RowWriteItem {
            row_key: "item#2".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(2),
                },
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("other:asd")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(45),
                },
            ],
        })?;
        writer.write(&writer::RowWriteItem {
            row_key: "item#3".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(3),
                },
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("other:asd")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(46),
                },
            ],
        })?;

        writer.finalize()?;

        let query_result = table.query(QueryInput {
            cell_limit: None,
            row_limit: None,
            column_filter: None,
            row_key: "".to_owned(),
        })?;

        assert_eq!(query_result.rows_scanned_count, 3);
        assert_eq!(query_result.cells_scanned_count, 6);

        assert_eq!(
            serde_json::to_value(query_result.rows).unwrap(),
            serde_json::json!([
                {
                    "row_key": "item#1",
                    "columns": {
                        "value": {
                            "": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 1
                                    }
                                }
                            ]
                        },
                        "other": {
                            "asd": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 44
                                    }
                                }
                            ]
                        }
                    }
                },
                {
                    "row_key": "item#2",
                    "columns": {
                        "value": {
                            "": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 2
                                    }
                                }
                            ]
                        },
                        "other": {
                            "asd": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 45
                                    }
                                }
                            ]
                        }
                    }
                },
                {
                    "row_key": "item#3",
                    "columns": {
                        "value": {
                            "": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 3
                                    }
                                }
                            ]
                        },
                        "other": {
                            "asd": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 46
                                    }
                                }
                            ]
                        }
                    }
                }
            ])
        );

        Ok(())
    } */

    /*  #[test]
    pub fn smoltable_multiple_column_families_in_locality_groups() -> fjall::Result<()> {
        let folder = tempfile::tempdir()?;

        let keyspace = fjall::Config::new(folder.path()).open()?;
        let table = open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "value".to_owned(),
                gc_settings: GarbageCollectionOptions {
                    ttl_secs: None,
                    version_limit: None,
                },
            }],
            locality_group: None,
        })?;

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "other".to_owned(),
                gc_settings: GarbageCollectionOptions {
                    ttl_secs: None,
                    version_limit: None,
                },
            }],
            locality_group: Some(true),
        })?;

        assert_eq!(2, table.list_column_families()?.len());

        let mut writer = TableWriter::new(table.clone());

        writer.write(&writer::RowWriteItem {
            row_key: "item#1".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(1),
                },
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("other:asd")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(44),
                },
            ],
        })?;
        writer.write(&writer::RowWriteItem {
            row_key: "item#2".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(2),
                },
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("other:asd")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(45),
                },
            ],
        })?;
        writer.write(&writer::RowWriteItem {
            row_key: "item#3".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(3),
                },
                writer::ColumnWriteItem {
                    column_key: ColumnKey::try_from("other:asd")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(46),
                },
            ],
        })?;

        writer.finalize()?;

        let query_result = table.query(QueryInput {
            cell_limit: None,
            row_limit: None,
            column_filter: None,
            row_key: "".to_owned(),
        })?;

        assert_eq!(query_result.rows_scanned_count, 3);
        assert_eq!(query_result.cells_scanned_count, 6);

        assert_eq!(
            serde_json::to_value(query_result.rows).unwrap(),
            serde_json::json!([
                {
                    "row_key": "item#1",
                    "columns": {
                        "value": {
                            "": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 1
                                    }
                                }
                            ]
                        },
                        "other": {
                            "asd": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 44
                                    }
                                }
                            ]
                        }
                    }
                },
                {
                    "row_key": "item#2",
                    "columns": {
                        "value": {
                            "": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 2
                                    }
                                }
                            ]
                        },
                        "other": {
                            "asd": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 45
                                    }
                                }
                            ]
                        }
                    }
                },
                {
                    "row_key": "item#3",
                    "columns": {
                        "value": {
                            "": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 3
                                    }
                                }
                            ]
                        },
                        "other": {
                            "asd": [
                                {
                                    "timestamp": 0,
                                    "value": {
                                        "U8": 46
                                    }
                                }
                            ]
                        }
                    }
                }
            ])
        );

        Ok(())
    } */
}
