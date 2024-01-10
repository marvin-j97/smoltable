pub mod cell;
pub mod reader;
pub mod single_row_reader;
pub mod writer;

use self::{
    cell::{Cell, Row, Value as CellValue},
    reader::VisitedCell,
    single_row_reader::{QueryRowInput, QueryRowInputRowOptions, SingleRowReader},
};
use crate::{column_key::ParsedColumnKey, table::single_row_reader::get_affected_locality_groups};
use fjall::{Batch, Keyspace, PartitionHandle};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, RwLock},
};

fn satisfies_column_filter(cell: &VisitedCell, filter: &ColumnFilter) -> bool {
    match filter {
        ColumnFilter::Key(key) => {
            if cell.column_key.family != key.family {
                return false;
            }

            if let Some(cq_filter) = &key.qualifier {
                if cell.column_key.qualifier.as_deref().unwrap_or("") != cq_filter {
                    return false;
                }
            }

            true
        }
        ColumnFilter::Multi(keys) => {
            for key in keys {
                if cell.column_key.family != key.family {
                    continue;
                }

                if let Some(cq_filter) = &key.qualifier {
                    if cell.column_key.qualifier.as_deref().unwrap_or("") == cq_filter {
                        return true;
                    }
                } else {
                    return true;
                }
            }

            false
        }
        ColumnFilter::Prefix(key) => {
            if cell.column_key.family != key.family {
                return false;
            }

            if let Some(cq_filter) = &key.qualifier {
                if !cell
                    .column_key
                    .qualifier
                    .as_deref()
                    .unwrap_or("")
                    .starts_with(cq_filter)
                {
                    return false;
                }
            }

            true
        }
    }
}

pub const BLOCK_SIZE: u32 = /* 32 KiB */ 32 * 1024;

#[derive(Clone)]
pub struct LocalityGroup {
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
    /// Keyspace
    pub keyspace: Keyspace,

    /// Manifest partition
    pub manifest: PartitionHandle,

    // TODO: metrics
    /// Default locality group
    pub tree: PartitionHandle,

    /// User-defined locality groups
    pub locality_groups: RwLock<Vec<LocalityGroup>>,
}

#[derive(Clone)]
pub struct Smoltable(Arc<SmoltableInner>);

impl std::ops::Deref for Smoltable {
    type Target = SmoltableInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ColumnFilter {
    #[serde(rename = "key")]
    Key(ParsedColumnKey),

    #[serde(rename = "multi_key")]
    Multi(Vec<ParsedColumnKey>),

    #[serde(rename = "prefix")]
    Prefix(ParsedColumnKey),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QueryPrefixInputRowOptions {
    pub limit: Option<u16>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(transparent)]
pub struct QueryPrefixInputColumnOptions {
    #[serde(flatten)]
    pub filter: Option<ColumnFilter>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QueryPrefixInputCellOptions {
    pub limit: Option<u16>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QueryPrefixInput {
    pub prefix: String,
    pub column: Option<QueryPrefixInputColumnOptions>,
    pub row: Option<QueryPrefixInputRowOptions>,
    pub cell: Option<QueryPrefixInputCellOptions>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QueryOutput {
    pub rows: Vec<Row>,
    pub cells_scanned_count: u64,
    pub rows_scanned_count: u64,
    pub bytes_scanned_count: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QueryRowOutput {
    pub row: Option<Row>,
    pub cells_scanned_count: u64,
    pub bytes_scanned_count: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ColumnFamilyDefinition {
    pub name: String,
    pub version_limit: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct CreateColumnFamilyInput {
    pub column_families: Vec<ColumnFamilyDefinition>,
    pub locality_group: Option<bool>,
}

impl Smoltable {
    pub fn open(name: &str, keyspace: Keyspace) -> fjall::Result<Smoltable> {
        let manifest = {
            let config = fjall::PartitionCreateOptions::default().level_count(2);
            let tree = keyspace.open_partition(&format!("_man_{name}"), config)?;
            tree.set_compaction_strategy(Arc::new(fjall::compaction::Levelled {
                target_size: 4 * 1_024 * 1_024,
                l0_threshold: 2,
            }));
            tree
        };

        let tree = {
            let config = fjall::PartitionCreateOptions::default().block_size(BLOCK_SIZE);
            let tree = keyspace.open_partition(&format!("_dat_{name}"), config)?;
            tree.set_compaction_strategy(Arc::new(fjall::compaction::Levelled {
                target_size: 64 * 1_024 * 1_024,
                l0_threshold: 2,
            }));
            tree
        };

        let table = SmoltableInner {
            keyspace,
            tree,
            manifest,
            locality_groups: RwLock::default(),
        };
        let table = Self(Arc::new(table));

        table.load_locality_groups()?;

        Ok(table)
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
            .prefix("cf:")
            .into_iter()
            .collect::<lsm_tree::Result<Vec<_>>>()?;

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
            .prefix("lg:")
            .into_iter()
            .collect::<lsm_tree::Result<Vec<_>>>()?;

        let items = items
            .into_iter()
            .map(|(key, value)| {
                let key = std::str::from_utf8(&key).expect("should be utf-8");
                let id = key.split(':').nth(1).expect("should have ID");

                let value = std::str::from_utf8(&value).expect("should be utf-8");

                let column_families = serde_json::from_str(value).expect("should deserialize");

                log::debug!("Loading locality group {id} <= {:?}", column_families);

                Ok(LocalityGroup {
                    column_families,
                    tree: self.keyspace.open_partition(
                        &format!("_lg_{id}"),
                        fjall::PartitionCreateOptions::default(),
                    )?,
                })
            })
            .collect::<fjall::Result<Vec<_>>>()?;

        *self.locality_groups.write().expect("lock is poisoned") = items;

        Ok(())
    }

    pub fn create_column_families(&self, input: &CreateColumnFamilyInput) -> fjall::Result<()> {
        log::debug!("Creating column families for table");

        let mut batch = self.keyspace.batch();

        for item in &input.column_families {
            let str = serde_json::to_string(&item).expect("should serialize");
            batch.insert(&self.manifest, format!("cf:{}", item.name), str);
        }

        let locality_group_id = nanoid::nanoid!();

        if input.locality_group.unwrap_or_default() {
            let names: Vec<Arc<str>> = input
                .column_families
                .iter()
                .map(|x| x.name.clone().into())
                .collect();
            let str = serde_json::to_string(&names).expect("should serialize");

            batch.insert(&self.manifest, format!("lg:{locality_group_id}"), str);
        }

        batch.commit()?;
        self.keyspace.persist()?;

        self.load_locality_groups()?;

        Ok(())
    }

    /* pub fn from_tree(keyspace: Keyspace, tree: PartitionHandle) -> fjall::Result<Smoltable> {
        Ok(Self {
            keyspace,
            tree,
            manifest: keys
            locality_groups: vec![],
        })
    } */

    pub fn count(&self) -> fjall::Result<(usize, usize)> {
        let mut cell_count = 0;
        let mut row_count = 0;

        // TODO: ideally, we should get counts per locality group
        // TODO: store in table-wide _metrics

        /*  let mut reader = TableReader::new(
            self.clone(),
            reader::Input {
                prefix: "".into(),
                /*  cell_limit: None,
                column_filter: None,
                row_limit: None, */
            },
        );

        let mut current_row: Row = Row {
            row_key: "".into(),
            columns: Default::default(),
        };

        for cell in &mut reader {
            let cell = cell?;
            cell_count += 1;

            if current_row.row_key.is_empty() {
                current_row.row_key = cell.row_key.clone();
            }

            if cell.row_key != current_row.row_key {
                // Rotate over to new row
                row_count += 1;

                current_row = Row {
                    row_key: cell.row_key.clone(),
                    columns: HashMap::default(),
                };
            }
        } */

        Ok((row_count, cell_count))
    }

    pub fn delete_row(&self, row_key: String) -> fjall::Result<u64> {
        let mut count = 0;

        let mut reader = SingleRowReader::new(
            self,
            self.keyspace.instant(),
            QueryRowInput {
                row: QueryRowInputRowOptions { key: row_key },
                column: None,
                cell: None,
            },
        )?;

        for cell in &mut reader {
            let cell = cell?;
            self.tree.remove(cell.raw_key)?;
            count += 1;
        }

        Ok(count)
    }

    pub fn multi_get(&self, inputs: Vec<QueryRowInput>) -> fjall::Result<QueryOutput> {
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

        Ok(QueryOutput {
            rows,
            rows_scanned_count,
            cells_scanned_count,
            bytes_scanned_count,
        })
    }

    // TODO: need a PrefixReader... for delete and count

    pub fn query_prefix(&self, input: QueryPrefixInput) -> fjall::Result<QueryOutput> {
        let column_filter = &input.column.as_ref().and_then(|x| x.filter.clone());
        let row_limit = input.row.as_ref().and_then(|x| x.limit).unwrap_or(u16::MAX) as usize;
        let cell_limit = input
            .cell
            .as_ref()
            .and_then(|x| x.limit)
            .unwrap_or(u16::MAX) as usize;

        let locality_groups_to_scan = get_affected_locality_groups(self, &column_filter)?;
        let instant = self.keyspace.instant();

        let mut rows_scanned_count = 0;
        let mut cells_scanned_count = 0;
        let mut bytes_scanned_count = 0;

        let mut rows: BTreeMap<String, Row> = BTreeMap::new();

        for locality_group in locality_groups_to_scan {
            let mut reader = reader::Reader::new(instant, locality_group, input.prefix.clone());

            for cell in &mut reader {
                let cell = cell?;

                if let Some(filter) = column_filter {
                    if !satisfies_column_filter(&cell, filter) {
                        continue;
                    }
                }

                let current_row = rows.entry(cell.row_key).or_insert_with_key(|key| Row {
                    row_key: key.clone(),
                    columns: HashMap::default(),
                });

                // Append cell
                let version_history = current_row
                    .columns
                    .entry(cell.column_key.family)
                    .or_default()
                    .entry(cell.column_key.qualifier.unwrap_or(String::from("_")))
                    .or_default();

                if version_history.len() < cell_limit {
                    version_history.push(Cell {
                        timestamp: cell.timestamp,
                        value: cell.value,
                    });
                }

                if version_history.len() >= cell_limit && rows.len() > (row_limit - 1) {
                    break;
                }
            }

            cells_scanned_count += reader.cells_scanned_count;
            bytes_scanned_count += reader.bytes_scanned_count;
        }

        // TODO: fix rows scanned count... not trivial??? need to keep track of EVERY row ID...

        Ok(QueryOutput {
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

    // TODO: need a SingleRowReader that emits cells of a single row (QueryRowInput)

    pub fn query_row(&self, input: QueryRowInput) -> fjall::Result<QueryRowOutput> {
        let cell_limit: usize = input
            .cell
            .as_ref()
            .and_then(|x| x.limit)
            .unwrap_or(u16::MAX)
            .into();

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

            if version_history.len() < cell_limit {
                version_history.push(Cell {
                    timestamp: cell.timestamp,
                    value: cell.value,
                });
            }

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

    pub fn batch(&self) -> Batch {
        self.keyspace.batch()
    }

    pub fn disk_space_usage(&self) -> u64 {
        self.tree.disk_space()
    }

    pub fn cached_block_count(&self) -> usize {
        // self.tree.block_cache_size()
        // TODO:
        0
    }

    pub fn cache_memory_usage(&self) -> usize {
        // self.tree.block_cache_size() * (self.tree.config().block_size as usize)
        // TODO:
        0
    }
}

#[cfg(test)]
mod tests {
    use super::single_row_reader::{
        QueryRowInput, QueryRowInputColumnOptions, QueryRowInputRowOptions,
    };
    use super::writer::Writer as TableWriter;
    use super::*;
    use crate::column_key::ParsedColumnKey;
    use test_log::test;

    #[test]
    pub fn smoltable_write_read_row() -> fjall::Result<()> {
        let folder = tempfile::tempdir()?;

        let keyspace = fjall::Config::new(folder.path()).open()?;
        let table = Smoltable::open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "value".to_owned(),
                version_limit: None,
            }],
            locality_group: None,
        })?;

        assert_eq!(1, table.list_column_families()?.len());

        let mut writer = TableWriter::new(table.clone());

        writer.write(&writer::RowWriteItem {
            row_key: "test".to_owned(),
            cells: vec![writer::ColumnWriteItem {
                column_key: ParsedColumnKey::try_from("value:")
                    .expect("should be valid column key"),
                timestamp: Some(0),
                value: CellValue::String("hello".to_owned()),
            }],
        })?;

        writer.finalize()?;

        let query_result = table.query_row(QueryRowInput {
            cell: None,
            column: None,
            row: QueryRowInputRowOptions {
                key: "test".to_owned(),
            },
        })?;

        assert_eq!(query_result.cells_scanned_count, 1);

        assert_eq!(
            serde_json::to_value(query_result.row).unwrap(),
            serde_json::json!({
                "row_key": "test",
                "columns": {
                    "value": {
                        "": [
                            {
                                "timestamp": 0,
                                "value": {
                                    "String": "hello"
                                }
                            }
                        ]
                    }
                }
            })
        );

        Ok(())
    }

    #[test]
    pub fn smoltable_write_read_row_multiple_families() -> fjall::Result<()> {
        let folder = tempfile::tempdir()?;

        let keyspace = fjall::Config::new(folder.path()).open()?;
        let table = Smoltable::open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![
                ColumnFamilyDefinition {
                    name: "value".to_owned(),
                    version_limit: None,
                },
                ColumnFamilyDefinition {
                    name: "another".to_owned(),
                    version_limit: None,
                },
            ],
            locality_group: None,
        })?;

        assert_eq!(2, table.list_column_families()?.len());

        let mut writer = TableWriter::new(table.clone());

        writer.write(&writer::RowWriteItem {
            row_key: "test".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::String("hello".to_owned()),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("another:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::String("hello2".to_owned()),
                },
            ],
        })?;

        writer.finalize()?;

        let query_result = table.query_row(QueryRowInput {
            cell: None,
            column: None,
            row: QueryRowInputRowOptions {
                key: "test".to_owned(),
            },
        })?;

        assert_eq!(query_result.cells_scanned_count, 2);

        assert_eq!(
            serde_json::to_value(query_result.row).unwrap(),
            serde_json::json!({
                "row_key": "test",
                "columns": {
                    "value": {
                        "": [
                            {
                                "timestamp": 0,
                                "value": {
                                    "String": "hello"
                                }
                            }
                        ]
                    },
                    "another": {
                        "": [
                            {
                                "timestamp": 0,
                                "value": {
                                    "String": "hello2"
                                }
                            }
                        ]
                    }
                }
            })
        );

        Ok(())
    }

    #[test]
    pub fn smoltable_write_read_row_simple_column_filter() -> fjall::Result<()> {
        let folder = tempfile::tempdir()?;

        let keyspace = fjall::Config::new(folder.path()).open()?;
        let table = Smoltable::open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![
                ColumnFamilyDefinition {
                    name: "value".to_owned(),
                    version_limit: None,
                },
                ColumnFamilyDefinition {
                    name: "another".to_owned(),
                    version_limit: None,
                },
            ],
            locality_group: None,
        })?;

        assert_eq!(2, table.list_column_families()?.len());

        let mut writer = TableWriter::new(table.clone());

        writer.write(&writer::RowWriteItem {
            row_key: "test".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::String("hello".to_owned()),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("another:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::String("hello2".to_owned()),
                },
            ],
        })?;

        writer.finalize()?;

        let query_result = table.query_row(QueryRowInput {
            cell: None,
            column: Some(QueryRowInputColumnOptions {
                filter: Some(ColumnFilter::Key(
                    ParsedColumnKey::try_from("value:").unwrap(),
                )),
            }),
            row: QueryRowInputRowOptions {
                key: "test".to_owned(),
            },
        })?;

        assert_eq!(query_result.cells_scanned_count, 1);

        assert_eq!(
            serde_json::to_value(query_result.row).unwrap(),
            serde_json::json!({
                "row_key": "test",
                "columns": {
                    "value": {
                        "": [
                            {
                                "timestamp": 0,
                                "value": {
                                    "String": "hello"
                                }
                            }
                        ]
                    }
                }
            })
        );

        Ok(())
    }

    #[test]
    pub fn smoltable_write_read_row_multi_column_filter() -> fjall::Result<()> {
        let folder = tempfile::tempdir()?;

        let keyspace = fjall::Config::new(folder.path()).open()?;
        let table = Smoltable::open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![
                ColumnFamilyDefinition {
                    name: "value".to_owned(),
                    version_limit: None,
                },
                ColumnFamilyDefinition {
                    name: "another".to_owned(),
                    version_limit: None,
                },
                ColumnFamilyDefinition {
                    name: "another_one".to_owned(),
                    version_limit: None,
                },
            ],
            locality_group: None,
        })?;

        assert_eq!(3, table.list_column_families()?.len());

        let mut writer = TableWriter::new(table.clone());

        writer.write(&writer::RowWriteItem {
            row_key: "test".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::String("hello".to_owned()),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("another:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::String("hello2".to_owned()),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("another_one:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::String("hello3".to_owned()),
                },
            ],
        })?;

        writer.finalize()?;

        let query_result = table.query_row(QueryRowInput {
            cell: None,
            column: Some(QueryRowInputColumnOptions {
                filter: Some(ColumnFilter::Multi(vec![
                    ParsedColumnKey::try_from("value:").unwrap(),
                    ParsedColumnKey::try_from("another_one:").unwrap(),
                ])),
            }),
            row: QueryRowInputRowOptions {
                key: "test".to_owned(),
            },
        })?;

        assert_eq!(query_result.cells_scanned_count, 3);

        assert_eq!(
            serde_json::to_value(query_result.row).unwrap(),
            serde_json::json!({
                "row_key": "test",
                "columns": {
                    "value": {
                        "": [
                            {
                                "timestamp": 0,
                                "value": {
                                    "String": "hello"
                                }
                            }
                        ]
                    },
                    "another_one": {
                        "": [
                            {
                                "timestamp": 0,
                                "value": {
                                    "String": "hello3"
                                }
                            }
                        ]
                    }
                }
            })
        );

        Ok(())
    }

    #[test]
    pub fn smoltable_write_read_row_multiple_locality_groups() -> fjall::Result<()> {
        let folder = tempfile::tempdir()?;

        let keyspace = fjall::Config::new(folder.path()).open()?;
        let table = Smoltable::open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "value".to_owned(),
                version_limit: None,
            }],
            locality_group: None,
        })?;
        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "another".to_owned(),
                version_limit: None,
            }],
            locality_group: Some(true),
        })?;

        assert_eq!(2, table.list_column_families()?.len());

        let mut writer = TableWriter::new(table.clone());

        writer.write(&writer::RowWriteItem {
            row_key: "test".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::String("hello".to_owned()),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("another:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::String("hello2".to_owned()),
                },
            ],
        })?;

        writer.finalize()?;

        let query_result = table.query_row(QueryRowInput {
            cell: None,
            column: None,
            row: QueryRowInputRowOptions {
                key: "test".to_owned(),
            },
        })?;

        assert_eq!(query_result.cells_scanned_count, 2);

        assert_eq!(
            serde_json::to_value(query_result.row).unwrap(),
            serde_json::json!({
                "row_key": "test",
                "columns": {
                    "value": {
                        "": [
                            {
                                "timestamp": 0,
                                "value": {
                                    "String": "hello"
                                }
                            }
                        ]
                    },
                    "another": {
                        "": [
                            {
                                "timestamp": 0,
                                "value": {
                                    "String": "hello2"
                                }
                            }
                        ]
                    }
                }
            })
        );

        Ok(())
    }

    #[test]
    pub fn smoltable_write_read_row_simple_column_filter_multiple_locality_groups(
    ) -> fjall::Result<()> {
        let folder = tempfile::tempdir()?;

        let keyspace = fjall::Config::new(folder.path()).open()?;
        let table = Smoltable::open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "value".to_owned(),
                version_limit: None,
            }],
            locality_group: None,
        })?;
        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "another".to_owned(),
                version_limit: None,
            }],
            locality_group: Some(true),
        })?;

        assert_eq!(2, table.list_column_families()?.len());

        let mut writer = TableWriter::new(table.clone());

        writer.write(&writer::RowWriteItem {
            row_key: "test".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::String("hello".to_owned()),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("another:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::String("hello2".to_owned()),
                },
            ],
        })?;

        writer.finalize()?;

        let query_result = table.query_row(QueryRowInput {
            cell: None,
            column: Some(QueryRowInputColumnOptions {
                filter: Some(ColumnFilter::Key(
                    ParsedColumnKey::try_from("value:").unwrap(),
                )),
            }),
            row: QueryRowInputRowOptions {
                key: "test".to_owned(),
            },
        })?;

        assert_eq!(query_result.cells_scanned_count, 1);

        assert_eq!(
            serde_json::to_value(query_result.row).unwrap(),
            serde_json::json!({
                "row_key": "test",
                "columns": {
                    "value": {
                        "": [
                            {
                                "timestamp": 0,
                                "value": {
                                    "String": "hello"
                                }
                            }
                        ]
                    }
                }
            })
        );

        let query_result = table.query_row(QueryRowInput {
            cell: None,
            column: Some(QueryRowInputColumnOptions {
                filter: Some(ColumnFilter::Key(
                    ParsedColumnKey::try_from("another:").unwrap(),
                )),
            }),
            row: QueryRowInputRowOptions {
                key: "test".to_owned(),
            },
        })?;

        assert_eq!(query_result.cells_scanned_count, 1);

        assert_eq!(
            serde_json::to_value(query_result.row).unwrap(),
            serde_json::json!({
                "row_key": "test",
                "columns": {
                    "another": {
                        "": [
                            {
                                "timestamp": 0,
                                "value": {
                                    "String": "hello2"
                                }
                            }
                        ]
                    }
                }
            })
        );

        Ok(())
    }

    #[test]
    pub fn smoltable_write_read_row_multi_column_filter_multiple_locality_groups(
    ) -> fjall::Result<()> {
        let folder = tempfile::tempdir()?;

        let keyspace = fjall::Config::new(folder.path()).open()?;
        let table = Smoltable::open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "value".to_owned(),
                version_limit: None,
            }],
            locality_group: None,
        })?;
        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "another".to_owned(),
                version_limit: None,
            }],
            locality_group: Some(true),
        })?;
        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "another_one".to_owned(),
                version_limit: None,
            }],
            locality_group: Some(true),
        })?;

        assert_eq!(3, table.list_column_families()?.len());

        let mut writer = TableWriter::new(table.clone());

        writer.write(&writer::RowWriteItem {
            row_key: "test".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::String("hello".to_owned()),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("another:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::String("hello2".to_owned()),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("another_one:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::String("hello3".to_owned()),
                },
            ],
        })?;

        writer.finalize()?;

        let query_result = table.query_row(QueryRowInput {
            cell: None,
            column: Some(QueryRowInputColumnOptions {
                filter: Some(ColumnFilter::Multi(vec![ParsedColumnKey::try_from(
                    "another_one:",
                )
                .unwrap()])),
            }),
            row: QueryRowInputRowOptions {
                key: "test".to_owned(),
            },
        })?;

        assert_eq!(query_result.cells_scanned_count, 1);

        assert_eq!(
            serde_json::to_value(query_result.row).unwrap(),
            serde_json::json!({
                "row_key": "test",
                "columns": {
                    "another_one": {
                        "": [
                            {
                                "timestamp": 0,
                                "value": {
                                    "String": "hello3"
                                }
                            }
                        ]
                    }
                }
            })
        );

        let query_result = table.query_row(QueryRowInput {
            cell: None,
            column: Some(QueryRowInputColumnOptions {
                filter: Some(ColumnFilter::Key(
                    ParsedColumnKey::try_from("another:").unwrap(),
                )),
            }),
            row: QueryRowInputRowOptions {
                key: "test".to_owned(),
            },
        })?;

        assert_eq!(query_result.cells_scanned_count, 1);

        assert_eq!(
            serde_json::to_value(query_result.row).unwrap(),
            serde_json::json!({
                "row_key": "test",
                "columns": {
                    "another": {
                        "": [
                            {
                                "timestamp": 0,
                                "value": {
                                    "String": "hello2"
                                }
                            }
                        ]
                    }
                }
            })
        );

        Ok(())
    }

    /*  #[test]
    pub fn smoltable_row_order() -> fjall::Result<()> {
        let folder = tempfile::tempdir()?;

        let keyspace = fjall::Config::new(folder.path()).open()?;
        let table = Smoltable::open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "value".to_owned(),
                version_limit: None,
            }],
            locality_group: None,
        })?;

        assert_eq!(1, table.list_column_families()?.len());

        let mut writer = TableWriter::new(table.clone());

        // TODO: i think : doesn't work in row keys right now

        writer.write(&writer::RowWriteItem {
            row_key: "item#1".to_owned(),
            cells: vec![writer::ColumnWriteItem {
                column_key: ParsedColumnKey::try_from("value:")
                    .expect("should be valid column key"),
                timestamp: Some(0),
                value: CellValue::U8(1),
            }],
        })?;
        writer.write(&writer::RowWriteItem {
            row_key: "item#2".to_owned(),
            cells: vec![writer::ColumnWriteItem {
                column_key: ParsedColumnKey::try_from("value:")
                    .expect("should be valid column key"),
                timestamp: Some(0),
                value: CellValue::U8(2),
            }],
        })?;
        writer.write(&writer::RowWriteItem {
            row_key: "item#3".to_owned(),
            cells: vec![writer::ColumnWriteItem {
                column_key: ParsedColumnKey::try_from("value:")
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
        let table = Smoltable::open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "value".to_owned(),
                version_limit: None,
            }],
            locality_group: None,
        })?;

        assert_eq!(1, table.list_column_families()?.len());

        let mut writer = TableWriter::new(table.clone());

        // TODO: i think : doesn't work in row keys right now

        writer.write(&writer::RowWriteItem {
            row_key: "item#1".to_owned(),
            cells: vec![writer::ColumnWriteItem {
                column_key: ParsedColumnKey::try_from("value:")
                    .expect("should be valid column key"),
                timestamp: Some(0),
                value: CellValue::U8(1),
            }],
        })?;
        writer.write(&writer::RowWriteItem {
            row_key: "item#2".to_owned(),
            cells: vec![writer::ColumnWriteItem {
                column_key: ParsedColumnKey::try_from("value:")
                    .expect("should be valid column key"),
                timestamp: Some(0),
                value: CellValue::U8(2),
            }],
        })?;
        writer.write(&writer::RowWriteItem {
            row_key: "item#3".to_owned(),
            cells: vec![writer::ColumnWriteItem {
                column_key: ParsedColumnKey::try_from("value:")
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
        let table = Smoltable::open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![
                ColumnFamilyDefinition {
                    name: "value".to_owned(),
                    version_limit: None,
                },
                ColumnFamilyDefinition {
                    name: "other".to_owned(),
                    version_limit: None,
                },
            ],
            locality_group: None,
        })?;

        assert_eq!(2, table.list_column_families()?.len());

        let mut writer = TableWriter::new(table.clone());

        // TODO: i think : doesn't work in row keys right now

        writer.write(&writer::RowWriteItem {
            row_key: "item#1".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(1),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("other:asd")
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
                    column_key: ParsedColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(2),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("other:asd")
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
                    column_key: ParsedColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(3),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("other:asd")
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
                ParsedColumnKey::try_from("other:").expect("should be valid column key"),
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
                ParsedColumnKey::try_from("other:asd").expect("should be valid column key"),
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
        let table = Smoltable::open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![
                ColumnFamilyDefinition {
                    name: "value".to_owned(),
                    version_limit: None,
                },
                ColumnFamilyDefinition {
                    name: "other".to_owned(),
                    version_limit: None,
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
                    column_key: ParsedColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(1),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("other:asd")
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
                    column_key: ParsedColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(2),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("other:asd")
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
                    column_key: ParsedColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(3),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("other:asd")
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
        let table = Smoltable::open("test", keyspace.clone())?;

        assert_eq!(0, table.list_column_families()?.len());

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "value".to_owned(),
                version_limit: None,
            }],
            locality_group: None,
        })?;

        table.create_column_families(&CreateColumnFamilyInput {
            column_families: vec![ColumnFamilyDefinition {
                name: "other".to_owned(),
                version_limit: None,
            }],
            locality_group: Some(true),
        })?;

        assert_eq!(2, table.list_column_families()?.len());

        let mut writer = TableWriter::new(table.clone());

        writer.write(&writer::RowWriteItem {
            row_key: "item#1".to_owned(),
            cells: vec![
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(1),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("other:asd")
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
                    column_key: ParsedColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(2),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("other:asd")
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
                    column_key: ParsedColumnKey::try_from("value:")
                        .expect("should be valid column key"),
                    timestamp: Some(0),
                    value: CellValue::U8(3),
                },
                writer::ColumnWriteItem {
                    column_key: ParsedColumnKey::try_from("other:asd")
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
