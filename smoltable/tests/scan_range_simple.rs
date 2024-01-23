use smoltable::{
    query::scan::{Input as QueryPrefixInput, Range, RowOptions as QueryPrefixRowOptions},
    CellValue, ColumnFamilyDefinition, CreateColumnFamilyInput, GarbageCollectionOptions,
    Smoltable, TableWriter,
};
use test_log::test;

#[test]
pub fn scan_range_simple() -> smoltable::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = fjall::Config::new(folder.path()).open()?;
    let table = Smoltable::open("test", keyspace.clone())?;

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

    writer.write(&smoltable::row!(
        "a",
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello".to_owned())
        )]
    ))?;

    writer.write(&smoltable::row!(
        "b",
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello".to_owned())
        )]
    ))?;

    writer.write(&smoltable::row!(
        "ba",
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello2".to_owned())
        )]
    ))?;

    writer.write(&smoltable::row!(
        "c",
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello".to_owned())
        )]
    ))?;

    writer.finalize()?;

    let query_result = table.scan(QueryPrefixInput {
        column: None,
        cell: None,
        row: QueryPrefixRowOptions {
            scan: smoltable::query::scan::ScanMode::Range(Range {
                start: "ba".into(),
                end: "c".into(),
                inclusive: true,
            }),
            cell_limit: None,
            limit: None,
            sample: None,
        },
    })?;

    assert_eq!(query_result.cells_scanned_count, 2);

    assert_eq!(
        serde_json::to_value(query_result.rows).unwrap(),
        serde_json::json!([
            {
                "row_key": "ba",
                "columns": {
                    "value": {
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
            },
            {
                "row_key": "c",
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
            }
        ])
    );

    Ok(())
}

#[test]
pub fn scan_range_simple_exclusive() -> smoltable::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = fjall::Config::new(folder.path()).open()?;
    let table = Smoltable::open("test", keyspace.clone())?;

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

    writer.write(&smoltable::row!(
        "a",
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello".to_owned())
        )]
    ))?;

    writer.write(&smoltable::row!(
        "b",
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello".to_owned())
        )]
    ))?;

    writer.write(&smoltable::row!(
        "ba",
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello2".to_owned())
        )]
    ))?;

    writer.write(&smoltable::row!(
        "c",
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello".to_owned())
        )]
    ))?;

    writer.finalize()?;

    let query_result = table.scan(QueryPrefixInput {
        column: None,
        cell: None,
        row: QueryPrefixRowOptions {
            scan: smoltable::query::scan::ScanMode::Range(Range {
                start: "ba".into(),
                end: "c".into(),
                inclusive: false,
            }),
            cell_limit: None,
            limit: None,
            sample: None,
        },
    })?;

    assert_eq!(query_result.cells_scanned_count, 2);

    assert_eq!(
        serde_json::to_value(query_result.rows).unwrap(),
        serde_json::json!([
            {
                "row_key": "ba",
                "columns": {
                    "value": {
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
            }
        ])
    );

    Ok(())
}

#[test]
pub fn scan_range_simple_multi_columns() -> smoltable::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = fjall::Config::new(folder.path()).open()?;
    let table = Smoltable::open("test", keyspace.clone())?;

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

    writer.write(&smoltable::row!(
        "a",
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello".to_owned())
        )]
    ))?;

    writer.write(&smoltable::row!(
        "b",
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello".to_owned())
        )]
    ))?;

    writer.write(&smoltable::row!(
        "ba",
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello2".to_owned())
        )]
    ))?;

    writer.write(&smoltable::row!(
        "c",
        vec![
            smoltable::cell!("value:asd", Some(0), CellValue::String("hello".to_owned())),
            smoltable::cell!("value:def", Some(0), CellValue::String("hello2".to_owned()))
        ]
    ))?;

    writer.finalize()?;

    let query_result = table.scan(QueryPrefixInput {
        column: None,
        cell: None,
        row: QueryPrefixRowOptions {
            scan: smoltable::query::scan::ScanMode::Range(Range {
                start: "ba".into(),
                end: "c".into(),
                inclusive: true,
            }),
            cell_limit: None,
            limit: None,
            sample: None,
        },
    })?;

    assert_eq!(query_result.cells_scanned_count, 3);

    assert_eq!(
        serde_json::to_value(query_result.rows).unwrap(),
        serde_json::json!([
            {
                "row_key": "ba",
                "columns": {
                    "value": {
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
            },
            {
                "row_key": "c",
                "columns": {
                    "value": {
                        "asd": [
                            {
                                "timestamp": 0,
                                "value": {
                                    "String": "hello"
                                }
                            }
                        ],
                        "def": [
                            {
                                "timestamp": 0,
                                "value": {
                                    "String": "hello2"
                                }
                            }
                        ]
                    }
                }
            }
        ])
    );

    Ok(())
}

#[test]
pub fn scan_range_simple_exclusive_multi_columns() -> smoltable::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = fjall::Config::new(folder.path()).open()?;
    let table = Smoltable::open("test", keyspace.clone())?;

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

    writer.write(&smoltable::row!(
        "a",
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello".to_owned())
        )]
    ))?;

    writer.write(&smoltable::row!(
        "b",
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello".to_owned())
        )]
    ))?;

    writer.write(&smoltable::row!(
        "ba",
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello2".to_owned())
        )]
    ))?;

    writer.write(&smoltable::row!(
        "c",
        vec![
            smoltable::cell!("value:asd", Some(0), CellValue::String("hello".to_owned())),
            smoltable::cell!("value:def", Some(0), CellValue::String("hello2".to_owned()))
        ]
    ))?;

    writer.finalize()?;

    let query_result = table.scan(QueryPrefixInput {
        column: None,
        cell: None,
        row: QueryPrefixRowOptions {
            scan: smoltable::query::scan::ScanMode::Range(Range {
                start: "ba".into(),
                end: "c".into(),
                inclusive: false,
            }),
            cell_limit: None,
            limit: None,
            sample: None,
        },
    })?;

    assert_eq!(query_result.cells_scanned_count, 3);

    assert_eq!(
        serde_json::to_value(query_result.rows).unwrap(),
        serde_json::json!([
            {
                "row_key": "ba",
                "columns": {
                    "value": {
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
            }
        ])
    );

    Ok(())
}
