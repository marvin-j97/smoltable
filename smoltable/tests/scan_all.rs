use smoltable::{
    query::scan::{Input as QueryPrefixInput, RowOptions as QueryPrefixRowOptions},
    CellValue, ColumnFamilyDefinition, CreateColumnFamilyInput, GarbageCollectionOptions,
    Smoltable, TableWriter,
};
use test_log::test;

#[test]
pub fn scan_all() -> smoltable::Result<()> {
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
        vec![
            smoltable::cell!("value:asd", Some(0), CellValue::String("hello".to_owned())),
            smoltable::cell!("value:def", Some(0), CellValue::String("hello2".to_owned()))
        ]
    ))?;

    writer.write(&smoltable::row!(
        "b",
        vec![
            smoltable::cell!("value:yxc", Some(0), CellValue::String("hello".to_owned())),
            smoltable::cell!("value:cxy", Some(0), CellValue::String("hello2".to_owned()))
        ]
    ))?;

    writer.write(&smoltable::row!(
        "ba",
        vec![smoltable::cell!(
            "value:asd",
            Some(0),
            CellValue::String("hello".to_owned())
        )]
    ))?;

    writer.write(&smoltable::row!(
        "c",
        vec![
            smoltable::cell!("value:asd", Some(0), CellValue::String("hello".to_owned())),
            smoltable::cell!("value:dsa", Some(0), CellValue::String("hello2".to_owned()))
        ]
    ))?;

    writer.finalize()?;

    let query_result = table.scan(QueryPrefixInput {
        column: None,
        cell: None,
        row: QueryPrefixRowOptions {
            scan: smoltable::query::scan::ScanMode::Prefix(String::from("")),
            cell_limit: None,
            limit: None,
            sample: None,
        },
    })?;

    assert_eq!(query_result.cells_scanned_count, 7);

    assert_eq!(
        serde_json::to_value(query_result.rows).unwrap(),
        serde_json::json!([
            {
                "row_key": "a",
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
            },
            {
                "row_key": "b",
                "columns": {
                    "value": {
                        "yxc": [
                            {
                                "timestamp": 0,
                                "value": {
                                    "String": "hello"
                                }
                            }
                        ],
                        "cxy": [
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
                "row_key": "ba",
                "columns": {
                    "value": {
                        "asd": [
                            {
                                "timestamp": 0,
                                "value": {
                                    "String": "hello"
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
                        "dsa": [
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
