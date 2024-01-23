use smoltable::{
    query::row::{
        ColumnOptions as QueryRowInputColumnOptions, Input as QueryRowInput,
        RowOptions as QueryRowInputRowOptions,
    },
    CellValue, ColumnFamilyDefinition, CreateColumnFamilyInput, GarbageCollectionOptions,
    Smoltable, TableWriter,
};
use test_log::test;

#[test]
pub fn read_row_column_cell_limit() -> smoltable::Result<()> {
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
        "test",
        vec![
            smoltable::cell!("value:asd", Some(0), CellValue::String("hello".to_owned())),
            smoltable::cell!("value:def", Some(0), CellValue::String("hello2".to_owned()))
        ]
    ))?;

    writer.finalize()?;

    let query_result = table.get_row(QueryRowInput {
        column: None,
        row: QueryRowInputRowOptions {
            key: "test".to_owned(),
            cell_limit: None,
        },
    })?;

    assert_eq!(query_result.cells_scanned_count, 2);

    assert_eq!(
        serde_json::to_value(query_result.row).unwrap(),
        serde_json::json!({
            "row_key": "test",
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
        })
    );

    Ok(())
}

#[test]
pub fn write_read_row_multiple_columns_cell_limit() -> smoltable::Result<()> {
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
        "test",
        vec![
            smoltable::cell!("value:asd", Some(1), CellValue::String("hello1".to_owned())),
            smoltable::cell!("value:asd", Some(2), CellValue::String("hello2".to_owned())),
            smoltable::cell!("value:asd", Some(3), CellValue::String("hello3".to_owned())),
            smoltable::cell!("value:asd", Some(4), CellValue::String("hello4".to_owned())),
            smoltable::cell!("value:asd", Some(5), CellValue::String("hello5".to_owned())),
            smoltable::cell!("value:def", Some(1), CellValue::String("hello1".to_owned())),
            smoltable::cell!("value:def", Some(2), CellValue::String("hello2".to_owned())),
            smoltable::cell!("value:def", Some(3), CellValue::String("hello3".to_owned())),
            smoltable::cell!("value:def", Some(4), CellValue::String("hello4".to_owned())),
            smoltable::cell!("value:def", Some(5), CellValue::String("hello5".to_owned())),
        ]
    ))?;

    writer.finalize()?;

    let query_result = table.get_row(QueryRowInput {
        column: Some(QueryRowInputColumnOptions {
            cell_limit: Some(2),
            filter: None,
        }),
        row: QueryRowInputRowOptions {
            key: "test".to_owned(),
            cell_limit: None,
        },
    })?;

    assert_eq!(query_result.cells_scanned_count, 10);

    assert_eq!(
        serde_json::to_value(query_result.row).unwrap(),
        serde_json::json!({
            "row_key": "test",
            "columns": {
                "value": {
                    "asd": [
                        {
                            "timestamp": 5,
                            "value": {
                                "String": "hello5"
                            }
                        },
                        {
                            "timestamp": 4,
                            "value": {
                                "String": "hello4"
                            }
                        }
                    ],
                    "def": [
                        {
                            "timestamp": 5,
                            "value": {
                                "String": "hello5"
                            }
                        },
                        {
                            "timestamp": 4,
                            "value": {
                                "String": "hello4"
                            }
                        }
                    ]
                }
            }
        })
    );

    Ok(())
}
