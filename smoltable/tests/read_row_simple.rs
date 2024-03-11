use smoltable::{
    query::row::{Input as QueryRowInput, RowOptions as QueryRowInputRowOptions},
    CellValue, ColumnFamilyDefinition, CreateColumnFamilyInput, GarbageCollectionOptions,
    Smoltable, TableWriter,
};
use test_log::test;

#[test]
pub fn read_row_simple() -> smoltable::Result<()> {
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
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello".to_owned())
        )]
    ))?;

    writer.finalize()?;

    let query_result = table.get_row(QueryRowInput {
        column: None,
        row: QueryRowInputRowOptions {
            key: "test".to_owned(),
            cell_limit: None,
        },
    })?;

    assert_eq!(query_result.affected_locality_groups, 1);
    assert_eq!(query_result.cells_scanned_count, 1);

    assert_eq!(
        serde_json::to_value(query_result.row).unwrap(),
        serde_json::json!({
            "row_key": "test",
            "columns": {
                "value": {
                    "": [
                        {
                            "time": 0,
                            "type": "string",
                            "value": "hello"
                        }
                    ]
                }
            }
        })
    );

    Ok(())
}

#[test]
pub fn read_row_simple_multiple_columns() -> smoltable::Result<()> {
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
                            "time": 0,
                            "type": "string",
                            "value": "hello"
                        }
                    ],
                    "def": [
                        {
                            "time": 0,
                            "type": "string",
                            "value": "hello2"
                        }
                    ]
                }
            }
        })
    );

    Ok(())
}
