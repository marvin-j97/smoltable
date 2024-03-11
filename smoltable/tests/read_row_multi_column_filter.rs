use smoltable::{
    query::row::{
        ColumnOptions as QueryRowInputColumnOptions, Input as QueryRowInput,
        RowOptions as QueryRowInputRowOptions,
    },
    CellValue, ColumnFamilyDefinition, ColumnFilter, ColumnKey, CreateColumnFamilyInput,
    GarbageCollectionOptions, Smoltable, TableWriter,
};
use test_log::test;

#[test]
pub fn read_row_multi_column_filter() -> smoltable::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = fjall::Config::new(folder.path()).open()?;
    let table = Smoltable::open("test", keyspace.clone())?;

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
                name: "another".to_owned(),
                gc_settings: GarbageCollectionOptions {
                    ttl_secs: None,
                    version_limit: None,
                },
            },
            ColumnFamilyDefinition {
                name: "another_one".to_owned(),
                gc_settings: GarbageCollectionOptions {
                    ttl_secs: None,
                    version_limit: None,
                },
            },
        ],
        locality_group: None,
    })?;

    assert_eq!(3, table.list_column_families()?.len());

    let mut writer = TableWriter::new(table.clone());

    writer.write(&smoltable::row!(
        "test",
        vec![
            smoltable::cell!("value:", Some(0), CellValue::String("hello".to_owned())),
            smoltable::cell!("another:", Some(0), CellValue::String("hello2".to_owned())),
            smoltable::cell!(
                "another_one:",
                Some(0),
                CellValue::String("hello3".to_owned())
            )
        ]
    ))?;

    writer.finalize()?;

    let query_result = table.get_row(QueryRowInput {
        column: Some(QueryRowInputColumnOptions {
            filter: Some(ColumnFilter::Multi(vec![
                ColumnKey::try_from("value:").unwrap(),
                ColumnKey::try_from("another_one:").unwrap(),
            ])),
            cell_limit: None,
        }),
        row: QueryRowInputRowOptions {
            key: "test".to_owned(),
            cell_limit: None,
        },
    })?;

    assert_eq!(query_result.affected_locality_groups, 1);
    assert_eq!(query_result.cells_scanned_count, 3);

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
                },
                "another_one": {
                    "": [
                        {
                            "time": 0,
                            "type": "string",
                            "value": "hello3"
                        }
                    ]
                }
            }
        })
    );

    Ok(())
}
