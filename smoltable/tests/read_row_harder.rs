use smoltable::{
    query::row::{Input as QueryRowInput, RowOptions as QueryRowInputRowOptions},
    CellValue, ColumnFamilyDefinition, CreateColumnFamilyInput, GarbageCollectionOptions,
    Smoltable, TableWriter,
};
use test_log::test;

#[test]
pub fn read_row_harder() -> smoltable::Result<()> {
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
    writer.write(&smoltable::row!(
        "test2",
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello2".to_owned())
        )]
    ))?;
    writer.write(&smoltable::row!(
        "test3",
        vec![smoltable::cell!(
            "value:",
            Some(0),
            CellValue::String("hello3".to_owned())
        )]
    ))?;

    writer.finalize()?;

    let query_result = table.get_row(QueryRowInput {
        column: None,
        row: QueryRowInputRowOptions {
            key: "test2".to_owned(),
            cell_limit: None,
        },
    })?;

    assert_eq!(query_result.affected_locality_groups, 1);

    assert_eq!(
        serde_json::to_value(query_result.row).unwrap(),
        serde_json::json!({
            "row_key": "test2",
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
        })
    );

    Ok(())
}
