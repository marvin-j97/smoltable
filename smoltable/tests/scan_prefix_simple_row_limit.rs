use smoltable::{
    query::scan::{Input as QueryPrefixInput, RowOptions as QueryPrefixRowOptions},
    CellValue, ColumnFamilyDefinition, CreateColumnFamilyInput, GarbageCollectionOptions,
    Smoltable, TableWriter,
};
use test_log::test;

#[test]
pub fn scan_prefix_simple_row_limit() -> smoltable::Result<()> {
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
            scan: smoltable::query::scan::ScanMode::Prefix(String::from("b")),
            cell_limit: None,
            limit: Some(1),
            sample: None,
        },
    })?;

    assert_eq!(query_result.affected_locality_groups, 1);
    assert_eq!(query_result.cells_scanned_count, 3);

    assert_eq!(
        serde_json::to_value(query_result.rows).unwrap(),
        serde_json::json!([
            {
                "row_key": "b",
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
