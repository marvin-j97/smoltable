use smoltable::{
    query::row::{Input as QueryRowInput, RowOptions as QueryRowInputRowOptions},
    CellValue, ColumnFamilyDefinition, CreateColumnFamilyInput, GarbageCollectionOptions,
    Smoltable, TableWriter,
};
use test_log::test;

#[test]
pub fn gc_version_limit() -> smoltable::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = fjall::Config::new(folder.path()).open()?;
    let table = Smoltable::open("test", keyspace.clone())?;

    assert_eq!(0, table.list_column_families()?.len());

    table.create_column_families(&CreateColumnFamilyInput {
        column_families: vec![ColumnFamilyDefinition {
            name: "value".to_owned(),
            gc_settings: GarbageCollectionOptions {
                ttl_secs: None,
                version_limit: Some(3),
            },
        }],
        locality_group: None,
    })?;

    assert_eq!(1, table.list_column_families()?.len());

    let mut writer = TableWriter::new(table.clone());

    writer.write(&smoltable::row!(
        "test",
        vec![
            smoltable::cell!("value:", Some(1), CellValue::String("hello".to_owned())),
            smoltable::cell!("value:", Some(2), CellValue::String("hello2".to_owned())),
            smoltable::cell!("value:", Some(3), CellValue::String("hello2".to_owned())),
            smoltable::cell!("value:", Some(4), CellValue::String("hello2".to_owned())),
            smoltable::cell!("value:", Some(5), CellValue::String("hello2".to_owned())),
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
    assert_eq!(query_result.cells_scanned_count, 5);

    table.run_version_gc()?;

    let query_result = table.get_row(QueryRowInput {
        column: None,
        row: QueryRowInputRowOptions {
            key: "test".to_owned(),
            cell_limit: None,
        },
    })?;
    assert_eq!(query_result.cells_scanned_count, 3);

    Ok(())
}

#[test]
pub fn gc_ttl() -> smoltable::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = fjall::Config::new(folder.path()).open()?;
    let table = Smoltable::open("test", keyspace.clone())?;

    assert_eq!(0, table.list_column_families()?.len());

    table.create_column_families(&CreateColumnFamilyInput {
        column_families: vec![ColumnFamilyDefinition {
            name: "value".to_owned(),
            gc_settings: GarbageCollectionOptions {
                ttl_secs: Some(5),
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
            smoltable::cell!("value:", Some(1), CellValue::String("hello".to_owned())),
            smoltable::cell!("value:", Some(2), CellValue::String("hello2".to_owned())),
            smoltable::cell!("value:", Some(3), CellValue::String("hello2".to_owned())),
            smoltable::cell!("value:", Some(4), CellValue::String("hello2".to_owned())),
            smoltable::cell!("value:", Some(5), CellValue::String("hello2".to_owned())),
            smoltable::cell!(
                "value:",
                Some(
                    std::time::SystemTime::UNIX_EPOCH
                        .elapsed()
                        .unwrap()
                        .as_nanos()
                ),
                CellValue::String("hello2".to_owned())
            ),
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
    assert_eq!(query_result.cells_scanned_count, 6);

    table.run_version_gc()?;

    let query_result = table.get_row(QueryRowInput {
        column: None,
        row: QueryRowInputRowOptions {
            key: "test".to_owned(),
            cell_limit: None,
        },
    })?;
    assert_eq!(query_result.cells_scanned_count, 1);

    Ok(())
}
