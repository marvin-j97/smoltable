use smoltable::{
    CellValue, ColumnFamilyDefinition, CreateColumnFamilyInput, GarbageCollectionOptions,
    Smoltable, TableWriter,
};
use test_log::test;

#[test]
pub fn write_count() -> smoltable::Result<()> {
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
            "value:asd",
            Some(0),
            CellValue::String("hello".to_owned())
        )]
    ))?;
    writer.finalize()?;

    let (row_count, cell_count) = table.count()?;
    assert_eq!(1, row_count);
    assert_eq!(1, cell_count);

    let mut writer = TableWriter::new(table.clone());
    writer.write(&smoltable::row!(
        "test",
        vec![smoltable::cell!(
            "value:def",
            Some(0),
            CellValue::String("hello!!!".to_owned())
        )]
    ))?;
    writer.finalize()?;

    let (row_count, cell_count) = table.count()?;
    assert_eq!(1, row_count);
    assert_eq!(2, cell_count);

    let mut writer = TableWriter::new(table.clone());
    writer.write(&smoltable::row!(
        "test2",
        vec![smoltable::cell!(
            "value:asd",
            Some(0),
            CellValue::String("hello".to_owned())
        )]
    ))?;
    writer.finalize()?;

    let (row_count, cell_count) = table.count()?;
    assert_eq!(2, row_count);
    assert_eq!(3, cell_count);

    let mut writer = TableWriter::new(table.clone());
    writer.write(&smoltable::row!(
        "test2",
        vec![smoltable::cell!(
            "value:def",
            Some(0),
            CellValue::String("hello!!!".to_owned())
        )]
    ))?;
    writer.finalize()?;

    let (row_count, cell_count) = table.count()?;
    assert_eq!(2, row_count);
    assert_eq!(4, cell_count);

    Ok(())
}
