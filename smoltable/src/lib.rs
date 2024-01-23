mod cell;
mod column_filter;
mod column_key;
mod error;
pub mod query;
mod row;
mod table;

pub use {
    cell::Cell,
    cell::Value as CellValue,
    cell::VisitedCell,
    column_filter::ColumnFilter,
    column_key::ColumnKey,
    error::{Error, Result},
    row::Row,
    table::writer::{ColumnWriteItem, RowWriteItem, Writer as TableWriter},
    table::{
        ColumnFamilyDefinition, CreateColumnFamilyInput, GarbageCollectionOptions, Smoltable,
        BLOCK_SIZE,
    },
};

#[macro_export]
macro_rules! row {
    ($key:expr, $cells:expr) => {
        $crate::RowWriteItem {
            row_key: $key.to_string(),
            cells: $cells,
        }
    };
}

#[macro_export]
macro_rules! cell {
    ($key:expr, $timestamp:expr, $cell_value:expr) => {
        $crate::ColumnWriteItem {
            column_key: $crate::ColumnKey::try_from($key).expect("should be column key"),
            timestamp: $timestamp,
            value: $cell_value,
        }
    };
}
