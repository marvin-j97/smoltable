---
title: Wide-column design
description: An intro to wide-column databases
---

Like Bigtable, Smoltable is a wide-column database. It could be described as a *sparse, persistent multi-dimensional sorted map*. Every table is sorted by the *row key*, which is a unique identifier for each row. There are no secondary indexes. This may seem limiting but surprisingly many access patterns can be modelled this way.

The row key is not necessarily the same as the primary key in a relational database: A row in general is not a fixed structure like in a relational database. Each row may contain arbitrarily many columns, grouped into column families. Column families must be defined upfront, but column qualifiers (column names) can be defined dynamically, and do not need to adhere to a fixed schema. Because the table is sparse, unused columns do not cost any disk space. Retrieving specific columns does not require retrieving the entire row.

![Wide column table](/wide-column.png)

Each row’s cells are sorted by the column key (family + qualifier), and a timestamp: this results in a multi-dimensional key:

> row key + col family + col qualifier + ts

which maps to some value, the `cell value`. The cell value, unlike in Bigtable, can be a certain type:

- String (UTF-8 encoded string)
- Boolean (like Byte, but is unmarshalled as boolean)
- Byte (unsigned integer, 1 byte)
- I32 (signed integer, 4 bytes)
- I64 (signed integer, 8 bytes)
- F32 (floating point, 4 bytes)
- F64 (floating point, 8 bytes)

The timestamp allows storing multiple versions of the same cell.

:::note
  If versioning is not intended for a specific column, the timestamp `0` should be used.
:::

## Row key design

As mentioned before, there are no secondary indices and no query planner. The ordering of the table is defined by the row key, which in return will determine how efficient scans over specific rows and columns are.

When using multiple components in a row key, sorting them by cardinality generally optimizes locality and allows better querying. Compare:

`CONTINENT#COUNTRY#CITY`

vs.

`CITY#COUNTRY#CONTINENT`

The second row key will always perform a full table scan if we search only for a specific continent or country, because we cannot use a prefix.

:::tip
Sort multi-component row keys by cardinality for better locality and querying (”drill down”).
:::

:::tip
The same goes for column qualifiers: Inside a column family, one could group columns by using a certain column qualifier key design and then using a prefix column filter. This only works when accessing a column family of a single row (read row operation), not in a scan operation.
:::

## Real-life example: Webtable

The *webtable,* the heart of the Google search engine, is stored in Bigtable. It stores web pages and references (anchors) between said pages.

`language` contains a single column containing the language code (e.g. **DE**).

`anchor` where each column qualifier is an anchor's href attribute. The cell value is the anchor text (`el.textContent` in JavaScript).

`contents` contains a single column containing the raw HTML document. By moving it into a separate locality group the rather large documents will not slow down frequent queries accessing the other columns. By using cell versions, the table can store a history of the web page.

The row key is the reversed domain key. This maximizes locality of pages under the same (sub-)domain.

![Webtable](/webtable.png)

By listing the anchors of a row, we can count how many websites link to this specific page (for example to calculate the *PageRank*).

For simplicity the examples do not show full row keys. Domains alone are not enough to store the entire internet, so a real row key would also contain the pathname, like:

- `com.github/`
- `com.github/about`
- `com.github/pricing`
