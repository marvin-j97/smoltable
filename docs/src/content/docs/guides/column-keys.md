---
title: Column key schema
description: About column keys
---

A column key consists of two **case-sensitive** components, separated by `:` (colon):
 
- column family name
- column qualifier

Columns are grouped under column families. To access a column, use the family's name and a column qualifier. For instance, to access the column `size` inside the `meta` column family, the column key would be `meta:size`.

:::caution
Column qualifier in Bigtable can be arbitrary byte arrays. In Smoltable, they need to be UTF-8 strings.
:::

## Default column

The column qualifier can be omitted to access the default column (empty). For instance,
we wanted to have a single column family that stores a title, the column would be accessed
by `title:`. In that case, the `:` can be omitted: `title`.
