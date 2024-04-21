---
title: Create column families
description: Create column families using the JSON API
---

### URL

POST http://smoltable:9876/v1/table/[name]/column-family

### Example body

```json
{
  "column_families": [
    {
      "name": "language"
    },
    {
      "name": "title"
    }
  ]
}
```

### Example response

```json
{
  "message": "Column families created successfully",
  "result": null,
  "status": 201,
  "time_ms": 25
}
```

## Define a locality group

```json
{
  "column_families": [
    {
      "name": "anchor"
    }
  ],
  "locality_group": true
}
```

You may create and group multiple column families into a [locality group](/smoltable/guides/locality-groups). However, the column families can not be created upfront and then moved into a locality group.

## Configure garbage collection

```json
{
  "column_families": [
    {
      "name": "title",
      "gc_settings": {
        "version_limit": 10,
        "ttl_secs": null
      }
    }
  ]
}
```

See the chapter [Data Retention](/smoltable/guides/data-retention) for more information about garbage collection.

### Parameters

##### `gc_settings.version_limit`

Maximum amount of versions to keep per cell. Oldest versions are deleted first.

##### `gc_settings.ttl_secs`

Time-to-live in seconds per cell.
