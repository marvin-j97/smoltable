# API

Preliminary API docs

## Response format

```json
{
	"message": "Readable message",
	"result": DEPENDS_ON_THE_ROUTE,
	"status": 200,
	"time_ms": 5
}
```

## Create table

Creates a new table. The name should be unique.

### URI

`PUT /table/{name}`

### Result

`null`

## Create column family

Creates a new column family in the given table.

### URI

`PUT /table/{name}/column-family/{name}`

### Request body

```json
{} // TODO:
```

### Result

`null`

## List tables

List all tables, their column families and some up-to-date statistics.

### URI

`GET /table/{name}`

### Result

```json
{
  "tables": {
    "count": 1,
    "items": [
      {
        "cache_stats": { // TODO:
          "block_count": 0,
          "memory_usage_in_bytes": 0
        },
        "column_families": [
          {
            "name": "stats",
            "row_limit": null
            // TODO:
          }
        ],
        "disk_space_in_bytes": 0, // TODO:
        "name": "my-table-name"
      }
    ]
  }
}
```

## System information

Shows some up-to-date system metrics.

### URI

`GET /system`

### Results

```json
{
  "system": {
    "info": {
      "cpu_name": "AMD EPYC",
      "memory_size_in_bytes": 196153344,
      "os_name": "Linux 3.18.5 Alpine Linux"
    },
    "stats": {
      "cpu_usage_percent": 0.07,
      "database_size_in_bytes": 167,
      "memory_used_in_bytes": 31875072
    }
  }
}
```

## Ingest data

Adds data to a table.

### URI

`POST /table/{name}/write`

### Request body

// TODO: types

```json
{
	"items": [
		{
			"row_key": "user#1#dev#p#1234",
			"cells": [
				{
					"column_key": "info:os",
					"value": "test",
					"timestamp": 0
				},
				{
					"column_key": "stats:cpu",
					"value": 7
				}
			]
		},
		{
			"row_key": "user#2#dev#p#1235",
			"cells": [
				{
					"column_key": "info:os",
					"value": "test",
					"timestamp": 0
				}
			]
		}
	]
}
```

### Result

... TODO:

## Get row

Gets a row, optionally filtering by specific columns.

### URI

`GET /table/{name}/get-row`

### Request body

```json
{
  "row_key": "my#row#key",
  "column_filter": "stats:",
  "cell_limit": 2,
}
```

### Result

```json
{
  "micros": 27, // TODO: time_micros
  "row": null // TODO:
}
```

// TODO: multi-get rows

## Scan rows

Scans rows using a prefix, optionally by specific columns.

### URI

`GET /table/{name}/prefix`

### Request body

```json
{
  "row_key": "my#row#key",
  "column_filter": "stats:cpu",
  "cell_limit": 2,
  "limit": 1000
}
```

### Result

```json
{
  "micros": 27, // TODO: time_micros
  "rows": null // TODO:
}
```

## Delete row

Deletes a row or some of its columns.

### URI

`DELETE /table/{name}/get-row`

### Request body

```json
{
	"row_key": "my#row#key",
	"column_filter": "please_only_delete_stats"
}
```

### Result

// TODO:
