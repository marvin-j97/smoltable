---
title: Retrieve rows
description: Retrieve rows using the JSON API
---

### URL

POST http://smoltable:9876/v1/table/[name]/rows

### Example body

```json
{
  "items": [
    {
      "row": {
        "key": "org.apache.spark"
      }
    }
  ]
}
```

### Example response

```json
{
  "message": "Query successful",
  "result": {
    "bytes_scanned": 124,
    "cells_scanned": 1,
    "micros": 23,
    "micros_per_row": 23,
    "rows": [
      {
        "columns": {
          "title": {
            "": [
              {
                "time": 0,
                "type": "string",
                "value": "Apache Spark"
              }
            ]
          }
        },
        "row_key": "org.apache.spark"
      }
    ],
    "rows_scanned": 1
  },
  "status": 200,
  "time_ms": 0
}
```

### Filter by column family

```json
{
  "items": [
    {
      "row": {
        "key": "org.apache.spark"
      },
      "column": {
        "key": "anchor:"
      }
    }
  ]
}
```

### Filter by column

```json
{
  "items": [
    {
      "row": {
        "key": "org.apache.spark"
      },
      "column": {
        "key": "anchor:com.apache.solr"
      }
    }
  ]
}
```

### Filter by multiple columns

```json
{
  "items": [
    {
      "row": {
        "key": "org.apache.spark"
      },
      "column": {
        "multi_key": ["anchor:com.apache.solr", "anchor:com.apache.hbase"]
      }
    }
  ]
}
```

### Filter by column qualifier prefix

```json
{
  "items": [
    {
      "row": {
        "key": "org.apache.spark"
      },
      "column": {
        "prefix": "anchor:com."
      }
    }
  ]
}
```

### Limit returned cell versions per column

```json
{
  "items": [
    {
      "row": {
        "key": "org.apache.spark"
      },
      "column": {
        "cell_limit": 3
      }
    }
  ]
}
```

### Limit returned cells per row

```json
{
  "items": [
    {
      "row": {
        "key": "org.apache.spark",
        "cell_limit": 10
      }
    }
  ]
}
```

<!-- TODO: -->
<!-- ### Limit returned columns

```json
{
	"items": [
		{
			"row": {
				"key": "org.apache.spark"
			},
      "column": {
				"key": "anchor:",
        "limit": 100
			}
		}
	]
}
``` -->
