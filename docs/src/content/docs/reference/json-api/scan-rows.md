---
title: Scan rows
description: Scan rows using the JSON API
---

### URL

POST http://smoltable:9876/v1/table/[name]/scan

### Example body

```json
{
  "row": {
    "prefix": "org.apache."
  }
}
```

### Example response

```json
{
  "message": "Query successful",
  "result": {
    "bytes_scanned": 124124,
    "cells_scanned": 2,
    "micros": 100,
    "micros_per_row": 50,
    "rows": [
      {
        "columns": {
          "title": {
            "": [
              {
                "time": 0,
                "type": "string",
                "value": "Apache Solr"
              }
            ]
          }
        },
        "row_key": "org.apache.solr"
      },
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
    "rows_scanned": 2
  },
  "status": 200,
  "time_ms": 0
}
```

### Filter by column family

```json
{
  "row": {
    "prefix": "org.apache."
  },
  "column": {
    "key": "anchor:"
  }
}
```

### Filter by column

```json
{
  "row": {
    "prefix": "org.apache."
  },
  "column": {
    "key": "anchor:com.apache.solr"
  }
}
```

### Filter by multiple columns

```json
{
  "row": {
    "prefix": "org.apache."
  },
  "column": {
    "multi_key": ["anchor:com.apache.solr", "anchor:com.apache.hbase"]
  }
}
```

### Filter by column qualifier prefix

```json
{
  "row": {
    "prefix": "org.apache."
  },
  "column": {
    "prefix": "anchor:com."
  }
}
```

### Limit returned cell versions per column

```json
{
  "row": {
    "prefix": "org.apache."
  },
  "column": {
    "cell_limit": 3
  }
}
```

### Limit returned cells per row

```json
{
  "row": {
    "prefix": "org.apache.",
    "cell_limit": 10
  }
}
```

### Limit returned cells

```json
{
  "row": {
    "prefix": "org.apache."
  },
  "cell": {
    "limit": 10
  }
}
```

### Limit returned rows

```json
{
  "row": {
    "prefix": "org.apache.",
    "limit": 10
  }
}
```

### Skip rows

```json
{
  "row": {
    "prefix": "org.apache.",
    "offset": 10,
    "limit": 10
  }
}
```

### Sample every N rows

```json
{
  "row": {
    "prefix": "org.apache.",
    "sample": 0.1
  }
}
```

<!-- TODO: scan backwards -->

<!-- TODO: -->
<!-- ### Limit returned columns

```json
{
  "row": {
    "key": "org.apache.spark"
  },
  "column": {
    "key": "anchor:",
    "limit": 100
  }
}
``` -->
