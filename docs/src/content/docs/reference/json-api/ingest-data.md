---
title: Ingest data
description: Ingest data using the JSON API
---

### URL

POST http://smoltable:9876/v1/table/[name]/write

### Example body

```json
{
  "items": [
    {
      "row_key": "org.apache.spark",
      "cells": [
        {
          "column_key": "title:",
          "type": "string",
          "value": "Apache Spark™ - Unified Engine for large-scale data analytics"
        },
        {
          "column_key": "anchor:org.apache.hbase",
          "type": "string",
          "value": "Visit Apache Spark"
        },
        {
          "column_key": "meta:size",
          "type": "i64",
          "value": 152014
        }
      ]
    }
  ]
}
```

### Example response

```json
{
  "message": "Data ingestion successful",
  "result": {
    "items": {
      "cell_count": 3,
      "row_count": 1
    },
    "micros_per_item": 5
  },
  "status": 200,
  "time_ms": 0
}
```
