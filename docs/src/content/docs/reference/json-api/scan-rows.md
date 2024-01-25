---
title: Scan rows
description: Scan rows using the JSON API
---

### URL

POST http://smoltable:9876/v1/table/[name]/scan

### Example body

```json
{
	"items": [
		{
			"row": {
				"prefix": "org."
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
								"timestamp": 0,
								"value": {
									"String": "Apache Solr"
								}
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
								"timestamp": 0,
								"value": {
									"String": "Apache Spark"
								}
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
	"items": [
		{
			"row": {
				"prefix": "org.apache."
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
				"prefix": "org.apache."
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
				"prefix": "org.apache."
			},
      "column": {
				"multi_key": [
          "anchor:com.apache.solr",
          "anchor:com.apache.hbase"
        ]
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
				"prefix": "org.apache."
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
				"prefix": "org.apache."
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
				"prefix": "org.apache.",
        "cell_limit": 10
			}
		}
	]
}
```

### Limit returned cells

```json
{
	"items": [
		{
			"row": {
				"prefix": "org.apache."
			},
			"cell": {
				"limit": 10
			}
		}
	]
}
```

### Limit returned rows

```json
{
	"items": [
		{
			"row": {
				"prefix": "org.apache.",
        "limit": 10
			}
		}
	]
}
```

### Sample every N rows

```json
{
	"items": [
		{
			"row": {
				"prefix": "org.apache.",
        "sample": 0.1
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
