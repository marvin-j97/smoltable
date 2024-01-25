---
title: Locality groups
description: Using locality groups
---

If we need to read columns of a specific column family for many rows (using a common prefix), scan performance will degrade as column families increase in size.

Consider the [`webtable` example](/smoltable/guides/wide-column-intro/#real-life-example-webtable):

If we wanted to get the language of all com.* pages, we would need to scan following column families:

- `anchor`, which can be a very wide column family
- `language`
- `contents`, which is always huge because it stores raw HTML

`language` is just 2 bytes (alpha2 country code, e.g. **DE**, **EN**, ...), but every row may require multiple kilobytes of data to be retrieved to get just the language. This heavily decreases read throughput of OLAP-style scans of large ranges.

To combat this, we can define a *locality group*, which can house multiple column families. Each locality group is stored in its own LSM-tree (a single partition inside the storage engine), but row mutations across column families stay atomic.

![Webtable locality groups](/webtable-locality.png)

:::tip
Store column families in their own locality group if they are not queried frequently, especially if they store a large chunks of data per row.
:::

The data inside the locality group can likely be compressed much more efficiently, if the data is of the same type and similar in some way.

One downside of partitioning using locality groups is the increased read latency if we need to access column families that are not part of the same locality group.

:::tip
Group column families into the same locality group if they are accessed together frequently.
:::

## Example: Without locality groups

### Setup

First, let's create a table `scan-example`:

```bash
curl --request PUT \
  --url http://localhost:9876/v1/table/scan-example
```

and two column families, `title` and `language`:

```bash
curl --request POST \
  --url http://localhost:9876/v1/table/scan-example/column-family \
  --header 'content-type: application/json' \
  --data '{
  "column_families": [
    {
      "name": "language"
    },
    {
      "name": "title"
    }
  ]
}'
```

By listing our table, we can see the column families have been created and are not part of any locality groups:

```json
{
  "message": "Tables retrieved successfully",
  "result": {
    "tables": {
      "count": 1,
      "items": [
        {
          "column_families": [
            {
              "gc_settings": {
                "ttl_secs": null,
                "version_limit": null
              },
              "name": "language"
            },
            {
              "gc_settings": {
                "ttl_secs": null,
                "version_limit": null
              },
              "name": "title"
            }
          ],
          "disk_space_in_bytes": 0,
          "locality_groups": [],
          "name": "scan-example",
          "partitions": [
            {
              "name": "_man_scan-example",
              "path": ".smoltable_data/partitions/_man_scan-example"
            },
            {
              "name": "_dat_scan-example",
              "path": ".smoltable_data/partitions/_dat_scan-example"
            }
          ]
        }
      ]
    }
  },
  "status": 200,
  "time_ms": 0
}
```

All data is stored in the `_dat_scan-example` partition.

### Ingest data

Let's ingest some data and query it (body is truncated for brevity):

```json
curl --request POST \
  --url http://localhost:9876/v1/table/scan-example/write \
  --header 'content-type: application/json' \
  --data '{
  "items": [
    {
      "row_key": "org.apache.spark",
      "cells": [
        {
          "column_key": "title:",
          "value": {
            "String": "Apache Spark™ - Unified Engine for large-scale data analytics"
          }
        },
        {
          "column_key": "language:",
          "value": {
            "String": "EN"
          }
        }
      ]
    },
    {
      "row_key": "org.apache.solr",
      "cells": [
        {
          "column_key": "title:",
          "value": {
            "String": "Welcome to Apache Solr - Apache Solr"
          }
        },
        {
          "column_key": "language:",
          "value": {
            "String": "EN"
          }
        }
      ]
    }
  ]
}'
```

### Query data

Let's query our entire table using a scan with empty prefix:

```bash
curl --request POST \
  --url http://localhost:9876/v1/table/scan-example/scan \
  --header 'content-type: application/json' \
  --data '{
	"row": {
		"prefix": ""
	},
	"column": {
		"key": "title:"
	}
}'
```

Smoltable returns (again, body truncated for brevity):

```json
{
  "message": "Query successful",
  "result": {
    "affected_locality_groups": 1, // TODO:
    "bytes_scanned": 1141,
    "cell_count": 8,
    "cells_scanned": 16,
    "micros_per_row": 18,
    "row_count": 8,
    "rows": [
      {
        "columns": {
          "title": {
            "": [
              {
                "timestamp": 1706197595375136143,
                "value": {
                  "String": "Apache Cassandra | Apache Cassandra Documentation"
                }
              }
            ]
          }
        },
        "row_key": "org.apache.cassandra"
      }
    ],
    "rows_scanned": 8
  },
  "status": 200,
  "time_ms": 0
}
```

Note, how we scanned 1 KB of data, and 16 cells, but only returned 8 cells (because we filtered by the `title` column family). That means we have a read amplification of `2`.

## Example: With locality groups

### Setup

First, let's create a table `locality-example`:

```bash
curl --request PUT \
  --url http://localhost:9876/v1/table/locality-example
```

and two column families, `title` and `language`, but move `title` into a locality group:

```bash
curl --request POST \
  --url http://localhost:9876/v1/table/locality-example/column-family \
  --header 'content-type: application/json' \
  --data '{
  "column_families": [
    {
      "name": "language"
    }
  ]
}'
```

```bash
curl --request POST \
  --url http://localhost:9876/v1/table/locality-example/column-family \
  --header 'content-type: application/json' \
  --data '{
  "column_families": [
    {
      "name": "title"
    }
  ],
  "locality_group": true
}'
```

By listing our table, we can see the column families have been created, and `title` is moved into a locality group:

```json
{
  "message": "Tables retrieved successfully",
  "result": {
    "tables": {
      "count": 1,
      "items": [
        {
          "column_families": [
            {
              "gc_settings": {
                "ttl_secs": null,
                "version_limit": null
              },
              "name": "language"
            },
            {
              "gc_settings": {
                "ttl_secs": null,
                "version_limit": null
              },
              "name": "title"
            }
          ],
          "disk_space_in_bytes": 0,
          "locality_groups": [
            {
              "column_families": [
                "title"
              ],
              "id": "ur_pSQZ2QAYR6XsF9Xz0o"
            }
          ],
          "name": "locality-example",
          "partitions": [
            {
              "name": "_man_locality-example",
              "path": ".smoltable_data/partitions/_man_locality-example"
            },
            {
              "name": "_dat_locality-example",
              "path": ".smoltable_data/partitions/_dat_locality-example"
            },
            {
              "name": "_lg_ur_pSQZ2QAYR6XsF9Xz0o",
              "path": ".smoltable_data/partitions/_lg_ur_pSQZ2QAYR6XsF9Xz0o"
            }
          ]
        }
      ]
    }
  },
  "status": 200,
  "time_ms": 0
}
```

Column families that are not `title` are stored in the `_dat_locality-example` partition, and `title` data is moved into the `_lg_ur_pSQZ2QAYR6XsF9Xz0o` partition.

### Ingest data

Ingest the same data as before into `locality-example`.

### Query data

```bash
curl --request POST \
  --url http://localhost:9876/v1/table/locality-example/scan \
  --header 'content-type: application/json' \
  --data '{
	"row": {
		"prefix": ""
	},
	"column": {
		"key": "title:"
	}
}'
```

which returns (truncated):

```json
{
  "message": "Query successful",
  "result": {
    "bytes_scanned": 681,
    "cell_count": 8,
    "cells_scanned": 8,
    "micros_per_row": 18,
    "row_count": 8,
    "rows": [
      {
        "columns": {
          "title": {
            "": [
              {
                "timestamp": 1706198298766257607,
                "value": {
                  "String": "Apache Cassandra | Apache Cassandra Documentation"
                }
              }
            ]
          }
        },
        "row_key": "org.apache.cassandra"
      }
    ],
    "rows_scanned": 8
  },
  "status": 200,
  "time_ms": 0
}
```

We get the exact same result, however, we reduce scanned bytes down to 680 bytes, and halved scanned cells, and achieved a read amplification of `1`!

## Example: Scanning another column family

Let's scan the `language` column instead, which is still stored in the default partition.

```bash
curl --request POST \
  --url http://localhost:9876/v1/table/scan-example/scan \
  --header 'content-type: application/json' \
  --data '{
  "row": {
    "prefix": ""
  },
  "column": {
    "key": "language:"
  }
}'
```

```bash
curl --request POST \
  --url http://localhost:9876/v1/table/locality-example/scan \
  --header 'content-type: application/json' \
  --data '{
  "row": {
    "prefix": ""
  },
  "column": {
    "key": "language:"
  }
}'
```

`scan_example` (no locality groups) returns:

```json
{
  "message": "Query successful",
  "result": {
    "bytes_scanned": 1141,
    "cell_count": 8,
    "cells_scanned": 16,
    "micros_per_row": 11,
    "row_count": 8,
    "rows": [
      // snip
    ],
    "rows_scanned": 8
  },
  "status": 200,
  "time_ms": 0
}
```

`locality_example` returns:

```json
{
  "message": "Query successful",
  "result": {
    "bytes_scanned": 460,
    "cell_count": 8,
    "cells_scanned": 8,
    "micros_per_row": 16,
    "row_count": 8,
    "rows": [
      // snip
    ],
    "rows_scanned": 8
  },
  "status": 200,
  "time_ms": 0
}
```

From `1141` bytes down to `460`, that's a **60%** decrease!
