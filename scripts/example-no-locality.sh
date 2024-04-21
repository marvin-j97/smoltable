curl --request PUT \
  --url http://localhost:9876/v1/table/no-locality-example >/dev/null 2>&1

curl --request POST \
  --url http://localhost:9876/v1/table/no-locality-example/column-family \
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
}' >/dev/null 2>&1

curl --request POST \
  --url http://localhost:9876/v1/table/no-locality-example/write \
  --header 'content-type: application/json' \
  --data '{
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
          "column_key": "language:",
          "type": "string",
          "value": "EN"
        }
      ]
    },
    {
      "row_key": "org.apache.solr",
      "cells": [
        {
          "column_key": "title:",
          "type": "string",
          "value": "Welcome to Apache Solr - Apache Solr"
        },
        {
          "column_key": "language:",
          "type": "string",
          "value": "EN"
        }
      ]
    },
    {
      "row_key": "org.apache.hbase",
      "cells": [
        {
          "column_key": "title:",
          "type": "string",
          "value": "Apache HBase - Apache HBase™ Home"
        },
        {
          "column_key": "language:",
          "type": "string",
          "value": "EN"
        }
      ]
    },
    {
      "row_key": "org.apache.lucene",
      "cells": [
        {
          "column_key": "title:",
          "type": "string",
          "value": "Apache Lucene - Welcome to Apache Lucene"
        },
        {
          "column_key": "language:",
          "type": "string",
          "value": "EN"
        }
      ]
    },
    {
      "row_key": "org.apache.kafka",
      "cells": [
        {
          "column_key": "title:",
          "type": "string",
          "value": "Apache Kafka"
        },
        {
          "column_key": "language:",
          "type": "string",
          "value": "EN"
        }
      ]
    },
    {
      "row_key": "org.apache.cassandra",
      "cells": [
        {
          "column_key": "title:",
          "type": "string",
          "value": "Apache Cassandra | Apache Cassandra Documentation"
        },
        {
          "column_key": "language:",
          "type": "string",
          "value": "EN"
        }
      ]
    },
    {
      "row_key": "org.apache.parquet",
      "cells": [
        {
          "column_key": "title:",
          "type": "string",
          "value": "Apache Parquet"
        },
        {
          "column_key": "language:",
          "type": "string",
          "value": "EN"
        }
      ]
    },
    {
      "row_key": "org.apache.arrow",
      "cells": [
        {
          "column_key": "title:",
          "type": "string",
          "value": "Apache Arrow | Apache Arrow"
        },
        {
          "column_key": "language:",
          "type": "string",
          "value": "EN"
        }
      ]
    }
  ]
}' >/dev/null 2>&1

curl --request POST \
  --url http://localhost:9876/v1/table/no-locality-example/scan \
  --header 'content-type: application/json' \
  --data '{
  "row": {
    "prefix": ""
  },
  "column": {
    "key": "title:"
  }
}' 2>/dev/null | jq

curl --request DELETE \
  --url http://localhost:9876/v1/table/no-locality-example  >/dev/null 2>&1
