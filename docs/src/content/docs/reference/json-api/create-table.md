---
title: Create a table
description: Create a table using the JSON API
---

### URL

PUT http://smoltable:9876/v1/table/[name]

<!-- TODO: column families -->

### Example response

```json
{
	"message": "Table created successfully",
	"result": null,
	"status": 201,
	"time_ms": 33
}
```