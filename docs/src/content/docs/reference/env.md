---
title: Environment variables
description: Available environment variables
---

##### `SMOLTABLE_DATA`

Data directory.

*Default: .smoltable_data*

##### `SMOLTABLE_METRICS_CAP_MB`

Maximum size of metrics to store *per table*.

*Default: 1 MiB*

##### `SMOLTABLE_HTTP_PORT`

> Aliases: SMOLTABLE_PORT, PORT

HTTP port to listen to.

*Default: 9876*

##### `SMOLTABLE_WRITE_BUFFER_SIZE`

Global write buffer size, shared by all tables, locality groups, metrics tables and internal tables.

*Default: 64 MiB*
