---
title: Environment variables
description: Available environment variables
---

##### `RUST_LOG`

Log level based on [Rust log levels](https://docs.rs/log/latest/log/enum.Level.html).

_Default: -_

##### `SMOLTABLE_DATA`

Data directory.

_Default: .smoltable_data_

##### `SMOLTABLE_METRICS_CAP_MB`

Maximum size of metrics to store _per table_.

_Default: 1 MiB_

##### `SMOLTABLE_HTTP_PORT`

> Aliases: SMOLTABLE_PORT, HTTP_PORT, PORT

HTTP port to listen to.

_Default: 9876_

##### `SMOLTABLE_WRITE_BUFFER_SIZE_MB`

Global write buffer size, shared by all tables, locality groups, metrics tables and internal tables.

_Default: 64 MiB_
