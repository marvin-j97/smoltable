<p align="center">
  <img src="/logo.png" height="96" />
</p>
<p align="center">
  Bigtable but smol.
</p>

## About

[![CI](https://github.com/marvin-j97/smoltable/actions/workflows/test.yml/badge.svg)](https://github.com/marvin-j97/smoltable/actions/workflows/test.yml)

Smoltable is a tiny wide-column store heavily inspired by [Google Bigtable](https://static.googleusercontent.com/media/research.google.com/de//archive/bigtable-osdi06.pdf). It is implemented in Rust and is based on a [custom-written LSM storage engine](https://github.com/marvin-j97/fjall), also written in Rust. It can be accessed through a JSON REST API, with potential plans for future embeddability.

## Data model

Its data model is essentially the same as Bigtable’s, where each row:

- is identified by its row and
- can have arbitrarily many columns

Columns are grouped into column families. The table is sparse, so unused columns do not count into space usage. For each row’s column there may be multiple values sorted by time. Optionally, old versions can then be lazily & automatically deleted.

In Bigtable, stored values are byte blobs; Smoltable supports multiple data types out of the box:

- String (UTF-8 encoded string)
- Boolean (like Byte, but is unmarshalled as boolean)
- Byte (unsigned integer, 1 byte)
- I32 (signed integer, 4 bytes)
- I64 (signed integer, 8 bytes)
- F32 (floating point, 4 bytes)
- F64 (floating point, 8 bytes)

Column families can be grouped into locality groups, which partition columns into separate files, increasing scan performance over those column families (e.g. OLAP-style queries over a specific column).

## Compatibility

Smoltable is not a replacement for Bigtable, nor is it wire-compatible with it. It is not distributed, but you probably could make it distributed. Then we would have `Bigsmoltable`. But it is a great, inexpensive way to learn about wide-column and single table data design.

## License

All source code is (MIT OR Apache 2.0)-licensed.
