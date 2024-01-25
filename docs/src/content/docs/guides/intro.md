---
title: What is Smoltable?
description: What is Smoltable?
---

Smoltable is a tiny wide-column store heavily inspired by [Google Bigtable](https://static.googleusercontent.com/media/research.google.com/de//archive/bigtable-osdi06.pdf). It is implemented in Rust and is based on a [custom-written LSM storage engine](https://github.com/fjall-rs/fjall), also written in Rust. It can be accessed through a JSON REST API, with potential plans for future embeddability.

## Compatibility

Smoltable is not a replacement for Bigtable, nor is it wire-compatible with it. It is not distributed, but you probably could make it distributed. Then we would have `Bigsmoltable`. But it is a great, inexpensive way to learn about wide-column and single table data design.

## License

All [source code](https://github.com/marvin-j97/smoltable) is (MIT OR Apache 2.0)-licensed.
