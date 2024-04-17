---
title: Data retention
description: Configuring data retention in Smoltable
---

Each _column family_'s data retention can be configured using two garbage collection (GC) mechanisms:

- TTL
- Version limit

These allow you to delete cells that are (1) too old, or (2) have too many versions stored, to reduce storage costs. Both GC mechanisms are disabled by default.

:::note
Garbage collection happens asynchronously and lazily, so data may live longer than the defined
limits.
:::
