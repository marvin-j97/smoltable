---
title: Installation
description: Installing smoltable
---

Smoltable is available as a standalone server using Docker.

## Docker

```bash
docker run \
-d \
--rm \
--restart unless-stopped \
-e SMOLTABLE_DATA=/data \
-e SMOLTABLE_PORT=9876 \
-e RUST_LOG=warn \
-v $(pwd)/smoltable-data:/data \
-p 9876:9876 \
ghcr.io/marvin-j97/smoltable:edge-debian
```

## Docker Compose

```yaml
version: "3.8"

services:
  smoltable:
    image: ghcr.io/marvin-j97/smoltable:edge-debian
    restart: unless-stopped
    volumes:
      - "./smoltable-data:/data"
    ports:
      - "9876:9876"
    environment:
      SMOLTABLE_DATA: "/data"
      SMOLTABLE_PORT: "9876"
      RUST_LOG: warn
```

## Available images:

##### `ghcr.io/marvin-j97/smoltable:edge-alpine`

Unstable release based on latest, stable Alpine.

##### `ghcr.io/marvin-j97/smoltable:edge-debian`

Unstable release based on latest, stable Debian.
