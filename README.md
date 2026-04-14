# YARS

Yet Another Redis Server — a lightweight Redis-compatible server written in Rust.

## What is this?

YARS is an experimental Redis server implementation. It speaks the RESP2 protocol (for now), keeps data in memory, and persists it to disk via an append-only file. It's a learning project and a lightweight option for local development (not a production Redis replacement).

## Quick start


```bash
git clone https://github.com/ahmed-mekky/yars.git
cd yars
cargo run
```

Connect with any Redis client on `127.0.0.1:6379`.

## Features

- Full RESP2 protocol support
- In-memory key-value store with key expiry
- AOF persistence with configurable fsync policy

## Development

```bash
pip install pre-commit
pre-commit install
```

## License

MIT
