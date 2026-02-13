# YARS - Yet Another Redis Server

A Redis-compatible server implementation in Rust.

## Overview

YARS (Yet Another Redis Server) is an experimental Redis server implementation written in Rust. It currently implements the RESP (Redis Serialization Protocol) parser and provides a basic TCP server that echoes back parsed commands.

## Status

**Early Development** - This project is in its initial stages. Currently implemented:

- RESP protocol parsing (all types: SimpleString, Error, Integer, BulkString, Array)
- TCP server listening on port 6379
- Basic command echo functionality

**Not Yet Implemented:**

- Actual Redis commands (GET, SET, DEL, etc.)
- Data persistence
- Authentication
- Replication
- Pub/Sub

## Tech Stack

- **Language:** Rust
- **Async Runtime:** Tokio
- **Parser:** Nom (RESP parsing)
- **Error Handling:** Anyhow

## Building

```bash
cargo build --release
```

## Running

```bash
cargo run
```

The server will start on `127.0.0.1:6379` (Redis default port).

## Testing with Redis CLI

```bash
# Connect using redis-cli
redis-cli

# Or test manually with netcat
echo -e "*1\r\n\$4\r\nTest\r\n" | nc localhost 6379
```

## RESP Implementation

YARS implements full RESP (Redis Serialization Protocol) parsing as defined in the [Redis protocol specification](https://redis.io/docs/reference/protocol-spec/). The parser handles:

| Type | Prefix | Example |
|------|--------|---------|
| Simple String | `+` | `+OK\r\n` |
| Error | `-` | `-ERR message\r\n` |
| Integer | `:` | `:1000\r\n` |
| Bulk String | `$` | `$6\r\nfoobar\r\n` |
| Array | `*` | `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` |

## License

MIT
