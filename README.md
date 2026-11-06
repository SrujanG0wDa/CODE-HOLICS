# CODE-HOLICS

## Bitcask Key-Value Store (`kvdb`)

A production-grade, crash-safe, Bitcask-style append-only key-value store built in Go (standard library only).

### Features
- **On-Disk Bitcask Format**: Little-endian log entries `[crc:4][timestamp:4][key_size:4][val_size:4][key][value]` with CRC-32 IEEE verification.
- **In-Memory Index**: Fast key lookups mapped to exact byte offsets for $O(1)$ read performance.
- **ACID Durability**: Synchronous disk flushes (`file.Sync()`) before acknowledging writes. Broken-state circuit breaker on failure.
- **Atomic Batches**: Single-buffer atomic writes using reserved marker records `\x00\x00kvdb-batch-v1` with all-or-nothing rollback.
- **Crash Recovery & Torn-Tail Detection**: Automatic truncation of torn/partial writes at startup.
- **Zero-Downtime Compaction**: Live record rewriting to `.compact` followed by directory sync and atomic swap.
- **Protocol Modes**: Interactive `serve` line-oriented protocol, standard CLI, and HTTP/REST daemon.

---

### Build & Test

```bash
# Build binary
make build

# Run static checks
make vet

# Run unit and stress tests
make test
```

---

### CLI Usage

```powershell
# Put a key
./kvdb mydb.db put user:101 "Alice"

# Get a key
./kvdb mydb.db get user:101

# Delete a key
./kvdb mydb.db del user:101

# Dump all live keys
./kvdb mydb.db dump

# Check log integrity
./kvdb mydb.db check

# Compact database
./kvdb mydb.db compact
```

---

### Serve Mode (Line Protocol)

```powershell
./kvdb mydb.db serve
```

Send commands via stdin:
```text
1 PUT user:101 Alice
ACK 1
2 GET user:101
VAL 2 Alice
3 BATCH 2
PUT k1 v1
PUT k2 v2
ACK 3
```

---

### HTTP / REST Mode

```powershell
./kvdb mydb.db http :8080
```

- `GET /keys/<key>` - Retrieve value
- `PUT /keys/<key>` - Store value from request body
- `DELETE /keys/<key>` - Delete key
- `GET /dump` - JSON map of all live keys
- `POST /compact` - Trigger compaction
