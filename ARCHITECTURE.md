# KeyValor Architecture

KeyValor is a Redis-compatible, persistent key-value database written in Go. It exposes a Redis wire protocol server and stores data durably on disk using a pluggable storage engine.

---

## System Overview

```
Redis Client (redis-cli, any Redis SDK)
        │ TCP :6379  (RESP protocol)
        ▼
┌──────────────────────────────────────┐
│   cmd/key-val-redis/main.go          │  TCP server (tidwall/redcon)
│   cmd/key-val-redis/commands/        │  Command dispatch table
└──────────────┬───────────────────────┘
               │
               ▼
┌──────────────────────────────────────┐
│   KeyValorDatabase  (db.go)          │  Public API + RWMutex
│   db_ops.go                          │  Thin forwarding layer
└──────────────┬───────────────────────┘
               │ DiskStorage interface
               ▼
┌──────────────────────────────────────────────────────────┐
│  Storage Engine (pluggable)                              │
│                                                          │
│  ┌──────────────────────┐  ┌───────────────────────────┐ │
│  │  HashTableStorage    │  │  LSMTreeStorage           │ │
│  │  (stable, wired in)  │  │  (partially implemented)  │ │
│  └──────────────────────┘  └───────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────┐
│   Disk (data files, index, WAL)      │
└──────────────────────────────────────┘
```

---

## Layer 1: Redis Server (`cmd/key-val-redis/`)

Listens on `:6379` using `tidwall/redcon`, which speaks the Redis wire protocol (RESP). Every inbound command is dispatched via `CommandMap` in `commands.go`:

```
"ping", "quit", "set", "get", "del", "keys", "exists", "expire", "ttl"
```

Each handler parses raw `[][]byte` args, calls the corresponding `KeyValorDatabase` method, and writes a RESP-formatted response back to the connection.

---

## Layer 2: Database API (`db.go`, `db_ops.go`)

`KeyValorDatabase` holds:
- `sync.RWMutex` — readers share the lock; writers hold it exclusively
- `cfg *config.DBCfgOpts` — directory, intervals, file size limits
- `storage DiskStorage` — the pluggable engine

`db_ops.go` is a pure pass-through: every method locks, delegates to storage, unlocks. No logic lives here.

**Configuration** (`config/db_config.go`):

| Field | Default | Purpose |
|---|---|---|
| `Directory` | `.` | Where data files are stored |
| `SyncWriteInterval` | 1 min | Intended fsync interval (not yet hooked up to any goroutine) |
| `CompactInterval` | 2 hours | Compaction background loop interval |
| `CheckFileSizeInterval` | 1 min | File rotation check interval |
| `MaxActiveFileSize` | 5 MB | Rotate active file when it exceeds this |

---

## Layer 3: Storage Interface

```go
type DiskStorage interface {
    Init() error
    Close() error
    dbops.DatabaseOperations  // Get, MGet, Set, Delete, Exists, Keys, AllKeys,
                              // TTL, SetEx, Expire, Persist, Incr, Decr
}
```

`NewKeyValorDB` currently hardwires `HashTableStorage` at `db.go:25`. Swapping to `LSMTreeStorage` requires changing that one line — once LSM implements `Init()` and `Close()`.

---

## Layer 4a: HashTableStorage (`internal/storage/hashtable/`)

The stable, production-wired engine. All writes are appends; reads are a single disk seek via an in-memory index.

### Core Data Structures

```
HashTableStorage
├── ActiveDataFile       AppendOnlyWithRandomReads  ← current write target (wal_file_N.db)
├── olddatafileFilesMap  map[int]ReadOnlyFile        ← sealed old files
└── keyLocationIndex     DatabaseIndex               ← in-memory hash: key → Meta
```

`Meta` is the index entry — it tells you exactly where to find a key's value on disk:

```go
type Meta struct {
    Timestamp    int64
    FileID       int
    RecordOffset int64
    RecordSize   int
}
```

### On-Disk Files

| File | Purpose |
|---|---|
| `wal_file_N.db` | Data files (N = 1, 2, 3 …) |
| `wal_file.merged.wip` | Temporary file during compaction |
| `hashtable.index` | Gob-encoded `map[string]Meta` index snapshot |
| `store.lock` | Exclusive process lock (unix flock) |

### Record Format on Disk

```
┌──────────────────────────────────────────────────────────┐
│  Header (24 bytes, fixed, binary little-endian)          │
│   CRC32 uint32 | Timestamp int64 | Expiry int64          │
│   KeySize int32 | ValSize int32                          │
├──────────────────────────────────────────────────────────┤
│  Key  (KeySize bytes, raw string)                        │
├──────────────────────────────────────────────────────────┤
│  Value (ValSize bytes, raw bytes)                        │
└──────────────────────────────────────────────────────────┘
```

CRC32 is computed over the value only and verified on every read.

### Write Path (SET)

```
1. Validate key and value (non-empty, within size limits)
2. Build Header: CRC32(value), timestamp_ns, expiry=0, len(key), len(value)
3. Borrow *bytes.Buffer from sync.Pool
4. Encode: binary header → raw key bytes → raw value bytes
5. Capture startOffset = ActiveDataFile.GetCurrentWriteOffset()
6. ActiveDataFile.Write(buf.Bytes())   ← O_APPEND, no seek
7. keyLocationIndex.Put(key, Meta{fileID, startOffset, recordSize})
8. Return buffer to pool
```

### Read Path (GET)

```
1. keyLocationIndex.Get(key) → Meta{fileID, offset, size}
2. If fileID == ActiveDataFile.ID() → use active file
   Else → look up olddatafileFilesMap[fileID]
3. file.ReadAt(offset, size) → raw bytes   ← single syscall, no scan
4. binary.Read header (24 bytes, little-endian)
5. value = data[size - valSize : size]
6. Check IsExpired() and IsChecksumValid()
7. Return value bytes
```

Reads are always a single `ReadAt` call — the index gives the exact byte position.

### Delete (tombstone)

```
1. Write a record with empty value to disk  ← tombstone marker
2. keyLocationIndex.Delete(key)              ← remove from in-memory index
```

The tombstone on disk signals to compaction that this key is gone.

### TTL / Expiry

`Expiry` in the header is a nanosecond Unix timestamp (0 = no expiry). Every `Get` calls `IsExpired()` after reading the record. `SetEx` pre-sets `Expiry` on write. `Expire` reads the record and rewrites it with the new expiry. `Persist` rewrites with `Expiry = 0`.

### File Rotation

`FileRotationLoop` goroutine ticks every `CheckFileSizeInterval`:

```
If ActiveDataFile.Size() >= MaxActiveFileSize:
  olddatafileFilesMap[currentID] = ActiveDataFile
  ActiveDataFile = new wal_file_(currentID+1).db
```

### Compaction

`CompactionLoop` goroutine ticks every `CompactInterval`:

```
1. Scan index; for each key check IsExpired() → delete expired keys and write tombstones
2. Open wal_file.merged.wip as temp file
3. For every live key in index:
   → read record from its current file
   → write into temp file
   → update index entry to point to new location
4. gob-encode index → hashtable.index
5. Close and delete all wal_file_*.db files
6. os.Rename(wal_file.merged.wip → wal_file_0.db)
7. fsync the directory (makes rename durable)
8. Reopen wal_file_0.db as new ActiveDataFile
```

After compaction: one file, one copy of each live key, all overwritten/deleted space reclaimed.

### Index Persistence

`keyLocationIndex` is typed as `DatabaseIndex` (interface with `Open/Flush/FlushSnapshot/Close`). The concrete implementation is `CheckpointIndex` — a gob snapshot of `map[string]Meta`.

- **Load**: `Open()` on startup gob-decodes `hashtable.index` if it exists; no-op otherwise.
- **Flush**: Atomic write via `fileutils.AtomicReplaceFile` — unique temp file (`os.CreateTemp`), fsync, rename, dir-sync. Crash during flush leaves the previous snapshot intact.
- **Periodic flush**: `IndexFlushLoop` goroutine fires every `SyncWriteInterval` (default 1 min). It snapshots the map under `RLock`, releases the lock, then flushes — writes are only blocked for the in-memory copy, not the disk I/O.
- **Shutdown flush**: `Close()` acquires the write lock, calls `Flush()`, then `Close()` on the index before closing data files.

**Crash risk**: At most `SyncWriteInterval` of index updates can be lost.

### Lock File

`CommonStorage` acquires `unix.Flock(LOCK_EX|LOCK_NB)` on `store.lock` at startup, preventing two processes from opening the same directory. Released on `Close()` via `unix.Flock(LOCK_UN)` + file close + `os.Remove`.

---

## Layer 4b: LSMTreeStorage (`internal/storage/lsmtree/`) — Partially Implemented

The LSM tree engine has its core structure in place but is not yet wired into `NewKeyValorDB`. `Init()` and `Close()` are not implemented. Three operations (`Exists`, `Keys`, `AllKeys`) panic. There is no SSTable compaction. See `docs/01-gaps.md` for the full gap list.

### Structure

```
LSMTreeStorage
├── ActiveWALFile          AppendOnlyFile       ← current_wal_file (append-only)
├── activeMemTable         SerializableTreeMap  ← sorted in-memory write buffer
├── prevMemTableImmutable  SerializableTreeMap  ← being flushed to SSTable
└── ssTables               []*SSTable           ← on-disk sorted tables, newest-first
```

### On-Disk Files

| File | Purpose |
|---|---|
| `current_wal_file` | Active write-ahead log; replayed on startup |
| `temp_wal_file` | Renamed from current during a memtable flush; also replayed on startup |
| `data_file_<unix_ns>.sst` | Immutable SSTable flushed from a full memtable |

### Write Path

```
1. Create CommandRecord{CmdType=Set, key, value}
2. Encode and append to ActiveWALFile   ← durability before in-memory update
3. activeMemTable.Put(key, cmdRecord)
4. If activeMemTable.Size() >= MAX_ENTRIES_IN_MEMTABLE (100) → rotateMemTableIndex()
```

`rotateMemTableIndex()`:
```
1. prevMemTableImmutable = activeMemTable
2. activeMemTable = new empty SerializableTreeMap
3. Rename current_wal_file → temp_wal_file
4. Open fresh current_wal_file as ActiveWALFile
5. persistMemtableToSSTable(prevMemTableImmutable) → write data_file_<unix_ns>.sst
6. prevMemTableImmutable = nil
7. fsync directory
```

### Read Path (cascading lookup)

```
1. activeMemTable.Get(key)        ← O(log n) red-black tree
2. prevMemTableImmutable.Get(key) ← if a flush is in progress
3. [TODO: bloom filter]
4. For each SSTable in ssTables[] newest-first: ssTable.Query(key)
5. Return first match, or ErrKeyMissing
```

Delete writes `CommandRecord{CmdType=Del}` to WAL and memtable. The read path converts a `Del` record into `ErrKeyMissing`.

### Startup Recovery

`processExistingFiles` scans the data directory:

| File found | Action |
|---|---|
| `current_wal_file` | Replay all commands → rebuild `activeMemTable`; open as `ActiveWALFile` |
| `temp_wal_file` | Replay all commands → merge into `activeMemTable` (crash during flush) |
| `data_file_<ts>.sst` | Load from disk via `NewSSTableLoadedFromFile`; add to `ssTables[]` sorted by timestamp |

**Known bug**: `NewLSMTreeStorage` loads state into a local variable via `processExistingFiles`, then returns a brand-new struct literal that discards all of it. Startup recovery is currently broken (gap #4 in `docs/01-gaps.md`).

---

## Layer 5: SSTable (`internal/sstable/`)

A sorted, immutable file flushed from a full memtable. File name: `data_file_<unix_ns>.sst`. Batch size is 100 records.

### File Layout

```
┌──────────────────────────────────┐
│  Data Region                     │  sorted CommandRecord batches (100 records each)
├──────────────────────────────────┤
│  Index Region                    │  SerializableTreeMap<string, *PositionRecord>
│  (sparse index, serialized)      │  one entry per batch: first_key → Position{Start, Size}
├──────────────────────────────────┤
│  Metadata (48 bytes, fixed LE)   │  Version, BatchSize, DataStart, DataSize,
│                                  │  IndexStart, IndexSize
└──────────────────────────────────┘
```

### Write (flush from memtable)

```
1. Iterate memtable in sorted key order
2. Every 100 records: encode as CommandBatch → append to data region
   Record first_key + Position{Start, Size} → sparseIndex
3. Encode sparseIndex (SerializableTreeMap.Encode) → append to index region
4. Encode SSTableMetaData (48 bytes, fixed LE) → append last
```

### Read (Query)

```
1. sparseIndex is in memory (loaded at startup)
2. Check: key < sparseIndex.Min or key > sparseIndex.Max → ErrKeyNotPresentInSSTable
3. sparseIndex.Floor(key) → lower batch Position
   sparseIndex.Ceiling(key) → upper batch Position
4. Seek to lower batch start; decode CommandRecord by CommandRecord up to upper batch end
5. Return on key match, else ErrKeyNotPresentInSSTable
```

### Loading from Disk

```
NewSSTableLoadedFromFile(path)
  1. ReadAt(DataStartOffset, 48) → decode SSTableMetaData
  2. ReadAt(IndexStartOffset, IndexSize) → raw bytes
  3. sparseIndex.Decode(bytes) → rebuild SerializableTreeMap in memory
  (Data region is NOT loaded — fetched on demand via ReadAt)
```

---

## Layer 6: Data Files (`internal/storage/datafile/`)

One concrete type — `ReadWriteDataFile` — exposed through three interface views depending on the caller's needs:

| Interface | Capabilities | Used by |
|---|---|---|
| `AppendOnlyFile` | Write, Close, Sync, GetCurrentWriteOffset | LSM WAL writes |
| `AppendOnlyWithRandomReads` | Above + ReadAt, Seek, Read | HashTable active data file |
| `ReadOnlyWithRandomReads` | ReadAt, Seek, Size, Close | Old HashTable files, SSTable reads |

`ReadWriteDataFile` holds two `*os.File` handles — one opened `O_APPEND|O_WRONLY` for writes, one opened `O_RDONLY` for reads — plus separate `writeOffset` and `readOffset` counters. An internal `RWMutex` guards concurrent access at the file level.

```
Write(p)        → Lock   → writer.Write(p) → writeOffset += n
ReadAt(p, pos)  → RLock  → reader.ReadAt(p, pos)
```

---

## Layer 7: Records & Serialization (`internal/records/`, `internal/storage/storagecommon/`)

### Two Record Families

Both follow the same on-disk shape: `[fixed header][key bytes][value bytes]`.

**DataRecord / Header** — HashTableStorage only:

| Field | Type | Bytes | Notes |
|---|---|---|---|
| CRC32 | uint32 | 4 | Checksum over value bytes; verified on every read |
| Timestamp | int64 | 8 | Nanoseconds since epoch |
| Expiry | int64 | 8 | Nanoseconds since epoch; 0 = no expiry |
| KeySize | int32 | 4 | |
| ValSize | int32 | 4 | |
| **Total** | | **24** | |

**CommandRecord / CommandHeader** — LSMTreeStorage and SSTables:

| Field | Type | Bytes | Notes |
|---|---|---|---|
| CmdType | byte | 1 | 0=Get, 1=Set, 2=Del |
| Expiry | int64 | 8 | |
| KeySize | int32 | 4 | |
| ValSize | int32 | 4 | |
| **Total** | | **17** | |

### Generic RecordEncoder

`RecordEncoder[K, H Header, R Record[K]]` handles encode/decode for any `(Header, Record)` pair. Both `CommandRecord` and `PositionRecord` implement `Record[string]`, so the same codec serves WAL files, SSTables, and the sparse index.

The `//go:generate` directive on `records_encoder.go` runs a static analysis tool (`cmd/tools/serialization_checker`) that verifies all `Header` implementors contain only fixed-size fields — a guard against accidentally breaking binary file compatibility.

---

## Layer 8: SerializableTreeMap (`internal/treemapgen/`)

A generic, type-safe, serializable sorted map wrapping `emirpasic/gods/treemap` (red-black tree).

```go
type SerializableTreeMap[K comparable, V records.Record[K]] struct {
    internalMap *treemap.Map
    keyType     reflect.Type
    valueType   reflect.Type
    encoder     *records.RecordEncoder[K, records.Header, V]
}
```

Two roles:
1. **Memtable** in LSMTreeStorage: `SerializableTreeMap[string, *CommandRecord]`
2. **Sparse index** in SSTable: `SerializableTreeMap[string, *PositionRecord]`

`Encode()` / `Decode()` serialize the entire map to/from bytes using `RecordEncoder`. This is how the SSTable sparse index is embedded inside the `.sst` file and reloaded at startup without scanning the data region.

---

## Concurrency Model

Three levels of locking, outermost first:

1. **`KeyValorDatabase.RWMutex`** — coarse gate. All reads share it; all writes hold it exclusively.
2. **`CommonStorage.RWMutex`** (embedded in both engines) — guards internal engine state: index map, active file pointer, old files map.
3. **`ReadWriteDataFile.RWMutex`** — guards concurrent reads and writes on a single file descriptor.

`sync.Pool` for `*bytes.Buffer` is used throughout both engines to avoid GC pressure on the write path.

---

## Startup Sequence (HashTableStorage)

```
NewKeyValorDB()
  └── NewHashTableStorage(cfg)
        1. Glob wal_file_*.db files; sort by numeric ID
        2. Open each as ReadOnlyDataFile → olddatafileFilesMap
        3. Open ID=max+1 as new AppendOnlyDataFile → ActiveDataFile
        4. NewCheckpointIndex(indexFilePath) → Open() → gob.Decode if file exists
        5. unix.Flock(LOCK_EX|LOCK_NB) on store.lock
  └── storage.Init()
        1. go CompactionLoop(CompactInterval)
        2. go FileRotationLoop(CheckFileSizeInterval)
        3. go IndexFlushLoop(SyncWriteInterval)
```

## Shutdown Sequence (HashTableStorage)

```
db.Shutdown()
  └── storage.Close()
        1. hts.Lock() → keyLocationIndex.Flush() → keyLocationIndex.Close() → hts.Unlock()
        2. ActiveDataFile.Close()
        3. Close each file in olddatafileFilesMap
        4. unix.Flock(LOCK_UN) + fd.Close() + os.Remove(store.lock)
```

---

## What Is Not Yet Implemented

See `docs/01-gaps.md` for the full gap list with severity ratings. Summary:

| Area | Gap |
|---|---|
| HashTable index | Periodic flush via `IndexFlushLoop` (every `SyncWriteInterval`); atomic write via temp+rename; no replay from data files on crash |
| LSMTreeStorage | `Init()` and `Close()` missing; cannot be wired into `NewKeyValorDB` |
| LSMTreeStorage | `Exists`, `Keys`, `AllKeys` panic |
| LSMTreeStorage | Bug in `NewLSMTreeStorage`: returns a fresh struct that discards loaded state |
| LSMTreeStorage | No SSTable compaction (SSTables grow unboundedly) |
| LSMTreeStorage | No bloom filter (every key-miss scans all SSTables) |
| Both | Background goroutines have no stop channel; keep running after `Close()` |
