# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Commands

**Never run `go` commands directly.** Always use the Makefile:

```bash
make -C /path/to/KeyValor build-check          # compile check (production + test code)
make -C /path/to/KeyValor test-run             # run all tests with output log
make -C /path/to/KeyValor test-run RUN=TestFoo # run a single test by name
make -C /path/to/KeyValor test-run PKG=./internal/storage/... # run tests for a package
make -C /path/to/KeyValor lint                 # golangci-lint (requires custom-gcl binary)
make -C /path/to/KeyValor format               # auto-fix formatting + imports
```

All bash output must be redirected: `make ... > /tmp/<name>.log 2>&1`

**Known pre-existing build failure**: `tools/serialization_checker` fails due to `golang.org/x/tools@v0.24.0` incompatibility with the current Go toolchain. This is unrelated to any feature work — ignore it.

---

## Architecture

See `ARCHITECTURE.md` for the full design. Critical cross-file relationships are below.

### Request path

```
TCP :6379 (RESP)
  → cmd/key-val-redis/commands/commands.go   (dispatch table)
  → db.go / db_ops.go                        (KeyValorDatabase, RWMutex, pass-through)
  → DiskStorage interface                    (internal/storage/storage.go)
  → HashTableStorage                         (internal/storage/hashtable/)
```

`db_ops.go` is a pure lock-then-delegate layer. No logic lives there.

### Storage engine selection

`NewKeyValorDB` in `db.go:25` hardwires `HashTableStorage`. Swapping to `LSMTreeStorage` requires changing that one call — once LSM implements `Init()` and `Close()` (currently missing; see `docs/01-gaps.md`).

### HashTableStorage — key invariants

- All writes are appends to `ActiveDataFile` (`wal_file_N.db`). Never seek on write.
- `keyLocationIndex DatabaseIndex` maps every live key to `Meta{FileID, RecordOffset, RecordSize}`. Every read is a single `file.ReadAt(offset, size)`.
- `IndexFlushLoop` goroutine snapshots the map under `RLock`, releases the lock, then writes to disk via `fileutils.AtomicReplaceFile`. Disk I/O is never done under any lock.
- `Close()` acquires the write lock before calling `keyLocationIndex.Flush()`.

### Locking model

`HashTableStorage` embeds `storagecommon.CommonStorage` which embeds `sync.RWMutex`. All public ops acquire `RLock` or `Lock` at the top. Internal helpers (`get`, `set`) are called with the lock already held — never re-acquire inside them.

### Atomic file writes

`fileutils.AtomicReplaceFile(dst, func(f *os.File) error)` — unique temp file via `os.CreateTemp`, fsync, rename, dir-sync. Use this for every file that must survive a crash mid-write.

### LSMTreeStorage status

Not wired into the DB. `Init()`, `Close()`, `Exists()`, `Keys()`, `AllKeys()` are unimplemented or panic. Constructor discards loaded state. All gaps in `docs/01-gaps.md`.

---

## Interface Architecture — SOLID is mandatory, not optional

Every layer boundary in this project is an interface. Hard coupling to concrete types is a design violation. The full interface map:

| Interface | Package | Purpose | Current concrete type |
|-----------|---------|---------|----------------------|
| `DiskStorage` | `internal/storage` | Entire storage engine | `HashTableStorage` |
| `DatabaseOperations` = `ReadOnlyOps` + `WriteOps` | `dbops` | ISP-split operation set | Both engines |
| `DatabaseIndex` | `storagecommon` | Index persistence strategy | `CheckpointIndex` |
| `AppendOnlyFile` | `datafile` | Write-only file | `ReadWriteDataFile` |
| `AppendOnlyWithRandomReads` | `datafile` | Active data file (write + seek) | `ReadWriteDataFile` |
| `ReadOnlyWithRandomReads` | `datafile` | Sealed old data files | `ReadWriteDataFile` |
| `Header` | `records` | Binary record header codec | `CommandHeader`, `DataRecordHeader` |
| `Record[K]` | `records` | Generic binary record codec | `CommandRecord`, `PositionRecord` |
| `Logger` | `log` | Logging backend | `ZapLogger` |

### Rules that must never be violated

1. **Never type-assert through an interface to reach a concrete type.** If you need a method only on the concrete type, that method belongs on the interface. The `DatabaseIndex` type assertion bug (`keyLocationIndex.(*CheckpointIndex)`) was a review finding and was fixed by adding `FlushSnapshot` to the interface.

2. **Swapping a concrete type must require changing exactly one line** — the wiring point. For storage engine: `db.go:25`. For index strategy: `NewHashTableStorage`. No other file should know which concrete type is in use.

3. **Pass the narrowest interface that satisfies the use.** The active data file is `AppendOnlyWithRandomReads`; old files are `ReadOnlyWithRandomReads`; the merge temp file uses `AppendOnlyFile`. Do not widen.

4. **All on-disk binary formats go through `RecordEncoder[K, H Header, R Record[K]]`** (`internal/records/`). This single generic codec serves hash table data files, LSM WAL, SSTable data regions, and SSTable sparse indexes. Never write a one-off binary encoder.

5. **`DatabaseOperations` is ISP-split into `ReadOnlyOps` + `WriteOps`.** A future read-only replica needs to implement only `ReadOnlyOps`. Do not collapse them.
