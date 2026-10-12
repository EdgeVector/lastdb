# Local backup receipt packs

The receipt pack keeps the exact original JSON bytes for each receipt.
It preserves receipt versions, job IDs, sequence numbers, object identities, and outcomes.
The reader supports legacy numbered files, packed blocks, and the complete terminal archive.

## One exact terminal job

Run the local command with one job UUID:

```bash
lastdb backup-gc-compact --job-id <job-uuid> --json
```

The default command creates or verifies `backup_gc_jobs/<job-uuid>/objects-v1.pack`.
It keeps the old source files.
The command uses local receipt files. It opens no database and makes no cloud request.
The primary stays online.
Use the global `--data-dir <node-home>` option before the command for a different home.

Add the explicit source removal option after review:

CAUTION: A legacy daemon has no packed receipt reader.
Its historical object pages can fail after the numbered source files are removed.
Activate the packed reader first if those pages must stay available.
Summary status and request-ID retry stay available because the summaries and index remain.

```bash
lastdb backup-gc-compact --job-id <job-uuid> --remove-sources --json
```

The command checks each source against the durable terminal archive before it removes that source.
Loose `intent`, `unknown`, and `uncertain_write` receipts stay in place.
The complete archive also keeps their exact original bytes.
Old packed blocks can be removed only after every original record matches the complete archive.
The command refuses active jobs, queued jobs, pending dispatches, and IDs in the active index.
It does not change job summaries, index contents, capacity counters, or cloud objects.
An unrelated live job can update the index while this command runs.

## Pack format

The format name is `LDBGCP01`. Integer fields use little-endian byte order.
The 44-byte header contains the following fields:

| Offset | Bytes | Value |
|---|---|---|
| 0 | 8 | `LDBGCP01` magic |
| 8 | 4 | Format version `1` |
| 12 | 16 | RFC 4122 job UUID bytes |
| 28 | 8 | First receipt sequence; `1` for a complete terminal archive |
| 36 | 4 | Slot count |
| 40 | 4 | Present record count |

Each slot has an absolute file offset as `u64` and a payload length as `u32`.
An absent slot has offset zero and length zero. Payload bytes contain the original JSON file bytes.
The caps are 1,048,576 slots, 64 KiB per receipt, and 512 MiB per pack.

The report gives receipt counts, pack size, removed source counts, and summary and pack SHA256 hashes.
It emits no cloud object keys. A repeated command verifies the archive and can finish a partial source removal.
Validation uses source review, format, lint, build, and exact receipt byte checks. No tests are required or run.
