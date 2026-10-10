# Verified local chunk cache for restore

Use an existing, stopped partial restore as a byte cache for a new restore:

```sh
lastdb --data-dir <source-home> restore --into <new-empty-home> \
  --reuse-chunks-from <old-partial-home> --env dev --json --progress-json
```

Keep the old home stopped and unchanged until the command exits. The destination
must be empty. The cache and destination must be separate homes without ancestor
or descendant overlap. The command does not adopt or resume the partial home.

The current cloud latest pointer and authenticated manifest chain select the cut.
The source database scope must match that manifest. The cache supplies no identity,
credentials, membership, high-water marker, or tail frontier. The restore does not
open a LastStore instance on the cache.

For each selected chunk, the restore resolves any replacement receipt to the
original native segment address. It reads the declared prefix from that address.
The declared byte count and SHA-256 must match before the normal installer accepts
the bytes. Extra bytes after the prefix stay in the cache. Extra local chunks do
not enter the destination. A missing, short, corrupt, or unsupported candidate
uses the normal cloud download path.

The cache supports numbered plain segments and UUID frame chunks.
A numbered lookup inspects at most 4096 entries in the exact addressed directory.
It computes UUIDs only for actual numbered segment names and leaves the installer
inverse map unchanged. A candidate beyond the entry cap can miss the cache. A legacy sorted
multipart piece can miss the cache. The cloud path then supplies that piece. The
installer retains its normal format, shard, group, identity, and integrity checks.

The existing queue limits apply to cached bodies: eight entries and a 128 MiB
reservation limit, with the existing serial rule for an oversized chunk. The
restore does not clone the old home. It copies only verified selected prefixes
into the destination. The remote write interlock remains active.

The progress counter `cache_read_ms` measures local lookup, read, and hash time.
It adds whole milliseconds for each cache attempt, including a miss or rejected prefix.
It excludes cloud fallback and chunk installation. An attempt below one millisecond
contributes zero. No cache option means no cache attempt and a zero counter.

The final JSON report and progress events expose `chunks_reused` and `bytes_reused`.
`chunks_installed` and `bytes_installed` include reused and downloaded chunks.
`response_body_bytes` counts network responses only. It also includes manifest and
tail responses, so it is not an S0 chunk download byte count.

## Verification

The DEV cloud proof uses synthetic records and a separate recovery home.
It restores a direct backup and a packed backup, then checks records by key.
It also checks a later create, update, Delete, a 160 KiB atom, and a local file blob.
An interrupted restore supplies 268 verified chunks and 983854 bytes to a fresh target.
That target replays 192 records and returns the same record and file values.
No test suite runs. Format, lint, and product build checks remain the code gates.
