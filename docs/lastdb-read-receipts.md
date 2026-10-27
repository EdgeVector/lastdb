# LastDB read receipts

The native reader reads the published `CURRENT` of each disk format v2 run home. It has no internal journal or local mutation replay.
The legacy loader and replay counters stay in the response for compatibility. The v2 reader does not use them.

A read receipt measures the storage work for one query. It identifies extra
file reads, bytes, and time before the pack file and disk directory changes.
The receipt does not change the file format or the warm-set byte budget.

## Request and response

Send `x-lastdb-read-receipt: true` with `POST /api/query` on the owner socket.
The response header `x-lastdb-read-receipt` contains a compact JSON object.
Ordinary queries omit the receipt header. Request telemetry uses its own
per-request scope for actual group-open counts.

The `query_storage` scope covers one query after request parse and before the
response envelope. It includes schema resolution and the final record result.
It follows the query into the core's blocking calls and attached
workers. An unrelated request does not contribute to that scope.
An absent receipt differs from a receipt whose counters are zero.

`POST /api/queries/batch` passes the opt-in header to each query. Each result
item includes `read_receipt` beside `status` and `response` when a receipt exists.
A parse rejection before the scope produces no receipt. An error inside the
scope can carry the partial receipt.

## Counter meanings

| Field | Meaning |
| --- | --- |
| `version` | Receipt format version, currently `1`. |
| `scope` | `query_storage`: work after request parse and before the response envelope. |
| `requested_keys` | Storage lookup attempts. This includes fallback probes and repeated requests for the same key. It is not the number of API records. |
| `logical_warm_hits` | Exact lookup results from the logical core path, including confirmed absence. |
| `logical_warm_misses` | Exact lookup attempts that the logical warm path cannot supply. |
| `loader_calls` | Storage group loader entry attempts, including cap refusals. |
| `group_opens` | Existing group directories that enter the storage parser. |
| `group_load_nanos` | Inclusive time in admitted group reconstruction. This overlaps the file, metadata, and decode times. |
| `returned_body_bytes` | Exact response body bytes after the result envelope. Header bytes are separate. |
| `file_open_attempts` | Read-only file open attempts, including failed attempts. |
| `file_opens` | Successful read-only file opens. A repeated open counts again. |
| `metadata_operations` | Explicit file status, existence, and directory-entry status calls. |
| `directory_reads` | Directory enumeration attempts. |
| `directory_entries` | Directory entries returned to the caller. |
| `read_operations` | File read calls, including EOF and failed calls. |
| `legacy_replay_bytes` | Bytes read for legacy group replay or append-tail replay. |
| `footer_bytes` | Bytes read for sorted footers and key sidecars. |
| `payload_bytes` | Bytes read from addressed record or sorted-block extents. |
| `format_probe_bytes` | Bytes read for format, manifest, part, or allocation-bound probes. |
| `decoded_bytes` | Output bytes from successful frame decode or plain replay copies. |
| `decrypted_bytes` | Plaintext bytes from the measured at-rest value envelope decode and inflate. |
| `temporary_bytes_peak` | Peak concurrent capacity of the transient byte buffers that the receipt measures. |
| `read_nanos` | Sum of file read time, including failed calls and OS-cache reads. |
| `decode_nanos` | Sum of authentication, decompression, and plain-copy time. |
| `decrypt_nanos` | Sum of at-rest value envelope decode and inflate time in `open_or_discard`. |
| `lock_wait_nanos` | Sum of waits for selected LastStore locks and core warm-set locks. |
| `blocking_queue_wait_nanos` | Sum of dispatch delay before an attached core blocking call starts. |
| `socket_queue_wait_nanos` | Wait for the socket worker before this request starts. |
| `open_nanos` | Sum of read-only file open time, including failed attempts. |
| `metadata_nanos` | Sum of explicit status and directory enumeration time. |
| `query_storage_elapsed_nanos` | Elapsed time for the complete query scope. |
| `filesystem_pages` | `unknown`: the receipt does not measure the operating system's filesystem page state. |
| `temporary_bytes_scope` | `instrumented_byte_buffers`: the scope of the transient byte peak. |

The addressed-read byte classes are legacy replay, footer, payload, and format-probe bytes.
They measure userspace reads from the operating system. They do not measure
the bytes that a disk device transfers. A warm miss does not prove a cold
filesystem page. A group open does not prove a new file open.

Time fields use nanoseconds. Parallel workers can overlap, so their summed
time can exceed `query_storage_elapsed_nanos`. HTTP elapsed time also includes
request parse, response transfer, and the caller's work.
The receipt retains no file paths or key values.

`blocking_queue_wait_nanos` measures the core dispatch delay for
`block_in_place` or `spawn_blocking`. `socket_queue_wait_nanos` measures the
separate socket worker wait. The query scope starts after that socket wait.
The batch items share the outer socket request's wait. Do not sum that value
across the items as separate socket waits.

The transient byte peak excludes returned bodies, warm caches, allocator
overhead, parsed metadata objects, and buffers outside the measured path.
It does not measure the process memory size or prove the warm set cap.

## Scope and limits

- The supported product shapes are an exact key, several exact keys, and a
  bounded range under one hash through `POST /api/query`.
- The receipt includes the measured native read path and its typed core lookups.
  Older receipts also contain legacy loader, replay, and sidecar work. Current native queries do not select that engine.
- The query can read one extra record to determine `has_more`. A request with
  `limit: 2` can therefore read three candidate records.
- `requested_keys` covers measured exact storage and typed core lookups.
  Those calls include schema and molecule work, atom
  bodies, fallback probes, and atom reads for the page lookahead.
- The logical hit, miss, and requested-key counters do not count the keys
  that a range enumerates. They need not equal the returned record count.
- A confirmed absent key can count as a warm hit.
  These hit and miss counts do not measure the warm set's record count.
- The receipt does not count writes, file sync calls, cloud reads, schema
  service network calls, or storage calls outside its attached context.
- At-rest value decode and inflate contribute to `decrypt_nanos`. Upstream
  atom-owner content decryption is outside this measurement.
- `lock_wait_nanos` covers selected storage and core memory locks.
  It does not measure every lock that a complete query uses.
- The time fields cover named code paths. A zero field does not prove zero
  time for all decryption, all locks, all queues, or the complete query.
- A file read that returns no bytes increases `read_operations` without a
  byte increase. A failed open increases `file_open_attempts` alone.
- The receipt is a cost measurement. Correct field values, range order,
  missing keys, and page completion need separate product result checks.
- These direct commands use generated data on a fresh node. They do not
  establish primary behavior, backup recovery, or upgrade readiness.

## Direct commands on generated data

CAUTION: Use a fresh empty home for this command sequence. Do not name the
primary home, copy primary data, or restart a shared daemon.

Run these commands in one shell. Set `candidate` to the absolute path of the
candidate daemon. Keep the resolved socket path under the macOS socket limit.
The node creates its own identity. Do not print or export that identity.

```bash
set -euo pipefail
candidate=/absolute/path/to/candidate/lastdbd
receipt_root="$(mktemp -d "${TMPDIR:?}/rd.XXXXXX")"
receipt_home="$receipt_root/node"
receipt_socket="$receipt_root/q.sock"
receipt_full_socket="$receipt_root/f.sock"
mkdir -p "$receipt_home"
env -u SENTRY_DSN -u FOLD_SENTRY_DSN \
  LASTDB_ISOLATED_COPY=1 LASTDB_SUPPRESS_RECOVERY_PHRASE=1 \
  LASTDB_SELF_METRICS_SAMPLE_SECS=1 \
  RUST_MIN_STACK=16777216 \
  "$candidate" --data-dir "$receipt_home" \
  --socket-path "$receipt_socket" --full-socket-path "$receipt_full_socket" \
  >"$receipt_root/node.log" 2>&1 </dev/null &
receipt_pid=$!
```

The native engine is the default. Every native write waits for disk commit and clean core publication before the ack.
No engine or resident-mode option is required. There is no collection timer or early memory ack.
Legacy Cloud Sync activation receives an explicit disabled error. A fresh local home needs no cloud configuration.
The [native route summary](lastdb-disk-native-node.md) separates local operation from the gated native backup source.
Wait for the socket, then read the health result. Continue when `status` is `ok`.

```bash
curl --fail-with-body --silent --show-error --max-time 10 \
  --unix-socket "$receipt_socket" http://localhost/health
```

Load the generated schema with the local product setup route. This route
requires the isolated-node flag. It does not contact the shared schema service.
`LASTDB_ISOLATED_COPY` identifies the isolated process; this sequence creates
new data and does not copy a database.

```bash
curl --fail-with-body --silent --show-error --max-time 30 \
  --unix-socket "$receipt_socket" -H 'Content-Type: application/json' \
  --data-binary '{"isolated_copy":true,"schema":{"source":"system_seed","name":"ReadReceiptProof","descriptive_name":"Read Receipt Proof","schema_type":"HashRange","key":{"hash_field":"bucket","range_field":"id"},"fields":["bucket","id","title","body"],"field_descriptions":{"bucket":"generated record hash","id":"generated record range","title":"generated title","body":"generated body"}}}' \
  http://localhost/api/schemas/seed-system
schema=ReadReceiptProof
```

Create eight generated records in one mutation batch. Each key is explicit.
The command sends the JSON directly to the product route and saves no helper.

```bash
jq -nc --arg schema "$schema" '{convergence:"sync",mutations:[range(0;8) | . as $n | ("record-" + ($n|tostring)) as $id | {type:"mutation",schema:$schema,mutation_type:"create",key_value:{hash:"generated",range:$id},fields_and_values:{bucket:"generated",id:$id,title:("title-" + ($n|tostring)),body:("body-" + ($n|tostring))}}]}' \
  | curl --fail-with-body --silent --show-error --max-time 60 \
    --unix-socket "$receipt_socket" -H 'Content-Type: application/json' \
    --data-binary @- http://localhost/api/mutations/batch
```

Read one exact record. Save the response headers and the result separately.

```bash
curl --fail-with-body --silent --show-error --max-time 30 \
  --unix-socket "$receipt_socket" -H 'Content-Type: application/json' \
  -H 'x-lastdb-read-receipt: true' \
  --dump-header "$receipt_root/point.headers" --output "$receipt_root/point.json" \
  --data-binary '{"schema_name":"ReadReceiptProof","fields":["title","body"],"filter":{"HashRangeKey":{"hash":"generated","range":"record-0"}}}' \
  http://localhost/api/query
jq '.results' "$receipt_root/point.json"
```

Read several exact records in one query. A missing exact key is useful evidence
for fallback-probe cost.

```bash
curl --fail-with-body --silent --show-error --max-time 30 \
  --unix-socket "$receipt_socket" -H 'Content-Type: application/json' \
  -H 'x-lastdb-read-receipt: true' \
  --dump-header "$receipt_root/keys.headers" --output "$receipt_root/keys.json" \
  --data-binary '{"schema_name":"ReadReceiptProof","fields":["title","body"],"filter":{"HashRangeKeys":[["generated","record-1"],["generated","record-4"],["generated","absent"]]}}' \
  http://localhost/api/query
jq '.results' "$receipt_root/keys.json"
```

Read a bounded range under the same hash. The result contains at most two
records. The receipt can include the third candidate for page lookahead.

```bash
curl --fail-with-body --silent --show-error --max-time 30 \
  --unix-socket "$receipt_socket" -H 'Content-Type: application/json' \
  -H 'x-lastdb-read-receipt: true' \
  --dump-header "$receipt_root/range.headers" --output "$receipt_root/range.json" \
  --data-binary '{"schema_name":"ReadReceiptProof","fields":["title","body"],"filter":{"HashRangeRange":{"hash":"generated","start":"record-2","end":"record-6"}},"limit":2}' \
  http://localhost/api/query
jq '{results,returned_count,has_more,next_cursor}' "$receipt_root/range.json"
```

The independent-query batch shape uses the same query objects:

```json
{"queries":[{"schema_name":"ReadReceiptProof","fields":["title"],"filter":{"HashRangeKey":{"hash":"generated","range":"record-0"}}},{"schema_name":"ReadReceiptProof","fields":["title"],"filter":{"HashRangeKey":{"hash":"generated","range":"record-7"}}}]}
```

Send it to `POST /api/queries/batch` with the receipt opt-in header. The route
accepts at most 64 query items and returns item results in request order.

## Restart and comparison

Read the same keys twice to compare the first request and the repeat request.
For a process-cold measurement, stop only this owned node with `SIGTERM`.
Wait for its exit, then launch the same candidate against the same generated
home. Use the same explicit home and socket flags.

```bash
kill -TERM "$receipt_pid"
wait "$receipt_pid"
```

A restart clears process memory. It does not clear the operating system's
filesystem pages. Record that the filesystem page state is unknown.
Do not label that result as a cold disk measurement.

For each comparison, preserve the candidate commit, node configuration,
generated record count and sizes, query shape, returned values, elapsed time,
and receipt. Keep the warm-set byte budget at its default. Use an ordinary status
read for the node's warm-set bytes and count; the receipt peak uses different units.

Stop the owned node after the final read. Preserve the required proof files
outside its temporary home. Check the exact temporary path, realpath, owner,
and stopped-node state before the supported cleanup. Retain the path if cleanup
requires an approval or a guard refuses the action.
