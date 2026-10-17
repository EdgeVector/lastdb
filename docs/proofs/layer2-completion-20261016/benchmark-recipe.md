# Benchmark command recipe

CAUTION: Use independent generated homes. Do not use the primary, a primary copy, or the archive.

This recipe describes the saved performance calls. It is not a test suite or a fixture harness.
The input records contain no user data.
The receipts contain exact binary hashes, input hashes, sample counts, and timestamps.
Use the binary with the recorded hash for each result.

## Physical engine inputs

Create 4096 records in increasing number order, from 0 through 4095.
The collection is `Layer2Receipt`.
Key N is `generated`, one NUL byte, then `record-NNNNNN` with a six-digit number.

For body size S, concatenate SHA-256 digest bytes from these UTF-8 strings:

`Layer2-Oct16:N:chunk`

Start chunk at zero. Use enough 32-byte digests to reach S bytes, then truncate to S bytes.
Use an empty body when N modulo 997 is zero.
Use a tombstone when N modulo 251 is zero; this rule takes precedence over the body rule.
Encode live bodies as lowercase hexadecimal text in the JSONL input.
Use S values 128, 1024, and 16384.

Each JSONL record has this shape:

```json
{"collection":"Layer2Receipt","key":"generated\u0000record-000001","body_hex":"<generated lowercase hex>"}
```

A tombstone replaces `body_hex` with `"deleted":true`.
The saved `initial_sha256` identifies the complete generated input file.
Both programs receive that same input through independent new homes.

```bash
"$engine" build --home "$new_home" --input "$initial_jsonl" --fanout 1
```

The main matrix uses 31 rounds per program for 1024-byte bodies.
The 128-byte and 16384-byte matrices use 11 rounds.
Each read pattern uses one fresh home per program.
Each write shape and batch size also uses its own fresh home per program.
Baseline runs first on even-numbered rounds; candidate runs first on odd-numbered rounds.
Round numbering starts at zero.

## Physical reads and ranges

Use batch sizes 1, 16, 64, and 256.
For slot i in a call, use these record numbers:

| Shape | Record number |
|---|---|
| Contiguous | 1024 + i |
| Scattered | (73 + 37 × i) modulo 4096 |
| Duplicate and absent | Repeat 1111, 1112, 1111, 999999 |

The read request contains each coordinate in that order:

```json
{"coordinates":[{"collection":"Layer2Receipt","key":"generated\u0000record-001024"}]}
```

```bash
/usr/bin/time -l "$engine" read --home "$home" --input "$read_request" \
  >"$read_reply" 2>"$process_resource_receipt"
```

The bounded range request uses the same four page sizes:

```json
{"collection":"Layer2Receipt","prefix":"generated\u0000","start":"record-001024","end":"record-002048","page_size":256,"max_pages":1}
```

```bash
/usr/bin/time -l "$engine" range --home "$home" --input "$range_request" \
  >"$range_reply" 2>"$process_resource_receipt"
```

Each call must return the expected bytes, absent positions, and range order.
The saved receipts replace full returned bodies with a SHA-256 digest after this check.
The range returns live records; explicit-key reads retain absent slots.

## Physical writes and size receipts

For each round r and operation slot i, generate S body bytes from SHA-256 digest bytes of:

`Layer2-final:r:i:chunk`

Use the same digest concatenation and truncation rule as the initial bodies.
Distinct writes use record number 2048 + i.
Repeated writes use record number 2048 for every slot.
Write every supplied operation, in order, including all repeated-key operations.

```bash
/usr/bin/time -l "$engine" write --home "$home" --input "$ordered_updates_jsonl" \
  >"$write_reply" 2>"$process_resource_receipt"
```

Each update has `collection`, `key`, and `body_hex`.
The product reply must count the complete batch and advance its durable batch by one.
After the final round, reopen each home and read all 4096 known keys.
For 16384-byte bodies, use four 1024-key verification calls within the normal read budget.
Run independent baseline and candidate verification calls in parallel, after the timed calls.

Record each new `locator` and `directory` part path before and after a write.
Sum new part lengths and divide by 16384 for new map pages.
Do not include journal payloads, generation receipts, or markers in that page count.
Record file count, logical file bytes, and allocated file bytes before and after each write case.
Keep the inventory within the exact owned home, depth six, and 262144 files.

The focused follow-up uses a fresh pair of 1024-byte homes.
It runs only distinct batches of 16 writes, with 101 alternating rounds per program.
Its final full-key verification uses the same rule.

## Node setup and requests

Use separate generated identities and homes. Disable cloud configuration.
Start the recorded node with short absolute socket paths:

```bash
env -u LASTDB_DISK_CAP_BYTES -u SENTRY_DSN -u FOLD_SENTRY_DSN \
  LASTDB_ISOLATED_COPY=1 LASTDB_SUPPRESS_RECOVERY_PHRASE=1 \
  LASTDB_DISK_PACK=1 LASTDB_RESIDENT_MODE=off \
  LASTDB_HOME="$generated_home" FOLDDB_HOME="$generated_home" \
  RUST_MIN_STACK=16777216 \
  "$node" --data-dir "$generated_home" --socket-path "$query_socket" \
  --full-socket-path "$full_socket" >"$node_log" 2>&1 </dev/null &
```

Reserve additional path space for the node's auxiliary socket suffixes.
Wait for `GET /health` before each product request sequence.
All requests use `Content-Type: application/json` and `X-LastDB-Client: layer2-final-benchmark`.

Seed this schema through `POST /api/schemas/seed-system`:

```json
{"isolated_copy":true,"schema":{"source":"system_seed","name":"Layer2FinalBenchmark","descriptive_name":"Layer 2 Final Benchmark","schema_type":"HashRange","key":{"hash_field":"bucket","range_field":"id"},"fields":["bucket","id","title","body"],"field_descriptions":{"bucket":"generated record hash","id":"generated record range","title":"generated title","body":"generated body"}}}
```

Create 320 records through five batches of 64, with `POST /api/mutations/batch`.
Use normal async convergence and per-mutation `durability:"durable"`.
The preserved old seed uses this normal async setup.
The first final off-mode preparation requests synchronous convergence; the report excludes its optional wait cost.
No setup duration contributes to the timed comparison.

Record N uses bucket `generated`, id `record-NNNNNN`, and title `title-N-revision-V`.
For the initial records, V is zero.
The body contains 1024 characters from concatenated lowercase SHA-256 hex digests of:

`Layer2Final:N:V:chunk`

Start chunk at zero and truncate to 1024 characters.
These hexadecimal text bodies can compress. They differ from the physical engine's digest bytes.

The create payload includes all four fields:

```json
{"mutations":[{"type":"mutation","schema":"Layer2FinalBenchmark","mutation_type":"create","key_value":{"hash":"generated","range":"record-000001"},"fields_and_values":{"bucket":"generated","id":"record-000001","title":"title-1-revision-0","body":"<generated text>"},"durability":"durable"}]}
```

## Node read sequence

After a normal process start, send the exact one-key query before any other user query.
Then verify all 320 explicit keys in one call.
Keep that same process for the ordered read matrix.
The operating system cache state remains unknown, including for the first query.

Each exact case sends one `POST /api/query` with 1, 16, 64, or 256 explicit pairs:

```json
{"schema_name":"Layer2FinalBenchmark","fields":["title","body"],"filter":{"HashRangeKeys":[["generated","record-000032"],["generated","record-000033"]]},"limit":2}
```

For count C, use record numbers 32 through 31 + C and set `limit` to C.
This is one explicit-key query, not C queries in `/api/queries/batch`.
The query processor also returns the required hash field `bucket`.

The separate range cases use:

```json
{"schema_name":"Layer2FinalBenchmark","fields":["title","body"],"filter":{"HashRangeRange":{"hash":"generated","start":"record-000032","end":"record-000288"}},"limit":256}
```

Use an exclusive end of record number 32 + C for each range size.
The final absent case uses `HashRangeKey` with range `absent-generated` and limit 1.
Each case has one first call, one prime, then 21 steady calls.
For the paired old run, program order alternates on the steady rounds.
The candidate-only final run retains the same case order and count.

Send header `x-lastdb-read-receipt: true` for the read receipts.
Check logical values, result count, range order, required fields, and complete response equality against the prime.
An unavailable old receipt counter remains null, never zero.
The final candidate requires all file-work counters before a zero-I/O claim.

## Node writes, ack, and flush

Use update batches 1, 16, 64, and 256 on record numbers 32 through 31 + C.
Send three rounds for counts 1 and 16, then one round for counts 64 and 256.
For durable round r, use V = 1 + C × 1000 + r.
Each update changes only `title` and `body`.
Omit convergence for the normal async contract; request `durability:"durable"` per mutation.

For queued rounds, use V = 1000000 + C × 1000 + r and `durability:"queued"`.
After each queued ack, call `POST /api/db/flush` with `{}`.
Retain the ack time, forced-flush time, and complete request-start-to-durable-response time.
Do not treat a queued ack as a durable completion.
For candidate counts 64 and 256, read native status before and after the timed update, outside its timer.
Those status calls do not request a flush.

For the normal flush observation, start from one explicit flush.
Use queued counts 1 and 256, with three rounds per size.
Use V = 8000000 + C × 1000 + r.
Poll `GET /api/db/native-status` at a 20 ms interval, with no forced flush during the sample.
Retain every poll and the durable completion bounds after the ack.
Completion requires no accepted batch, no failed writer, and a later durable frontier.
The policy delays the flush for up to 500 ms of idle time, then adds the actual disk work.

For the visibility check, prime key 32, then update it with V = 9000000.
Read it immediately, repeat the read, flush when queued, and read after a normal reopen.
Check every returned title and body against that new revision.

Use resident mode `write` for the separate intended memory-first profile.
Keep durable-request results separate from queued ack and normal automatic flush results.
Use an additional fresh generated candidate for the separate 8589934592-byte cap measurement.
Record that profile explicitly; do not mix it with the unset-cap comparison.

## Current-engine maintenance

Use the final 1024-byte candidate home after its 31 distinct 256-write rounds.
The home contains only generated physical records and has no active node.

```bash
"$maintain" records --home "$home"
"$maintain" reference --home "$home" --purpose backup
"$maintain" compact --home "$home" --grace-ms 0
"$maintain" reclaim --home "$home" --grace-ms 0
"$maintain" release --home "$home" --reference "$returned_reference"
"$maintain" reclaim --home "$home" --grace-ms 0
"$maintain" records --home "$home"
```

Record command time, file counts, logical bytes, and allocated bytes after each stage.
Record separate totals for packs, maps, journals, and other top-level file kinds.
Compare the ordered record digest before and after the commands.
This is a current-engine maintenance measurement, not a false old-to-new compaction comparison.

## Clocks, summaries, and retained limits

Physical medians use `elapsed_nanos` from the product reply.
Node times start after client JSON serialization and end after response JSON decode.
The node times include request transport, server work, response transport, and client decode.
Store each individual result before the summary.
Use the nearest-rank p95, at sorted position ceiling(0.95 × N).
Do not supply a p95 when fewer than five node samples exist.
Compute rate as supplied operation count divided by the median time.

`/usr/bin/time -l` supplies whole-command process memory, including final output work.
Product temporary-buffer receipts have a different scope.
File bytes and map pages do not establish physical-device write bytes.
No cache-drop command, power-loss claim, cloud-recovery claim, or primary-upgrade claim forms part of this recipe.
