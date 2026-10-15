# Generated native-node receipt, October 15, 2026

The native backend serves the LastDB product routes on fresh generated homes.
Exact keys, batches, ranges, updates, deletes, normal restarts, and explicit resource refusals return the expected results.
This receipt completes one gated integration unit. It does not complete conversion, backups, rollback, or the primary upgrade.

## Source and limits

- Final source: `65d327d463d176905002e8e038e5342e5b4008e1`.
- Final debug daemon SHA-256: `c7103e15a3f66996a9e5010d119017ef2b16e55867da0d05cbb5e6ac76b04924`.
- Main route, restart, resource, and comparison receipts use the earlier build SHA-256 `9884f0adba1f2043a00df1efa320628473d0e41469a925426b123cd90a8b0a27`.
- The final source adds a fresh write-root capture after the exclusive gate. The final daemon also completes an update and 64 concurrent product reads.
- Each home starts empty and creates its own identity. No primary data, primary copy, archive, factory, or shared service participates.
- The nodes use the isolated local schema setup route. Cloud sync stays off.
- Native selection is explicit: `LASTDB_DISK_PACK=1`, with `LASTDB_RESIDENT_MODE=off`.
- The writer waits for the complete native directory. Memory-first acknowledgements and cloud-connected native homes remain refused.

The generated schema is `ReadReceiptProof`, with hash `bucket`, range `id`, and fields `bucket`, `id`, `title`, and `body`.
One product mutation batch creates eight explicit keys, `generated / record-0` through `generated / record-7`.
The schema and mutation payloads follow the commands in [the read receipt guide](../../lastdb-read-receipts.md#direct-commands-on-generated-data).
There is no test suite, fixture harness, failure injection, or primary operation.

## Product results

| Product call | Observed result |
|---|---|
| Exact `record-0` | `title-0`, `body-0` |
| Exact batch `record-1`, `record-4`, `absent` | Two correct records; the absent key adds no result |
| Range `[record-2, record-6)`, limit 2 | `record-2`, `record-3`, `has_more=true`, next cursor `record-3` |
| Four query items | Statuses 200, 400, 200, 200; the unknown schema retains its own error |
| Update `record-0`, delete `record-7` | Updated fields for zero; no record for seven |
| Normal close and reopen | The update, delete, and bounded range remain correct |
| 64 large query items | 35 successful items and 29 explicit 503 `read_resource_limit` items |
| A second distinct batch | The same 35/29 result; cache count 10,000, over-cap count zero, 6,891 purged entries |

The successful resource-batch items retain the requested existing record. A resource error does not become an empty success.
The cap receipt covers the native fetched-record cache, including addressed absences and internal metadata.
It does not establish the complete logical-path admission rule or a process memory-size cap.

The final daemon accepts another update to `final-title-0` and `final-body-0`.
During that request, all 64 concurrent reads return the prior complete values.
Those reads finish in 1.897171 seconds; the mutation reply follows in 4.451261 seconds.
The next exact read returns the final values. This overlap does not prove a read across the publication instant.

## Read work and time

All corrected exact, batch, and range receipts report zero legacy replay, journal replay, group opens, loader calls, footer bytes, and directory walks.
The durable CURRENT digest stays unchanged across the first fresh-node exact read and the post-mutation query batch.
See [the root digests](root-checks.json), [batch results](batch.json), and [restart results](reopen-after-change.json).

The same debug daemon also runs a separate fresh legacy home with the same eight generated records.
These are individual calls after creation; they are not cold-device measurements or a distribution of elapsed times.

| Exact call after creation | Native | Legacy |
|---|---:|---:|
| Query time | 43.219041 ms | 3.011625 ms |
| File opens | 24 | 3 |
| Format bytes | 278,656 | 16 |
| Payload bytes | 15,253 | 173 |
| Legacy replay bytes | 0 | 173 |
| Group opens | 0 | 1 |
| Repeat query time | 34.453792 ms | 2.679667 ms |

The native path is slower for this small debug case. It does not establish the speed completion gate.
Native creation of eight records takes 5.224189 seconds; the legacy request takes 0.336851 seconds.
The native writer still retains all parts and checks a conservative dependency inventory at each publication.
Release-mode measurements, more generated data, writer cost, and complete memory admission remain required.
The host and OS cache also affect these times; receipts count userspace reads, not physical-device traffic.

## Problems found and corrected

Two initial starts refuse existing metadata range boundaries. The adapter now accepts the existing four-byte end marker and empty administrative start.
The same generated home then opens without a reset.
Initial queries also create empty namespaces inside the held read boundary.
The retained `before-namespace-fix-*.headers` show thousands of opens and journal work from that defect.
Corrected query scopes leave absent namespaces read-only. A first mutation registers a namespace in its own complete batch.
The fresh-node CURRENT comparison and corrected receipts confirm the result.

The older maintenance command from PR #196 exits 2 with `refusing: packaging is not plain` on the stopped native store.
CURRENT and the refusal descriptor retain their digests, and no legacy data directory appears.
This checks that command's refusal. It does not establish a rollback through every older Mini binary.

## Source checks and remaining work

The daemon build, workspace library/binary Clippy, format, diff, source-size, function-size, new-allow, redaction, byte-slice, and storage-write lints pass.
No test command runs. Source checks do not prove recovery from an interrupted publication.

Memory-first admission, complete logical-path protection, encrypted-frame conversion, concurrent conversion, retained references, backup restore, reverse conversion, and final performance remain open.
The default new-node backend remains unchanged. This explicit development path grants no primary upgrade authority.
