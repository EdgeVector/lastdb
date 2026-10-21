# Durable local write ack

Date: 2026-10-21. Source: `a583590a8ab49b2ae1f3d9f8f4946d167d15f807`.
Binary SHA-256: `e1460cae8b54f1bd3bfe1c6e40f18bb679b8e15c9bec72de38524887fc00d112`.

Every successful local write waits for complete disk publication. The 500 ms queue delay and periodic flush task are removed.
The old flush environment controls cannot select an earlier ack.
The native journal retains its complete-batch durability barriers and recovery rules.
Each API batch uses one complete native request. No second finalize flush follows its disk receipt.
Cloud delivery remains separate from local disk durability.

## Product result

The daemon used a fresh generated home and the owner Unix socket.
The local system-seed route supplied one HashRange schema. It made no shared schema-service change.
No primary data or archive entered this check. No explicit flush request ran.

- Create and Update used the legacy `durability: "queued"` input. Both returned `durability: "durable"`.
- Immediately after the Update response, SIGKILL stopped the owned daemon. The next process returned the updated value, `two`.
- A three-record Create batch returned `durability: "durable"`. SIGKILL followed the response. The next process returned all three records.
- Delete returned `durability: "durable"`. SIGKILL followed the response. The next process returned the three retained records and omitted the deleted record.
- The final native status reported no pending batch, no dirty core records, and no writer failure.
- The final daemon stopped through SIGTERM and returned exit code 0.

All successful operations used `LASTDB_MUTATION_SYNC_FLUSH=0`, `LASTDB_BACKGROUND_FLUSH_MS=0`, and `LASTDB_RESIDENT_PERSIST_MS=30000`.
Those retired controls did not weaken durability or introduce a 30-second wait.

## Latency limits

These are individual unoptimized debug operations, not a throughput benchmark.
Create took 1.022 seconds; Update took 1.019 seconds.
The three-record batch took 1.379 seconds; Delete took 1.372 seconds.
The removed timer adds no delay. Actual disk publication still contributes to each ack.
These results do not establish a low-latency target for a release build.

The first debug mutation exceeded the default 8 MiB worker stack before it returned an ack.
The later operations used two workers and a 64 MiB stack per worker. No shared process setting changed.
The recovery check covers process loss. It does not simulate power loss or a failed device.
The existing LastStore barriers use F_FULLFSYNC on macOS and propagate disk errors.

## Source checks

The following checks passed:

- Full workspace Clippy for libraries and binaries, with warnings as errors.
- Product daemon build.
- Workspace format check and diff whitespace check.
- Changed-file size, function size, and new-allow checks.
- Write-path store, atom-counter writer, hardcoded-URL, redaction, and UTF-8 byte-slice checks.

No test suite or fixture harness ran or entered the change.
The primary requires a separate approved upgrade before it uses this code.

The owned temporary home and request files are removed. The product receipts remain in `evidence.json`.
