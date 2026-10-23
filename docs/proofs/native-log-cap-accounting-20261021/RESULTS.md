The disk-cap review found a real race between a disk inventory and the release of a write reservation.
The inventory could omit a new file after its reservation left the ledger.
Commit `550836f1b` keeps those released bytes in the admission charge until that inventory ends.
The change stays unmerged. The primary and its archive stayed unchanged.

The code starts a per-root inventory record before the file walk or free-space check.
A reservation release adds its full charge to a counter while any inventory remains active.
Admission counts the current reservations, the new reservation, and all releases since that inventory started.
Thus, a new file has either an active reservation or a retained release charge.
The state remains present until the last inventory and reservation end. An overflow refuses admission.

No reservation mutex spans the file walk.
The existing write lock does not cover the compaction file walk.
The uncapped path still uses the filesystem free-space check without a file walk.
The change does not alter the disk format or the durable ack path.
Process-wide numeric counters appear under `native_disk.space_inventory`.

The charge is conservative. It can include files that the inventory already counted, or reservations for cancelled work.
Near the cap, this can cause a temporary refusal. A later call starts a fresh inventory.
This avoids an unbounded series of repeated file walks when writes continue.

Real daemon checks used two fresh generated homes, 64 schemas, two clients, and eight records per batch.
Each client used separate schemas. No two requests used the same schema at the same time.
The receipts retain every request and its durable ack, known-key reads, forced-stop recovery, and cap refusals.
No test suite, fixture, primary copy, or schedule injection ran.

| Run | Disk cap | Durable batch calls | Record mutations | Successful explicit compactions | Final records recovered |
|---|---:|---:|---:|---:|---:|
| `capped` | 2 GiB | 1,019 | 8,152 | 1 | 513 / 513 |
| `headroom` | 32 GiB | 256 | 2,048 | 3 | 513 / 513 |

In `headroom`, 245 durable batch calls completed during successful compact API calls.
The daemon reported four total maintenance runs, no maintenance failures, and a healthy writer.
All 256 batch calls returned a durable ack.
All known final values and the sentinel returned after a forced stop.
The next durable write advanced the author clock.

The first run reached its 2 GiB reservation cap.
One client write returned HTTP 507. A compact call also failed at the cap.
The original driver stopped after 1,020 attempts; its `failure.json` remains in the receipts.
Recovery used the same generated home with a 32 GiB cap and a higher automatic compaction threshold.
All acknowledged final values returned after restart and a subsequent forced stop.
`capped/resumption.json` records those recovery settings.

Both homes then restarted with a cap below the required write headroom.
Each refused a new write with HTTP 507. The refused key stayed absent, and all 513 known records remained correct.
Both writers stayed healthy. After the cap increased, each new write returned a durable ack.
The `headroom` home returned exit 1 during the constrained shutdown; that receipt remains visible.
Both final shutdowns returned exit 0 with `flush_ok: true` after capacity returned.

The exact inventory/release race did not recur in these workloads. Both overlap counters remained zero.
The live receipts show concurrent writes, compaction, cap refusal, and recovery; they do not reproduce that exact interleaving.
The source invariant above addresses that interleaving without a long mutex hold.
These capped batch workloads do not establish the small-write latency target.

The optimized daemon build, Clippy with warnings denied, and format check passed.
The file-size, function-size, new-allow, byte-slice, redaction, and write-path source checks passed.
`checks.json` records the checks. `matrix.json` records the workload results.
The inputs record the product commit and binary hash. The raw daemon logs remain with the receipts.
