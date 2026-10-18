# Selected tips in the core warm set

## Design choice

The design describes four states: unknown, absent, a current tip, and a current tip with its value.
Before this change, the shared core retains complete values and confirmed absence.
A selected tip without a value lasts only for its call.
That difference causes repeated count calls to fetch tips from disk again.

The core now retains selected tips under the same logical-record limit and purge rule.
A value fetch adds the atom to that same record.
A later count preserves the full value when its tip is unchanged.
Accepted writes retain complete values and their memory charges.
There is no second tips cache, independent value purge, or new write buffer.

The selected physical copy, key metadata, and source controls remain part of each record's authority.
A source change invalidates both the tip and its value.
An unknown value never means an absent record.
Complete preloaded range keys establish candidate keys; they do not establish that every tip or value is present.

## Matched synthetic operations

Both releases use fresh generated data through the ordinary socket API.
The schema contains 512 records under one hash, with 1 KiB title and body values.
Writes use batches of 64 mutations.
The node flushes and stops, then reopens with an empty warm set.
Three identical count-and-page calls request one title and the total count of 512.
Two exact calls then request both values at key 511.

| Operation | Before: file reads | After: file reads | Before: elapsed ms | After: elapsed ms |
|---|---:|---:|---:|---:|
| First count and page | 232 | 235 | 34.150 | 38.525 |
| Second count and page | 210 | 215 | 75.568 | 87.495 |
| Third count and page | 210 | 0 | 77.455 | 24.249 |
| First exact value read | 21 | 18 | 1.426 | 1.384 |
| Repeated exact value read | 0 | 0 | 0.333 | 0.296 |

All returned values and counts match the generated inputs.
The third count changes from 1,019,811 file payload bytes to zero.
The first two calls still pay the initial key and tip fetch costs.
Their observed times increase; this change does not establish an initial-read speed gain.

After the third count, the core reports 510 tip-only paths and three full atoms.
The first exact value call changes those counts to 509 tip-only paths and five full atoms.
The repeated exact value call performs zero file reads.
This confirms that the same record gains its value and supports later full reads.

These are individual operational observations, not a repeated statistical benchmark.
The operating system file cache is uncontrolled.
The measurements do not establish general throughput or a whole-process memory bound.

## Additional accepted operations

The candidate also completes these ordinary operations on fresh generated data:

- A count after a full value read, then a full value read with zero file reads.
- A two-key delete, count 510, and an absent result for the deleted key.
- A two-key recreate, count 512, and the expected revised values.
- A reopen with a 128-record warm-set cap and two correct counts of 512 through the disk fallback.
- An exact read under that cap, then a repeated exact read with zero file reads.
- A separate fresh run with partition-prefixed atom files: correct counts and values, then zero file reads for the warmed count and repeated exact read.

The prefixed run reports file-read counts of 221, 200, 0, 19, and 0.
Its startup log confirms the selected file format.
Those times are not a matched comparison with the default-format baseline.

## Source and checks

The accepted product source is commit `b174cb56637362dcc012d6c4af4e7230833617ce`.
Its release binary SHA-256 is `0c4598fa3ddf54bf6e29f5447dbbe46c0d1c29f47ee760170e332bc9d8d30142`.
The source inventory records 1,883 inputs before the build.
Each input matches its committed Git blob, and the post-build source check passes.
The release uses the normal default features.

The full ordinary workspace Clippy lane passes with warnings denied.
The off-lane Clippy check also passes on the final source.
The release build, format check, source size checks, new-allow check, and local boundary lints pass.
An independent source review finds no actionable concern with promotion, invalidation, physical copies, or retained write memory owners.
No tests or coverage commands run.

The existing frozen logical and protein API workflows also pass on the final release.
They complete 100 keyed value checks and 48 required zero-file-read checks across eight normal node sessions.
The memory ack check observes accepted writes before disk completion, with their complete values in the core.
The root review verifies all 102 sealed files and checks the raw value and zero-file-read receipts independently.
Exact temporary-home cleanup records accompany those receipts.

## Retained diagnostic attempts

Earlier candidate receipts remain separate from the accepted run.
They expose a value-promotion defect: a direct core tip selection lacks a call-local disk receipt.
The final fix uses the already authorized tip under the same protected read boundary and checks its atom identity.
It then links the actual fetched value source and preserves the tip's source controls.
The accepted repeated exact reads require zero file reads; that requirement remains unchanged.

The initial baseline driver also rejected an automatically returned hash field.
The corrected check validates all requested values and permits that existing response field.
The rejected driver run remains a diagnostic receipt, not a product failure.
The candidate preparation check retries flush after a later private housekeeping write; it does not bypass an incomplete flush.

## Scope

The work changes memory retention and its disk adapter.
The primary database and its archive remain untouched.
Cloud activation and format migration remain outside this change.
The 10,000-record warm-set cap is unchanged.
Counts that exceed the available record capacity can still require disk tip reads.

All owned synthetic database homes are removed through the supported exact-home cleanup command.
The cleanup check refuses the source-only temporary directory because it lacks a database owner marker.
No bypass runs; its 12,184-byte source remains at `/private/var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T/wt.xyJASG/operations.py`.
The same source is preserved in `operations-source.txt`, with its cleanup receipt in `driver-cleanup-preflight.json`.
