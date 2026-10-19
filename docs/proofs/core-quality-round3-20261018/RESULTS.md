# Current field reads and complete shared keys

## Review changes

| Finding | Change |
|---|---|
| A fixed-field read restores each molecule, then reads its tip again. | Select each current tip once, then fetch the atom bodies in one batch. |
| Schema metadata acts as a second fallback for a current tip. | Remove that fallback. Schema metadata supplies definitions; the captured native reader supplies current tips. |
| Optional source receipts copy an atom before source and capacity checks. | Check both controls under the existing lock before the value copy. |
| An invalid peer-key value enters the Delete storage retry path. | Use a typed field refusal. The request ends before any deletion; transient storage errors retain their retry behavior. |
| A shared key changes from null to usable, but unchanged fields remain absent under that key. | Prepare those fields from their existing atoms under the accepted key-change trigger. Old-key removal still requires a confirmed old association. |

The final change adds no value store, write queue, or retained lookup map.
The fixed-field reader retains the existing missing-data checks, selected-tip clocks, physical hints, and storage scope.
The receipt allowance remains 10,000 entries and 4 MiB per call.
Its refusal no longer creates a discarded copy of the atom body.

Two Delete peer-resolution steps remain intentionally.
The second step also serves scoped HashRange keys and peers that require fields from the first related schema.
Purge uses that step separately. This review does not change those cases.
The captured dirty-atom fallback also remains: accepted values can precede their disk root.
Ordinary warm logical reads bypass that fallback.

## Source and product evidence

The reference is merged PR #217, source `8c0531e6ca032bbc1e5b11379792c3a120f170a6`.
Its accepted release has SHA-256 `1b79500275f732f15b6405bd0d46d63fe853362790dcd76934cead37651b1c6c`.
The reference reuses that release after all 1,878 product inputs match the merged source.
It is not the retired disk engine.

The final product source is `8c2b0eb89bc140cd453fb80ee5637b378c0513cb`.
Its release has SHA-256 `264ed137a697526f8f779bca3e70a98ed7b75c614a6a71785486efec256aa003`.
The final source manifest and Git verification identify its release and all 1,878 product inputs.
Later report-only commits do not change those product bytes.
The independent source review covers tip selection, field bodies, error types, clocks, scopes, destination conflicts, and old-key removal.
The format, workspace Clippy, separate embedding-package Clippy, file/function size, new-allow, whitespace, write-route, and registry checks pass.
No new lint exceptions form part of the change.

The ordinary product checks use fresh synthetic homes and the normal native startup.
They check shared atoms, current conditions, successful and refused Deletes, unrelated records, and normal reopen.
The null-to-usable key check requires every unchanged field at the new key before and after reopen.
Its original failed result remains separate from the corrected release.
The final matrix passes all 696 read checks and 112 timed write batches.
The final shared-key and conditional Delete case passes 107 query items and both expected refusals.
The final nullable-key case passes 18 query items, the final refusal, the next valid write, and both reopens.
The aggregate case passes 35 query items: totals remain correct at 3, 8, 12, and 7, including repair, finalize, and reopen.
Its member winner identities match the current source mutations. No replay or optional repeated-payload claim forms part of that check.
The memory review also confirms that unchanged field bodies enter the existing complete write reservation quote.
On reservation refusal, the write finishes on disk and skips positive core entries that lack an owner.

## Measurement method

The paired matrix uses the unchanged existing benchmark helpers.
Each case starts with 512 synthetic records across Hash and HashRange schemas.
Each body has 1,024 bytes. Timed writes contain 64 records.
Selected-schema preload runs both on and off.
The second pair reverses the order of the reference and candidate.
All individual latencies, request identities, file costs, and write-state observations remain available.

The first read follows a normal process reopen. It does not clear the operating-system file cache.
Each first-read comparison has two observations; each repeated-read comparison has ten.
The write result distinguishes the ack from the later explicit flush.
The duration through flush includes a status request between the ack and flush.
A queued ack alone does not prove memory-only preparation.

These measurements use an Apple M4 Max with 36 GiB of memory.
The dataset fits the 10,000-record warm limit.
It does not measure a 16–20 GiB database, maximum throughput, memory pressure, or cloud upload.
The host lock prevents concurrent agent builds and product runs. It does not reserve the machine.

## Final measurements

All values below are median milliseconds. The warm read and warm update groups each contain ten observations across two runs.
Preload refers to the selected HashRange schema. It does not load keys for the Hash schema.

| Operation | Preload | Reference | Final | Change |
|---|---|---:|---:|---:|
| Warm read, 64 exact Hash keys | on | 4.974 | 5.191 | +4.4% |
| Warm read, 64 exact HashRange keys | on | 2.403 | 2.472 | +2.9% |
| Warm read, HashRange interval with 64 keys | on | 5.281 | 5.258 | -0.4% |
| Warm update, 64 Hash records, ack | on | 88.504 | 91.086 | +2.9% |
| Warm update, 64 HashRange records, ack | on | 225.446 | 175.999 | -21.9% |
| Warm update, 64 HashRange records, through flush | on | 543.039 | 498.690 | -8.2% |
| Warm update, 64 Hash records, through flush | off | 383.885 | 466.224 | +21.4% |
| Warm read, HashRange interval with 16 keys | off | 1.846 | 2.094 | +13.4% |

The performance result is mixed. Warm reads remain near the reference in the selected-schema mode.
The HashRange update ack is shorter in this run, but some other modes take longer.
The preload-off Hash duration through flush ranges from 330.89–521.75 ms before and 343.39–543.38 ms after.
The ranges overlap. These observations do not establish a universal improvement or its cause.
The code changes improve clarity and correct two specific failures; the measurements do not show a new broad speedup.

The first 64-key interval read with preload takes 130.061 ms before and 126.201 ms after, including its one-time load.
Its repeated read takes 5.281 ms before and 5.258 ms after.
All 160 repeated reads with preload on report zero file-read calls, across both releases.
This result confirms the existing warm path; it is not a new improvement.
Without preload, the repeated interval reads still report 19–20 file-read calls for 16 keys and 27–30 for 64 keys.
These counts describe LastDB requests, not operating-system page faults.
Durable seed-create batches with preload on take 372.78→372.00 ms for Hash and 403.91→393.24 ms for HashRange.
Those durable create groups contain eight observations per binary; they are separate from the warm update groups.
All 112 timed write replies report queued durability. Each binary has 56 pending-after-ack observations and zero allowance refusals.

The [full comparison](benchmark-final/TABLES.md) keeps all 42 read comparisons and 12 write comparisons.
`benchmark-final/all-samples-compact.json` retains every measured API duration, read cost, write sample, round, and request identity.
The separate source labels identify the reference and the final release.
The final ordinary durable Delete takes 584.63 ms. That single observation is not a paired timing round or a throughput result.

## Preserved earlier candidate

Source `894c459d` and release `4e6a2aa9` pass the first eight-case matrix and both ordinary protein/conditional Delete cases.
The matrix contains 696 checked reads and 112 timed write batches.
Its tables remain in `benchmark/TABLES.md`, with the correct source labels.
The successful durable Delete observations are 473.92 ms for the reference and 515.30 ms for that candidate.
One pair does not establish a performance result.

The additional nullable-key procedure then stops with `complete=false`.
The invalid Delete receives its expected final refusal, and the next valid update succeeds.
The new peer returns the changed fields but omits the unchanged `id` field.
That actual product failure leads to the final key-introduction fix.
The earlier result and all original request and response bytes remain unchanged.

The final release receives a fresh matrix and fresh ordinary product checks.
This repeat follows a new product failure and source change; it does not replace an unfavorable timing result.
Compact reports retain all measurement samples.
Full repeated summaries and input copies remain local with immutable hashes.
The earlier committed copies have exact Git recovery references.
The final local summaries have a separate index with raw receipt hashes and helper reconstruction steps.

## Temporary data cleanup

All 25 owned temporary roots are removed: 22 synthetic database homes and three private executable roots.
The receipts record 268 normal node sessions across the initial and final procedures.
No owned node or host lock remains.
The independent root review confirms that all 392 sealed files remain unchanged after cleanup.
The exact paths, supported cleanup receipts, and final checks remain in `cleanup-verification.json` and `root-cleanup-verification.json`.

## Boundaries

The primary database and archive remain untouched.
No existing database home is loaded. No migration or cloud activation runs.
No tests, coverage commands, or new executable fixture suite run.
The existing per-field clock order and separate mixed update/Delete phases remain unchanged.
