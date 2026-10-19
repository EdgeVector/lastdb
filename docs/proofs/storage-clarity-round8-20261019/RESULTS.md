# Memory and disk clarity review — round 8

Normal reads tried old molecule names, and schema lists bypassed the core read rules. Reads now use one exact identity. Lists use captured core fields. The changes keep one logical value model and one private disk queue.

## Changes

- Exact HashRange batches check each record against its own hash before the result window. Counts and single reads share that rule.
- Time conditions retain integer milliseconds and negative fractional seconds.
- Valid empty hashes keep their complete preloaded key set during initial load and later writes.
- Schema key lists use one captured read, molecule-specific encryption keys, bounded pages, and batch value reads. Membership precedes the result limit.
- Empty and whitespace list hashes retain their exact values. The list command also preserves them in requests and continuation commands.
- An unfiltered blinded Hash list refuses an unrecoverable hash. A filtered Hash list uses the supplied hash. HashRange lists exclude foreign-layout candidates.
- Value reads, counts, and schema key lists share one resolved-schema state check. A blocked schema receives the same refusal.
- Startup namespace errors propagate. Encryption checks refuse unreadable data without deleting shared records.
- The old backend-specific skip and plaintext-duplicate cleanup are removed from normal reads and startup.
- Normal reads and Delete use one exact molecule UUID. Old UUID-spelling probes and their duplicate complete-key domains are removed.

## Conceptual result

The core model owns logical values. Schema key lists now use its existing field lookup and admission rules. Each list holds the selected schema, tips, and atom values under one read boundary.

The private disk queue owns prepared flush work. Complete preloaded keys establish range completeness. These roles remain separate from the partial set of values in memory.

Explicit physical maintenance retains its own scope. It does not supply an alternate logical value model. An unfiltered schema list still fetches selected bodies to recover API hashes; these changes do not promise a disk-free list.

## Source and build checks

Independent source reviews found no remaining blocker in the changed query, memory, list, boot, or preload routes. The first compiler pass caught a schema ownership error. The correction and all earlier logs remain preserved.

The final source includes accepted main `adbec20cdbf39b3fc397be5f34c3cec5e0e280cc`, which adds the warm-set byte gauge. The integration has no conflicting files. The gauge reports charges without a new enforced memory cap.

Final source `78f2c6a67523f34c9c2494e8c42ac638f7e37c0c` passes workspace and separate-lane compiler checks, the normal release build, format, size, and added-allow checks. Its 1,882 product inputs match the frozen inventory and Git. The release SHA256 is `bf9c5cb7451c3d862d38e6b3ecd30cb50717634108e5f4fcb7d008dd075ea6e1`. The earlier source identities and compiler/release attempts remain preserved.

## Operational checks

The V5 diagnostic baseline and both stopped candidates remain preserved. The first candidate exposes duplicate key sets from the legacy UUID path. The next candidate passes all five empty-hash key-set observations, then stops at the incorrect `data.list` expectation. Both programs use the existing top-level `list` response.

V6 corrects that response path and two dependent cursor paths. Its old-program run exposes another comparison error for omitted empty fields. V7 matches the existing serializer: empty ranges and absent optional fields stay absent on the wire. Whole key objects still require exact equality. All logical key values, query conditions, and normal API requests stay unchanged.

The accepted V7 comparison uses the same inputs for both programs on fresh synthetic homes:

| Program | Observations | Pass | Fail | Operational errors |
|---|---:|---:|---:|---:|
| Accepted old executable | 181 | 107 | 74 | 0 |
| Updated executable | 181 | 181 | 0 | 0 |

The updated program passes cold and warm queries, mapped exact and batch reads, counts, time conditions, typed keys, and list continuation. The empty-hash key set remains one ready domain, with keys from 0 to 2 and no second load.

The queued write appears in both the key list and exact read while native work remains pending. Native accepted/completed sequences remain 4/2 after the ack, list, and exact read. The public ack does not map that record to one sequence, so this proves no individual flush time.

The unchanged broader cohort also passes: 74 read checks and 29 strict checks with no file-read calls. No operational error occurs. It uses fresh owned synthetic homes and unchanged existing helpers. No test suite or new executable exercise helper forms part of this review.

The old executable remains tied to source `122072ed51e20a1f67ad4915c0efcb9a2ffc1551`. Its 1,877 product inputs match accepted main `ea01331f7aa318c8748a6f41fa95e0b4eddf3b3a`. The old and updated programs receive the same saved requests and literal expected results.

All 21 owned node sessions stop normally. Supported exact cleanup removes all seven owned temporary roots after independent review. The 56 original receipt files and all original seal hashes remain unchanged.

## Limits

Boot refusal, corrupt-data branches, the CLI request builder, and the blocked-schema gate receive source and compiler checks only. No corrupt-home or wrong-key startup operation forms part of this review. The saved native sequence span establishes pending aggregate work during the successful reads; it does not identify an individual record's flush time.

No benchmark, throughput, whole-process memory-cap, or complete key-rotation claim follows from these correctness changes. The primary and archive remain untouched. No migration, cloud activation, installation, or upgrade forms part of this review.
