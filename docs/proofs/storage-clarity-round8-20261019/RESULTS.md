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

Final source `d64d918ff0e757f34863277a69dc9f07120aabbb` passes workspace and separate-lane compiler checks, the normal release build, format, size, and added-allow checks. Its 1,882 product inputs match the frozen inventory and Git. The release SHA256 is `0f020fd6653e581ac0e808ae3a4c6ee683369e749b32e04898dc99abb29b64ec`. The earlier source identities and compiler/release attempts remain preserved.

## Operational checks

The V5 diagnostic baseline and both stopped candidates remain preserved. The first candidate exposes duplicate key sets from the legacy UUID path. The next candidate passes all five empty-hash key-set observations, then stops at the incorrect `data.list` expectation. Both programs use the existing top-level `list` response.

V6 corrects only that response path and two dependent cursor paths. Every request and expected key, value, type, count, condition, status, and page flag stays unchanged. V6 repeats both programs with fresh synthetic data before the final comparison. Its results remain in progress. It uses fresh owned synthetic homes and unchanged existing helpers. No test suite or new executable exercise helper forms part of this review.

The old executable remains tied to source `122072ed51e20a1f67ad4915c0efcb9a2ffc1551`. Its 1,877 product inputs match accepted main `ea01331f7aa318c8748a6f41fa95e0b4eddf3b3a`. The old and updated programs receive the same saved requests and literal expected results.

## Limits

Boot refusal, corrupt-data branches, the CLI request builder, and the blocked-schema gate receive source and compiler checks only. No corrupt home or alternate identity is created. A queued-write observation proves a read before flush only when its saved native sequence span shows that fact.

No benchmark, throughput, whole-process memory-cap, or complete key-rotation claim follows from these correctness changes. The primary and archive remain untouched. No migration, cloud activation, installation, or upgrade forms part of this review.
