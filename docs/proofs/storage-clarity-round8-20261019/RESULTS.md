# Memory and disk clarity review — round 8

This review found inconsistent key selection and an old destructive startup rule. The changes keep one logical value model and one private disk queue.

## Changes

- Exact HashRange batches check each record against its own hash before the result window. Counts and single reads share that rule.
- Time conditions retain integer milliseconds and negative fractional seconds.
- Valid empty hashes keep their complete preloaded key set during initial load and later writes.
- Schema key lists use one captured read, molecule-specific encryption keys, bounded pages, and batch value reads. Membership precedes the result limit.
- Empty and whitespace list hashes retain their exact values. The list command also preserves them in requests and continuation commands.
- Unrecoverable blinded schema keys receive an explicit refusal. A filtered Hash list uses the supplied hash.
- Value reads, counts, and schema key lists share one resolved-schema state check. A blocked schema receives the same refusal.
- Startup namespace errors propagate. Encryption checks refuse unreadable data without deleting shared records.
- The old backend-specific skip and plaintext-duplicate cleanup are removed from normal reads and startup.

## Conceptual result

The core model owns logical values. Schema key lists now use its existing field lookup and admission rules. Each list holds the selected schema, tips, and atom values under one read boundary.

The private disk queue owns prepared flush work. Complete preloaded keys establish range completeness. These roles remain separate from the partial set of values in memory.

Explicit physical maintenance retains its own scope. It does not supply an alternate logical value model. An unfiltered schema list still fetches selected bodies to recover API hashes; these changes do not promise a disk-free list.

## Source and build checks

Independent source reviews found no remaining blocker in the changed query, memory, list, boot, or preload routes. The first compiler pass caught a schema ownership error. The correction and all earlier logs remain preserved.

The final source includes accepted main `adbec20cdbf39b3fc397be5f34c3cec5e0e280cc`, which adds the warm-set byte gauge. The integration has no conflicting files. The gauge reports charges without a new enforced memory cap.

The integrated workspace compiler and lint checks, separate compiler lane, normal release build, format check, size checks, and added-allow check pass. Final source `1b1b3205cfd894f9bb8583ecdf0ef55f2bf839b5` has 1,882 verified product inputs. The release SHA256 is `1f6444fc47590cabd0561f8fbb8e5bc1d326a235be25587e21114bede2f4143a`.

## Operational checks

The frozen V5 API procedure is in progress. It uses fresh owned synthetic homes and unchanged existing helpers. No test suite or new executable exercise helper forms part of this review.

The old executable remains tied to source `122072ed51e20a1f67ad4915c0efcb9a2ffc1551`. Its 1,877 product inputs match accepted main `ea01331f7aa318c8748a6f41fa95e0b4eddf3b3a`. The old and updated programs receive the same saved requests and literal expected results.

## Limits

Boot refusal, corrupt-data branches, the CLI request builder, and the blocked-schema gate receive source and compiler checks only. No corrupt home or alternate identity is created. A queued-write observation proves a read before flush only when its saved native sequence span shows that fact.

No benchmark, throughput, whole-process memory-cap, or complete key-rotation claim follows from these correctness changes. The primary and archive remain untouched. No migration, cloud activation, installation, or upgrade forms part of this review.
