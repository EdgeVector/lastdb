# Memory and disk clarity review — round 8

This review found inconsistent key selection and an old destructive startup rule. The changes keep one logical value model and one private disk queue.

## Changes

- Exact HashRange batches check each record against its own hash before the result window. Counts and single reads share that rule.
- Time conditions retain integer milliseconds and negative fractional seconds.
- Valid empty hashes keep their complete preloaded key set during initial load and later writes.
- Schema key lists use one captured read, molecule-specific encryption keys, bounded pages, and batch value reads. Membership precedes the result limit.
- Empty and whitespace list hashes retain their exact values. Unrecoverable blinded schema keys receive an explicit refusal; physical helpers retain their separate role.
- Startup namespace errors propagate. Encryption checks refuse unreadable data without deleting shared records. The old backend-specific skip and plaintext-duplicate cleanup are removed.

## Validation

Source review is complete for the initial memory, disk, and query routes. Final list review, compiler checks, release build, and synthetic API checks remain pending.

The operational plan uses fresh owned synthetic homes and unchanged existing helpers. No test suite, executable exercise helper, primary, archive, migration, cloud activation, installation, or upgrade forms part of this review.

The old executable remains tied to source `122072ed51e20a1f67ad4915c0efcb9a2ffc1551`. Its 1,877 product inputs match accepted main `ea01331f7aa318c8748a6f41fa95e0b4eddf3b3a`.

Boot refusal and corrupt-data branches receive source and compiler checks only. No corrupt home or alternate identity is created. A queued-write observation proves a read before flush only when its saved native sequence span shows that fact.

No benchmark, throughput, whole-process memory-cap, or complete key-rotation claim follows from these correctness changes.
