# Journal resume operational receipt — 2026-10-15

The commands used separate generated artifacts. No primary home, primary copy, archive, factory, or node was used.

Candidate source: `44459824d5dbfb2cd5d5c844a58f6444a467c1c6`.

Release SHA-256:

- `lastdb-pack`: `a7e14455ffaaaa0a05565bf4336efaf6f8dbdb6d7267b1a3a6fac2e92e134b30`
- `lastdb-journal`: `0864dde0a3f290d1e67982340098f861c3e55867e264ba0bf8ea16fdb69d9f25`
- `lastdb-range`: `855d91b56898dfee91be2f12f89ec1f0caef31d7f7e319375ed672d8b941218d`

## Generated data and commands

The initial artifact contained 64 records. Each command used an explicit artifact path and explicit input file.
An append command left two complete 16-operation batches outside the committed root.
A subsequent write command recovered those batches before its first input mutation.
It then wrote 128 operations and published after each two batches.
A second write command reopened the artifact and wrote another 32 operations.

The same sequence used an older feature-1 artifact from PR #195, with 32 new operations after recovery.
This was normal command exit and reopen. No damaged files, injected failures, or interrupted writes were used.

## Results

- All five exact-read results matched the expected values across 129 coordinates each.
- Both hash-range results matched the expected values, physical versions, and order.
- The new artifact reached batch 14, generation 17. The older artifact reached batch 6, generation 7.
- The writer recovered both pending batches before it accepted new input in both formats.
- All eight earlier parts in each artifact kept the same SHA-256 bytes.
- All 30 physical frame identities were unique across the two artifacts.
- All 12 archived tail files passed their checksums. Each predecessor link matched its exact archived bytes.
- The new artifact's initial stream admitted no frames.
- Both final tails were empty at the committed frontier. A repeated publication kept generation 17.
- The older reader refused required feature 3 before it emitted a value.
- Ordinary exact and range reads reported zero journal replay bytes and zero legacy replay bytes.
- Recovery at the final checkpoint returned zero batches with zero replay bytes.

The 128-operation write command took 1.121 seconds. The older-format recovery and 32-operation write took 0.716 seconds.
These are single command times on a shared host, not latency percentiles.
The final new-artifact range returned 85 records in three pages and examined 112 keys, including tombstones.
It took 1.544 milliseconds and read 213,205 file bytes. Its largest tracked temporary allocation was 231,253 bytes.
That allocation counter is not process memory size.

## Scope

This receipt proves the artifact commands and normal reopen path. It does not prove a crash window or a native LastDB node.
Native home activation, the memory ack boundary, conversion, backup, rollback, and the primary upgrade remain separate work.
The receipt preserves no generated values or database home. The owned scratch path is removed after PR review checks.

## Non-test checks

The package and workspace library/binary Clippy checks passed with warnings denied.
The format, changed-file size, changed-function size, new-allow, write-path, URL, artifact, and secret checks passed.
No tests or fixture harness ran.
