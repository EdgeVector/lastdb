# Native home API operational receipt — 2026-10-15

Source: `0521faa385aa799ab561934284071ca4c2fc0882`.
The commands used explicit generated homes. No primary, primary copy, archive, factory, or node was used.

Release SHA-256:

- `lastdb-disk-home`: `3a9b6c9a5070d00672f46de52d27daa5885ddeded3cd5255d016ed7c21bcd5f5`
- `lastdb-pack`: `c0c605d11be7d9d1bdd9ccbea0560b00fa4d804bc6b904366f738f1e79356236`
- `lastdb-journal`: `9329b879fed621fccf7d8a60bf38ca401196ea3086ba6f8908502375b267b034`
- `lastdb-range`: `ce5b7080a50d35317124ae4f363124cf83d5546d2984e7dfb7aa031e4a83c4fa`

## Generated data

The small home contained 419 records. The large home contained 20,403 records.
Both contained 400 target keys under one encoded molecule hash, with 100 tombstones.
Additional records used a different hash with the same 16 destination groups.
Three other records covered an atom id with 8,192 opaque binary bytes, a schema id, and a plain id.
Both homes used the native saved placement codec, fanout 16, and initial physical version `(0,0)`.

The commands built both homes, read 468 collected coordinates, and read the whole target hash in pages.
They also requested seven records, an explicit half-open range, and a batch with a three-key work budget.
An append left 32 complete operations outside the readable root.
The next write recovered that batch before its 32 new operations, then published both complete batches.

## Correctness

- All four 468-coordinate reads matched the expected values and absences.
- Before publication, the pending journal did not change the readable results.
- After publication, all 327 present results matched the expected bytes.
- All seven range results matched the expected order, values, and physical versions.
- The initial whole-hash reads returned 300 live records and examined 400 keys each.
- The later whole-hash read returned 324 live records and examined 432 keys.
- Both seven-record requests examined 25 keys and reported an unfinished range.
- Both explicit ranges returned 75 live records and examined 116 keys.
- A seven-item batch with a three-key budget returned three successful slots and four explicit resource errors.
- A range with a four-key budget returned a resource error with zero output bytes.
- All 58 initial data and metadata parts kept the same SHA-256 after both journal batches.
- The final root reached complete batch 2, generation 7, with required feature bits 7.
- Both fixed-object checksums and the root's exact home-descriptor digest matched.
- The older reader refused feature 4 before it emitted a value.
- The artifact publication command refused the native home before any write.
- A separate artifact still used feature bits 3, a zero home digest, and returned its original value.

## Read cost

The following single command samples include the initial home handle check.
The native read call itself charges its CURRENT and descriptor, plus all addressed work, against one budget.
The command handle check adds 12,288 format bytes outside that call. A node can hold the checked home handle.
The disk receipt includes those command-level bytes.

| Read | Small home | Large home |
|---|---:|---:|
| One key: locator pages | 1 | 2 |
| One key: call file bytes | 58,461 | 74,523 |
| One key: command time, ms | 0.620 | 0.503 |
| First seven records: examined keys | 25 | 25 |
| Whole target hash: examined keys | 400 | 400 |
| Whole target hash: call file bytes | 568,538 | 1,004,353 |
| Whole target hash: command time, ms | 3.802 | 5.158 |

The extra records increased tree height and metadata pages. They did not increase the target key count.
Ordinary reads reported zero legacy replay, journal replay, group opens, footer bytes, and directory reads.
Operating-system file pages were not reset. These samples are not latency percentiles or fresh-node proof.
The allocation counter tracks disk buffers. It does not measure process memory or JSON output space.

## Checks and limits

Package and workspace library/binary Clippy passed with warnings denied.
Format, changed-file size, changed-function size, new-allow, write-path, URL, artifact, redaction, byte-slice, and secret checks passed.
No tests, fixture harness, damaged files, failure injection, or primary data were used.

This proves the explicit native home API and command. The node backend still uses the legacy home path.
Native query integration, memory ack admission, the warm set, conversion handoff, backup, rollback, and fresh-node proof remain unfinished.
The generated homes and scratch files are removed after the PR checks.
