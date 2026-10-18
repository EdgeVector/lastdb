# Schema preload product measurements

These measurements use ordinary Mini requests and fresh generated homes.
They do not use the primary, an archive, an existing home, or a fixture harness.
The committed engine at `aa64f61cd3d2d01a3f8953d4a1b3cf21f9940bab` is the before reference.
The candidate uses the current source after its build completes.

The [final results](../../lastdb-schema-range-preload-results.md) use candidate 5 with SHA256
`2c14d7d2f04865454ba9b1a41ba441ee7b69e2bad11a577f1056fe5b43c1324b`.
The [final identity check](candidate5-final-identity-check.json) matches 1,656 source files with zero differences.
The [validation report](VALIDATION.md) records the release build and non-test source checks.

| Final receipt directory | Result |
|---|---|
| `after-off-main-256-candidate5/` | All 54 reads, writes, flushes, and reopen pass. |
| `after-on-main-256-candidate5/` | All 54 reads, writes, flushes, and reopen pass. |
| `after-off-main-20000-candidate5/` | 42 correct exact/range reads, then the recorded preview resource refusal. |
| `after-on-main-20000-candidate5/` | All 44 reads pass, including the preview and reopen. |
| `after-on-small-key-budget-256-candidate5/` | Correct DiskOnly fallback; no repeated complete-key load attempt. |
| `after-on-path-pressure-256-candidate5/` | Values purge at the 128-path cap; complete keys stay Ready. |

`comparison-small-candidate5.json` and `comparison-large-candidate5.json` summarize the preserved receipts without another product call.
The [extra product report](EXTRA_PRODUCT.md) covers more query shapes, physical compaction, and concurrent-load request overlap.
The [size-limit audit](KEEP_SMALL_LIMIT.md) preserves the separate existing shutdown size limit.
Earlier candidate directories remain diagnostic and do not enter the final comparison.
All final measurement nodes stop normally.
Every final run directory contains a successful supported cleanup receipt with its exact owned data root absent.

`reference.json` records the clean reference source and release executable identity.
The reference build used its own DEV checkout and target directory under the host Cargo lock.
After the final comparison, normal Git worktree removal reclaims that clean detached reference checkout without force.
`reference-cleanup.json` records exit code zero and the exact checkout path absent.
The temporary executable is no longer retained; the recorded commit and build command support a later rebuild.

## Measurement command

```sh
python3 docs/proofs/schema-preload-product-20261017/measure.py \
  --binary /absolute/path/to/lastdbd \
  --variant before \
  --receipt-dir docs/proofs/schema-preload-product-20261017/before-main-256 \
  --source-commit aa64f61cd3d2d01a3f8953d4a1b3cf21f9940bab
```

The default cohort has 256 records across four hashes.
Each record has hash and range fields, a title, and a generated 1 KiB body.
The range has 54 bytes: a column, an eight-digit position, and a 40-byte slug.
The normal full-path cap is 10,000.
Writes use batches of at most 64 mutations, with explicit flushes after seed batches.

The driver records first exact and range queries after process reopen.
It then records repeated 16-, 64-, and 256-key reads where one hash contains enough keys.
Four independent first range queries run concurrently after another process reopen.
A capped schema preview uses the explicit inspection header.
Updates, inserts, deletes, and a two-mutation key move use the ordinary batch route.
The final phases cover selected-value reads, flush, and normal reopen.

Use `--records-per-hash 2500 --hashes 4` for 10,000 keys.
Use `--records-per-hash 5000 --hashes 4` for 20,000 keys.
Use `--records-per-hash 25000 --hashes 4` for 100,000 keys.
Use `--reads-only` to omit mutation and selected-value-walk phases on large cohorts.
Each cohort creates new data; no data home is copied between programs.

Candidate off uses an empty `LASTDB_SCHEMA_PRELOAD_SCHEMAS` value.
Candidate on adds these arguments:

```sh
--env LASTDB_SCHEMA_PRELOAD_SCHEMAS=SchemaPreloadProduct \
--env LASTDB_SCHEMA_KEY_BYTES=134217728
```

Run deliberate low-cap or key-cap cases in separate fresh roots and receipt directories.
Do not combine their refusals with normal-cap timings.

## Evidence and limits

Every successful request keeps its full request, response, cost receipt, and elapsed time.
Refused requests keep the error response.
Read checks compare generated values, live membership, range order, and returned atom and molecule identities.
Status observations retain native counters and sampled process memory.
The candidate stores key counters under `native_disk.schema_keys`.
The observation record also exposes those counters with the `schema_key_` prefix.
This receipt-only change follows the reference runs; the requests and value checks stay the same.
The memory samples are not a complete peak-memory measurement.
The final driver also records allocated data-home disk size after seed and after node stop.
It uses `du -sk` on that exact owned data home, outside query timing.
The disk-size scope excludes private executable copies and request receipts.
The earlier reclaimed homes have no corresponding exact data-home size receipt.
Queued-write observations report whether pending work exists before and after the read.
A false observation does not prove a read before flush.

Process reopen clears process memory; it does not clear the operating-system file cache.
The machine has no exclusive reservation.
Five repeated reads supply a median and an observed p95; they do not establish a latency guarantee.
First-query time is separate from repeated-read time.
Missing tip or atom reads remain possible after complete key preload.

The driver copies the executable into its private owned temporary root before node start.
It preserves receipts in the requested proof directory and stops every owned node normally.
The temporary data stays until supported exact-path cleanup follows receipt preservation.
All final roots now have successful cleanup receipts; no generated data home or private executable copy remains.
Data homes and executable copies do not enter the proof directory.

After a run, `preserve_run.py --receipt-dir <run-directory>` saves its exact sources and receipt hashes.
`cleanup_owned.py --receipt-dir <run-directory>` checks ownership and previews the supported exact cleanup.
The actual cleanup uses `--execute-reviewed-preview` after review of the preserved preview.

## Initial low-cap observation

`before-256/` preserves the first 128-path cohort and its exact source files.
Its first and repeated exact and range reads return correct generated values.
Its four concurrent 16-key requests receive capacity refusals.
That aggregate request does not fit the lowered cap.
Every owned node stops with exit code zero.
This is separate from the normal 10,000-path-cap measurements.
