# Compatible primary disk-growth change

The source change adds automatic compaction and small file cleanup steps. It
preserves the deployed journal format. It also stops automatic work before the
final shutdown flush and resumes unfinished cleanup after a restart.

The final runtime result is the `RESULTS.json` workflow artifact. A source check
does not prove that result. The primary upgrade requires a successful run and
verification of the exact program hashes.

## Method and source

The compatible baseline is `7d079c6c10fcfe17c69a7169ee804a2a8e0892f7`.
The product change ends at `388e70c4570b33f0750a9a89be3a8625e56ebb3a`.
The workflow builds that change from the proposed PR head, including these source
scripts. Its immutable head revision appears in `source.txt` and `RESULTS.json`.

The GitHub Apple silicon runner builds both programs with:

```sh
cargo build --profile dev-release --locked -p lastdb_node --bin lastdb --bin lastdbd -p laststore --bin lastdb-pack-maintain --bin lastdb-disk-home
```

The workflow then calls `run.py` with the two program directories, an explicit
temporary report directory, and the proposed source revision. The script uses
generated data and actual daemon requests. It checks durable writes, exact-key
reads, deletion, compaction, clean stops, old-program reads, and a restart during
cleanup. Each program receives 256 batches of 16 updates. A separate generated
home contains more than one million stored records.

## Artifacts and limits

[PR checks](https://github.com/EdgeVector/lastdb/pull/285/checks) link the exact
workflow run. It uploads `primary-compatible-growth-receipts` and
`primary-compatible-growth-programs` with 14-day retention. Program checksums
appear in `SHA256SUMS`. The operator records each artifact download URL, archive
checksum, source revision, and expiration outside Git before local cleanup.

Raw output uses the runner's temporary directory. It never enters Git. No primary
home or archive enters the runner. Timing results remain advisory. The primary
upgrade and its live read, write, compaction, and restart checks remain separate.
Tests stay retired.
