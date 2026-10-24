# Equal-size write comparison

PR #282 needs a one-client and four-client comparison with eight records per request.
The candidate retains one private metadata writer within each commit.
It closes that writer when the object changes or the commit ends.
All publication and disk barriers stay in place.

## Historical evidence

The [PR checks](https://github.com/EdgeVector/lastdb/pull/282/checks) contain the current CI validation record.

The historical phase capture predates the CI-only instruction.
It uses source `14da061a5a8dca6d2555a81de9241a77fd0deebf`, before the metadata change.
It contains 1,000 four-client requests, eight records per request, and 64 schemas.
The capture disables automatic compaction and enables phase diagnostics.
Other work uses the host during this capture.

| Measure | Historical result |
|---|---:|
| Mean complete request | 87.81 ms |
| P99 complete request | 245.67 ms |
| Mean physical commit | 55.35 ms |
| Metadata edits within that commit | 35.03 ms |
| Final device flush within that commit | 7.05 ms |
| Requests per physical commit | 4 |

These measurements identify metadata edits as a large cost in that capture.
They do not establish a source effect or a candidate speed improvement.
The metadata writer previously closed after every hash-group edit.
The next edit reopened the same private file and checked its length.
The change removes that repeated work within one object and one commit.
New numeric counters record writer closes and reopens.

The candidate local build stopped after Tom supplied the CI-only rule.
No candidate measurement ran.
The external archive retains the stop and cleanup receipts for the owned temporary roots.
The capture recovered all acknowledged values and mutation notices after a forced process stop.
A process stop does not simulate device power removal.

## CI comparison

`run_pairs.py` accepts explicit before/after binaries, source revisions, and an artifact output path.
It requires a GitHub-hosted Actions runner.
It never builds a binary or takes a host lock.
Each run starts a fresh generated home with eight request workers.
Each source uses one and four clients with eight records per request.
Each pair alternates the source order; every request remains in the receipts.
No reported request delay is divided by eight.

The default is three pairs with 1,000 requests per run.
Use a separate `--profile` campaign for numeric phase diagnostics and the atom-counter diagnostic.
The CI command is:

```sh
python3 docs/proofs/matched-write-concurrency-20261023/run_pairs.py \
  --before-binary "$BEFORE_BINARY" --after-binary "$AFTER_BINARY" \
  --before-revision "$BEFORE_SHA" --after-revision "$AFTER_SHA" \
  --pairs 3 --count 1000 --output "$RUNNER_TEMP/matched"
```

The profile counters include nested timers; do not sum parent and child timers.
Apple captures require one final device flush per physical commit.
Linux retains its native file and directory barriers and has no Apple device-flush counter.
CI results on Linux cannot establish the macOS device cost.

Successful captures require correct known-key values, durable receipts, exact notice replay, and a clean shutdown.
Ownership checks precede deletion of each generated home and temporary root.
The CI artifacts retain raw samples, binary hashes, phase data, and archive hashes.
The historical raw archive stays outside Git under the LastDB state artifact root.
It remains local until an external upload supplies a verified download receipt.
No raw logs, samples, or generated receipts belong in this source directory.
The next result must include CI measurements for the combined PR source.
Continuous-write compaction progress also remains required before merge review.
