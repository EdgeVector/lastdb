# Exact-key refactor review

Reviewed implementation: `d7a5f441cc084a9624fac97d60d811c9b230702b`.
Comparison base: `4118c7baa12539c8c6bcf4391c497159f2a1529a`.
Both include the snapshot-commit change; the comparison isolates PR #269.

No blocking correctness finding was identified in the 31-file implementation diff.
This is a self-review, not an independent review.

## Checks of the implementation

- The five removed codec APIs each returned a singleton. Supported encodings
  are Plain/BlindV1 for hashes and Plain/OpeV1 for ranges; there is no migrating
  encoding variant whose alternate key could be lost.
- `load_visible_per_key_records` reads distinct exact keys, then maps each original
  request position back to its result. Duplicate input positions and missing
  results survive; it does not return the storage deduplication order to callers.
- The exact storage read and delete-barrier routines remain unchanged. They still
  apply active generation overlays, sparse generation deletes, captured changes,
  and the visible Delete barrier before records reach callers.
- Query receipts retain the same encoded base key that the old singleton group
  contained. The native dependency builder and its physical-source validation are
  unchanged. Returning a record without a key from the batch helper does not
  replace an actual source receipt with a different key spelling.
- The CAS/protein batches initialize one exact string per slot. Their slot maps
  assign dense indices, and every map entry fills its assigned string before
  the read; the former empty candidate-list placeholders are not observable.
- Range bounds still encode the same API values once. Empty-prefix and empty-key
  behavior remains unchanged. HashRange exact/multi-get filters remain in API
  coordinates because their narrowed loader remaps the selected ranges.
- Captured changes now encode one key once instead of encoding its singleton
  candidate list and encoding the primary key again. The absence entry still
  precedes any present replacement.
- Delete/purge uses the same storage hash/range conversion and framed key builder.
  It retains encoded coordinates directly instead of decoding its own generated
  key. Old-key protein removal still checks the expected atom entry and metadata.
- Preload encoding identity is an in-process fingerprint, not an on-disk format.
  Removing the singleton-vector debug representation changes no stored key or
  wire format. Both identity creation and checking use the new fingerprint.

## Evidence and limits

The implementation head's required GitHub CI succeeded (run 39840238019).
The preceding local Mini build and non-ONNX workspace Clippy lane also passed.

The paired product benchmark described in RESULTS.md covers generated Hash and
HashRange records with plain keys, default blind/OPE keys, and selected schema
preload. Its integrity checks include values, misses, duplicate-key membership,
range ordering, Delete/Purge visibility, explicit flush completion, and graceful
reopen. Those checks support the measured workloads, not every mutation shape.

CAS conflicts, protein re-keying, active molecule-generation replacement, concurrent
writers, power loss, and cross-device sync are not exercised by this benchmark.
Their relevant call paths were reviewed in source. No test suite was added or run.

## Performance observation

The primary default-encrypted warm HashRange point-read median increased from
0.401 to 0.486 ms (+21.3%), while pair-level changes varied from -14.6% to +67.0%.
The batch/range reads changed by about 1–3%, and the longer write-phase medians
improved about 5–7%. This is a mixed performance result, not a demonstrated
universal speedup. No source change was made in response to an unlocalized,
variable timing difference; the exact implementation measured remains unchanged.
