# Exact native routing review

Baseline: bd517062ed3b86aef3e431d1e2cd64fd9dd08aa5 (current main).
Implementation: 6965ecb93a4007e50130cc479b4a4040ad03c997.
PR #277 is a separate unmerged partition-reader change, not part of this baseline.

Implementation diff: 85 Rust lines deleted, 44 added; net 41 removed across 17 files.

## Equivalence argument

main_collections_for_key(key) returned exactly vec![main_collection_for_key(key)].
The private native read_candidates wrapper simply forwarded that singleton.
Replace membership checks in completeness, generation-control invalidation, and
schema-key publication with equality against collection_for_key. Source-body
matching no longer constructs a candidate vector or models an impossible missing
primary destination. Both reads and writes share this same scalar selector.

PackKvStore coordinates previously returned [write_target(namespace, key)], an
array of length one. Expose that existing target calculation as coordinate and
use it in reads and writes directly. Replace flat_map/extend with map/push only
where the old input was this exact singleton. The consumers' actual dependency
vectors, source sorting/deduplication, and control expansion remain. Atomic source
invalidation still takes a slice, now explicitly containing that one coordinate.

Captured atom body reads no longer select first/default from an always-nonempty
array. Core definition records retain one CoreMemorySource per exact record.
Tip refresh retains the body coordinate only if the prior source certificate
contained it, as before. Initialization markers and the disabled restore path use
the same canonical routing, with no changes to their checks or formats.

Raw value-length preflight still groups requests by collection and restores input
positions. Each slot belongs to exactly one group, so lengths need direct assignment
rather than maxima/OR merging across alternate collections. Duplicate input keys
keep separate output positions. Missing lengths remain None. The per-collection
admission callback and BTreeMap group order are unchanged.

Prefix/range routing remains plural: those requests can intersect several current
collections. Logical paths still retain multiple real dependencies, including
body, tip, Delete barrier, and generation controls. Scoped-key recognition,
namespace overrides, unknown-key routing fallback, and physical key encoding are
unchanged. No old-collection migration or dual read exists in this native format.

No blocking correctness finding in this self-review. The benchmark exercises
normal schemas, tips, atom bodies, mutation publication, misses, prefix/range,
multi-get, Delete/Purge, and reopen. It does not exhaustively exercise every
physical namespace, protein sibling source, generation replacement, disabled
cloud restore, CAS conflict, or power-loss recovery. Those changed internal
call sites were source-reviewed.

## Validation and recommendation

Corrected baseline/candidate optimized product builds passed. Full workspace
non-ONNX clippy passed with warnings denied. Formatting, source size, function
size, no-new-allows, artifact, whitespace, and Python syntax checks passed.
No test suite was added or run, per repository instructions.
All 24 primary and 16 focused runs passed their applicable integrity checks.
The focused runs verify values and write flush/reopen; the primary matrix also
checks Delete/Purge visibility. No blocking correctness finding was identified.
Performance is inconsistent, with a slower plain Hash point median in both
measurements; recommendation: hold, not recommended for merge yet. The net 41-line
benefit does not justify the current point-read uncertainty. See RESULTS.md.
