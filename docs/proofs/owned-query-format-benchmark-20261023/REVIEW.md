# Owned query-result formatting review

Baseline: `bcdcbba38b20188bbd8c506326d6c451f0363f50` (main after PR #281).
Implementation: `74355dfd67a7082254247ebd880f13ef21683273`.

## Change and value

The query executor returns an owned field-oriented result map. Its only formatter
caller borrowed it to build a record-oriented map, cloning the key, JSON value,
and five atom-metadata members for every selected field. The input was discarded
immediately afterward. Both conversions now consume their input, so those values
move into records. Field-name strings still need separate copies for each record's
field and metadata maps. Author selection borrows the chosen writer string until
JSON serialization instead of cloning it first.

Two Rust files: 20 lines deleted, 14 added, net six removed. The value is removing
a redundant deep-copy stage, rather than a large source reduction. Seven clone
sites leave the per-field conversion; a conditional author-string clone leaves
the per-row envelope builder. No new types, dependencies, macros, cache, unsafe
code, runtime setting, or alternate path.

## Equivalence inspection

- The converter has one code caller, the host row formatter; that formatter has
  one code caller, immediately after the executor yields its owned result map.
  The query module export and one phase documentation reference also remain.
  Workspace compilation checks the ownership change at every caller.
- Input fields and output types are unchanged. The same KeyValue goes into the
  same record grouping. Values move rather than clone. FieldMetadata still has
  exactly atom_uuid, source_file_name, metadata, molecule_uuid, writer_pubkey.
  FieldValue.written_at remains omitted, as it was before.
- Both loops traverse the same maps. Per-record field/metadata insertion policy
  is unchanged. Missing fields, empty maps, and duplicate coordinates across
  fields retain the existing behavior. Final total-order sorting is untouched.
- Author selection still chooses the first Some writer and then rejects empty
  strings; as_deref replaces cloning at the same point in that expression. JSON
  serialization produces an owned Value before the borrow ends. The prior
  unspecified choice between distinct per-field writers is unchanged.
- Projection, hidden sort-field removal, ordering, pagination, conflict flags,
  query capture/read scopes, unresolved/tombstone counts, and error handling are
  untouched. No storage, write-path, durability, authorization, or HTTP schema
  changes. Logical serving values are not mutated: the formatter owns its input.

No blocking correctness issue found in this self-review. The Rust helper's
parameter changes from borrowed to owned. These crates are publish=false and its
one workspace caller is updated; out-of-tree path consumers would need to supply
an owned map. The supported socket API and serialized records are unchanged.
The change does not eliminate copies needed to read logical warm state or to
serialize a response; it removes the additional formatter-stage deep copy.

## Product evidence scope

The same-fixture driver reuses the committed capture, controlled affinity, read/
write validation, and ownership cleanup helpers. Baseline and candidate builds
use the same primary worktree and target directory, sequentially, with the same
compiler/flags/lockfile. Source files remain frozen during each build. Heavy Cargo
and the complete measurement window use the host performance lock.

Plain, encrypted, and preloaded configurations each use a baseline-created
synthetic fixture with 512 Hash and 512 HashRange flat four-field records (1 KiB
bodies), plus 256 three-field HashRange records containing nested JSON documents.
Fixture paths/content digests are checked before each run and after all runs.
Four alternating adjacent pairs/configuration, with configurations interleaved.
Client CPU 0, daemon CPU 1; all four configured workers share that server CPU.

Workloads include timed warm Hash/HashRange points, bounded range/prefix/
multi-get/partition reads, nested JSON
ranges, requested queued batches, and sustained warmed durable single/batch
writes, plus untimed misses/duplicate-input and Delete/Purge integrity probes.
Full pre-write response fingerprints compare key/field/metadata/envelope
shapes within each configuration, including projections with the implicit hash-key field and an unprojected sort
field. Every query checks metadata field membership and atom identifier types.
Every timed read checks values/counts/order; write/flush/reopen integrity checks
read back all flat records and the nested partition. The driver's fixed key/
partition filters preserve the no-full-schema-scan contract.

One host/client, small warm datasets, OS caches and neighboring load uncontrolled.
CPU affinity does not establish production concurrency/capacity or significance.
Fingerprints use one writer population; mixed-writer author choice is inspected
in source. This does not exercise every conflict state or power-loss recovery.
No test suite added or run, following repository instructions.

See RESULTS.md for measurements, completed validation, and the merge assessment.

## Final validation and merge assessment

Optimized baseline/candidate builds and full workspace non-ONNX clippy with
warnings denied passed, along with formatting, source/function size and
no-new-allows checks. Primary 24 runs/226,560 timed requests passed, including
240 complete pre-write response fingerprints. A baseline-only benchmark assertion
initially forgot the implicit hash-key projection field; no timed pair completed.
That attempt is explicitly excluded, its four stopped homes cleaned, and the
experiment restarted with the correct expectation. Product source was unchanged.

Structured JSON p50 medians improved 4.8–8.6%; flat point medians ranged -1.9% to
+0.9%. Unfavorable flat multi-get and encrypted-write cells prompted a further
24 runs/27,136 requests. Multi-get32 medians were -0.3% to -1.3%, with all four
preload first-block p50s lower. Encrypted durable singles/batches were +1.6%/+2.0%
with mixed pair signs. All applicable values, metadata, fingerprints and reopened
records were correct; all owned synthetic homes were cleaned.

No blocking correctness issue found. Recommendation: merge the ownership cleanup
for less duplication and the observed structured-read benefit. Do not advertise
a general speedup or an improvement to writes. Keep the original adverse cells
and the scope limits; their precise cause is unresolved. HTTP behavior remains
stable, while out-of-tree Rust path consumers must pass ownership to this helper.
