# Injected record format writer spike

Branch: `spike/format-writer-record-replacement-20261103`.

The executable accepts complete JSON records. The composition root selects
one format and passes it into the same `Store<F>` writer. The format asks a
data-only provider for its values, invokes its injected nested encoder, and
constructs the complete encoded record. One private LastStore owner commits
that record. Omitted fields disappear; `{}` is a valid empty replacement.

```text
lastdb-format-spike --format nested --home /explicit/new/spike-home init
echo '{"title":"first","obsolete":"remove me"}' |
  lastdb-format-spike --format nested --home /explicit/new/spike-home put notes one
echo '{"title":"second"}' |
  lastdb-format-spike --format nested --home /explicit/new/spike-home put notes one
lastdb-format-spike --format nested --home /explicit/new/spike-home get notes one
lastdb-format-spike --format nested --home /explicit/new/spike-home maintain --steps 8
```

The read returns `{"title":"second"}` from the durable result of a different
process. `nested` is the default. `plain` uses a different data directory,
collection, prefix, wire version, key limit and record cap through the same
writer. Each home requires its exact selected descriptor before native open.
Both formats require a newly created spike home; the first spike's version-1
homes are not compatible. Creation refuses an existing directory.

## Components and sequence

1. `main.rs` selects `PLAIN` or `NESTED` and injects it into `Store<F>`.
2. `lastdb_model::record_replacement` supplies logical values without I/O.
3. `format/records.rs` is the sole concrete format assembly. It declares the
   home names, addresses, envelope, limits and selected child functions.
4. `format/fields.rs` owns the child block shape. Its injected functions take
   field values and return a typed block, or decode that block. They receive
   no writer, path, callback for commit, or native operation type.
5. The parent nests that block in its envelope, serializes it, and checks the
   size of the entire result before minting an opaque `EncodedRecord`.
6. `writer.rs` performs bounded pre-write maintenance, converts the encoded
   output into one native operation, and commits it durably before the ack.

`format/mod.rs` holds the neutral contract and opaque output types. Private
fields let only that module's child definitions construct encoded addresses
and records. The writer can consume them, and cannot construct them directly.
It imports no concrete format constants or child codecs.

The nested representation is `{"version":3,"fields":{"version":1,"values":{...}}}`.
The plain representation is `{"version":2,"fields":{...}}`. Each shape has
one typed definition shared by encoding and decoding. The format descriptor
includes both parent and child versions. Child failure or an oversized nested
result prevents the record commit. There is no independent child flush.

## Native maintenance

Each put performs at most two native merge steps before its record commit.
Maintenance failure therefore cannot report a successfully committed put as
failed. `maintain --steps N` explicitly drains more work and reports completed
steps, frontier, run count and pending work. Zero steps just reports state.
The bound is a step count, not a time or byte guarantee for each native step.
This synchronous policy is deliberately small; production scheduling is open.

## Scope and limits

No new crates, macro language, transaction planner or plugin registry. Parent
selection is statically dispatched; nested encode/decode functions are paired
in a typed bundle. Function pointers are not an enforcement sandbox. The
concrete definitions and codecs are trusted format code; a code change must
update its descriptor version when it changes the wire grammar.

LastStore still owns its native run-file format. This is an isolated record
storage prototype, not a new daemon API. Kind is a spike label, not an installed
schema. Schema validation, alternate keys, history, file lifecycle, conditional
writes, field stamps, multi-device merge, cloud backup, the core warm set and
the core accepted-write queue remain outside this slice.

Unconditional replacement can overwrite another application's whole-record
update. The native lock permits one CLI owner at a time, with no retry wrapper.
The input and complete encoded record are bounded. Encoding clones the field
map once. There are no throughput or latency claims.

CI compiles and runs both supplied formats on generated homes, including
replacement, reopening and native maintenance. It retains its transcript
outside Git. No tests or local builds/checks/clippy/benchmarks are run.
