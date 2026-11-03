# Injected record format writer spike

Branch: `spike/format-writer-record-replacement-20261103`.

The executable accepts complete JSON records. The composition root selects
one format and passes it into the same `Store<F>` writer. The format asks a
data-only provider for its values, invokes its injected nested encoder, and
constructs an admitted batch containing the record and its alternate pointers.
One private LastStore owner commits that batch atomically. Omitted fields
disappear; `{}` is a valid empty replacement.

```text
lastdb-format-spike --format nested --home /explicit/new/spike-home init
echo '{"title":"first","obsolete":"remove me"}' |
  lastdb-format-spike --format nested --home /explicit/new/spike-home put notes one
echo '{"title":"second"}' |
  lastdb-format-spike --format nested --home /explicit/new/spike-home put notes one
lastdb-format-spike --format nested --home /explicit/new/spike-home get notes one
lastdb-format-spike --format nested --home /explicit/new/spike-home maintain --steps 8
echo '{"title":"addressed through an alias","alias":"team"}' |
  lastdb-format-spike --format nested --home /explicit/new/spike-home put notes one
lastdb-format-spike --format nested --home /explicit/new/spike-home lookup notes team
```

The read returns `{"title":"second"}` from the durable result of a different
process. `nested` is the default. `plain` uses a different data directory,
collection, prefix, wire version, key limit and record cap through the same
writer. Each home requires its exact selected descriptor before native open.
Both formats require a newly created spike home; earlier spike homes are not
compatible. Creation refuses an existing directory.

## Components and sequence

1. `main.rs` selects `PLAIN` or `NESTED` and injects it into `Store<F>`.
2. `lastdb_model::record_replacement` supplies logical values without I/O.
3. `format/records.rs` is the sole concrete format assembly. It declares the
   home names, addresses, envelopes, alternate definition, limits and child functions.
4. `format/fields.rs` owns the child block shape. Its injected functions take
   field values and return a typed block, or decode that block. They receive
   no writer, path, callback for commit, or native operation type.
5. `format/alternate.rs` supplies a separate stored entry containing only the
   primary key. It also supplies a tombstone for a removed alternate address.
6. The parent composes the record and alternate operations and checks every
   key, body, total batch size and operation count before minting `EncodedBatch`.
7. `writer.rs` performs bounded pre-write maintenance, converts the admitted
   operations into one native batch, and commits it durably before the ack.

`format/mod.rs` holds the neutral contract and opaque output types. Private
fields let only that module's child definitions construct encoded addresses,
operations and admitted batches. The writer consumes them without constructing them.
It imports no concrete format constants or child codecs.

The nested representation is `{"version":5,"fields":{"version":1,"values":{...}}}`.
The plain representation is `{"version":4,"fields":{...}}`. Each shape has
one typed definition shared by encoding and decoding. The format descriptor
includes both parent and child versions. Child failure or an oversized nested
result prevents the batch commit. There is no independent child flush.

## Multiple keys through pointers

The optional `alias` field declares one nonunique alternate key. When present,
it must be a nonempty string. The encoded alternate hash contains kind and
alias; its range contains the primary identity. Several records can therefore
share the same alias without overwriting each other's pointers. The pointer
body is `{"version":1,"key":"one"}`; it contains no record fields.

A replacement reads the previous record to discover its old alias. The format
then produces the whole new record, a new pointer when applicable, and a
tombstone for the old pointer when its address changes or disappears. It does
not merge old field values. A local mutex covers the old-record read through
commit; the native home lock excludes other process owners. The entire batch
is committed with one frontier increment. Duplicate operation addresses and
oversized batches are refused before commit. No startup key audit is added.

`lookup KIND ALIAS` reads one exact alternate hash, decodes its pointers, then
batch-reads the primary keys. Both stages use one held native snapshot. Replies
include the snapshot frontier and primary keys for subsequent mutations.
Pointer identity and the queried alias are checked while resolving the selected
records. A missing target is an error. This spike returns at most 64 records;
it refuses an incomplete result rather than returning a partial list. Native
read budgets can also refuse work. Pagination and general key declarations
remain future work. No unkeyed scan or record-payload duplication is introduced.

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
schema. It demonstrates one nonunique alternate field, not production schema
installation or its general key grammar. History, file lifecycle, conditional
writes, field stamps, multi-device merge, cloud backup, the core warm set and
the core accepted-write queue remain outside this slice.

Unconditional replacement can overwrite another application's whole-record
update. The native lock permits one CLI owner at a time, with no retry wrapper.
The input and complete encoded record are bounded. Encoding clones the field
map once. There are no throughput or latency claims.

CI compiles and runs both supplied formats on generated homes, including
replacement, reopening, native maintenance and shared/moved/removed alternate
pointers. It retains its transcript outside Git. No tests, local Cargo
builds/checks/clippy, or local benchmarks are run.
