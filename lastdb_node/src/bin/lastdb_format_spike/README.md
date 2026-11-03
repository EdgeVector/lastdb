# Whole record format writer spike

Branch: `spike/format-writer-record-replacement-20261103`.

The executable accepts complete JSON records, asks a data-only provider for
their values, encodes them using the top-level format, and writes them through
one private LastStore owner. A put replaces the whole record. Fields omitted
from the new object disappear; `{}` is a valid empty replacement.

```text
lastdb-format-spike --home /explicit/new/spike-home init
echo '{"title":"first","obsolete":"remove me"}' |
  lastdb-format-spike --home /explicit/new/spike-home put notes one
echo '{"title":"second"}' |
  lastdb-format-spike --home /explicit/new/spike-home put notes one
lastdb-format-spike --home /explicit/new/spike-home get notes one
```

The final read returns `{"title":"second"}`. Each command opens and closes
the home, so the read uses the durable result of a different process.
Creation refuses an existing directory. Other commands require the spike's
exact format marker before opening its native data. The home is separate
from an installed LastDB home; it has no daemon or socket.

## Components

- `lastdb_model::record_replacement` holds only logical values and their
  provider contract. The existing model dependency/purity rules apply.
- `format.rs` owns the record prefix, framing, version, wire fields, names and
  limits. It composes key and value encoder functions at compile time.
- `writer.rs` owns the only native write handle. Its API takes a complete
  record provider; it exposes no raw key/value write or native accessor.
- `main.rs` is a small CLI for creating, replacing and reading records.

There is no macro language, new transaction planner or new runtime crate.
This uses the existing LastStore durable commit. LastStore's run-file format
remains its existing implementation; this spike injects only the record format.

## Scope

This is an isolated storage prototype, not a new daemon API. Record kind is
a label within the spike, not an installed LastDB schema. It does not claim
schema validation, alternate keys, history, file lifecycle, conditional writes,
field stamps, multi-device merge, cloud backup, the core warm set or the core
accepted-write queue. Existing LastDB behavior remains available for comparison.

Unconditional replacement can overwrite a concurrent writer's whole record.
The native owner lock prevents simultaneous CLI owners; there is no retry or
compare-and-set wrapper here. The stdin and encoded record sizes are bounded.
Encoding currently clones the field map once; this is not a performance claim.

Build and run only in CI under Tom's host rules. The branch-only job added to
the existing manual CI workflow demonstrates puts and reads on generated data
and retains the transcript outside Git. It runs no tests or benchmarks.
