# Native read state and complete fields

A native query captures one complete disk root.
Live graph tips, atoms, and deletion marks do not carry that root identity.
PR #204 enabled those live values in the native `off` mode and allowed a read to mix states.
A flush could remove a selected live atom before its later fetch, while the query still held an older disk root.

## Read policy

One backend-aware policy controls live graph reads.
Legacy unprefixed reads keep their graph-first behavior in every acknowledgement mode.
Native pack reads exclude the live graph and use their captured committed root.
The native warm cache remains available only when its saved frontier matches that root.
The existing native call holds and shared 10,000-key budget stay in force.

The policy covers exact fields, Hash fields, secondary fields, ranges, counts, document envelopes, and live schema deletion filters.
Direct count calls now capture a native read scope, as ordinary queries already do.
Named local record-molecule lookups also exclude live graph tips on the native backend.
Shared namespace filters keep their existing behavior.

This restriction restores the existing native activation gate.
It does not complete the separate work for protected memory writes, full logical-path admission, or schema-cache versions per query.
Native resident-first modes and cloud-connected native homes remain refused.

## Selected tip without its atom

The shared selected-tip atom loader now requires an atom for every unprefixed local tip that it selected.
A missing atom returns `SchemaError::MissingLocalAtom`, which the host maps to a server error.
Storage and decode errors on that read path preserve server classification.
Typed resource limits and access failures retain their distinct meanings.
The exact and secondary query paths no longer replace these failures with an invalid-field error.

An absent key produces no selected tip and remains a normal absence.
An optional field with no tip also remains absent.
A scoped body that the caller cannot read remains a filter.
The document-envelope lookup uses the same selected-tip rule instead of silently falling back after a missing local atom.

The shared failure check prevents successful partial local field results.
It does not repair damaged data or enable the native backend on the primary.

## Validation

Use the product commands on fresh generated nodes and the existing non-test build and lint gates.
Keep scalar, batch, range, optional-field, delete, capacity, and normal-reopen receipts together.
Do not force missing atoms through data corruption or add a fixture harness.
The source audit establishes the missing-atom refusal and the read policy; ordinary product results cover the exercised data paths.
