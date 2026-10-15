# Complete journal quota before append

The writer can now calculate and own the quota for one complete batch before the stored bodies move.
The calculation covers BEGIN, every operation, COMMIT, worst-case uncompressed frames, data-part headers, and block-map pages.
A put with an empty body differs from a tombstone. Collection, key, group, body, operation count, and total bytes retain their format limits.
The append encoder and the public shape quote use the same checked calculation.

`JournalQuote::from_shapes` accepts dimensions without a body copy.
`HomeDescriptor::quote` derives each native group and borrows the supplied stored bodies.
`JournalWriter::reserve_quote` returns a non-cloneable `JournalReservation` from that exact writer instance.
`HomeJournal::reserve` exposes the same boundary for native mutations.
The ordinary append path also uses a reservation.

The reservation ledger includes all outstanding tickets plus the writer's durable suffix.
The existing aggregate cap remains 512 MiB, with 8,192 bytes reserved for the fixed publications, and at most 4,096 batches.
A different writer cannot consume the ticket. Larger actual dimensions fail before file work begins.
An unused ticket returns its quota when it drops.
Before append I/O, the ticket becomes uncertain. A failed append retains that charge on the poisoned writer.
A successful append transfers its actual worst-case charge to the durable suffix.
Only a successful complete directory publication clears that suffix charge.

Pressure includes the bytes, ticket count, and monotonic age of all unmaterialized work.
The caller receives a materialization recommendation at 384 MiB or two seconds.
New reservation admission stops at five seconds. Existing tickets can still append so their owner can drain the accepted work.
The writer does not start a timer or publish an unexpected root from `append`.
The caller must schedule its normal flush and act on the pressure result.

## Explicit operator command

`lastdb-disk-home plan --home PATH --input JSONL` calculates the journal quota without opening a writer.
The command reads the home identity and CURRENT. It writes no TAIL, root, or data part.
It accepts the same complete JSONL mutation input as `write`, with the existing 1 MiB command body limit.
Its output states that directory capacity, logical records, and memory acknowledgements are not reserved or authorized.
The `append` and `write` results also show the held quota, the transfer to the suffix, and the final state.

## Remaining admission gates

This reservation is an engine quota. It does not allocate filesystem space.
It does not reserve directory output, protect logical paths, prepare the complete schema envelope, or permit a memory ack.
Those reservations must exist before resident publication. The native resident modes remain refused.
No disk format, encryption, atom identity, record history, backup, conversion, or primary configuration changes.

The [operational receipt](proofs/disk-journal-reservations-20261015/report.md) covers the quote, ordinary append, recovery, publication, and input limits.
