# Native memory and disk quality review

Date: 2026-10-20.

The review covers the current warm set, read ownership, counters, delete keys, and protein bindings.
It also checks the ordinary query routes and the disk journal.

## Changes

- Charge the metadata that each molecule key retains. Schema aliases share one charge for the same molecule key.
- Replace that charge on an update. Release it when the key becomes absent or leaves memory.
- Read each counter and its related records from one captured state.
- Use the actual key for delete selection and cutoffs. Display text does not identify a key.
- Use an unambiguous tuple for a protein binding identity.
- Remove the obsolete generic blob diagnostic wrapper. Refuse unsupported blob batch requests explicitly.
- Release saved call values before the purge. Restore the byte limit after the last call releases its records, when clean records permit it.

The purge still protects other active calls and writes before flush.
A held read can exceed the warm set limit temporarily.
The warm set charge is an estimate. It is not a limit on total process memory.

## First attempt

Source: `37a36d037aa6c440dd7cbfb1f6d98a16163acaa5`.

The build and both Clippy passes succeed.
The ordinary requests return the correct values, metadata, and delete results.
The independent review finds one memory failure after the batch returns:

| State | Charged bytes | Limit | Held records | Dirty records |
|---|---:|---:|---:|---:|
| After the 32-key cold read | 3,635,648 | 262,144 | 0 | 0 |
| After the next key fetch | 261,728 | 262,144 | 0 | 0 |
| After the repeated 32-key read | 3,635,648 | 262,144 | 0 | 0 |

The first attempt is not accepted. A quiet node can retain too much clean data until another fetch.
The follow-up change fixes the release order and runs the purge at call release.

Original receipts and the independent failure report remain intact.
Large raw replies stay intact locally. The tracked gzip copies contain their exact bytes.
The supported cleanup removes both temporary database homes after six normal node stops.

## Validation boundaries

All database data is synthetic. Each run starts from a new, empty, owned home.
The primary and its archive remain unused. No migration or cloud activation occurs.
No tests, new executable check suite, or fixture harness are added or run.
The existing operational helpers remain unchanged. The request file contains ordinary owner API data.

The query check covers Hash, HashRange, exact keys, bounded ranges, metadata, repeated writes, deletes, normal reopen, and concurrent calls.
The counter and binding identity changes also receive independent source review.
No operation pauses at a selected concurrent instruction, and the unusual library-only field-name identities receive source review only.
These runs do not establish a throughput improvement or an operating-system cold-cache comparison.

## Accepted repeat

Source: `a0ccb6b505e42ce39c85f4bb27a1b90fed7f59ba`.
The executable hash starts with `d453d7c6`.
All 1,812 captured source inputs match the frozen commit.
All 1,573 repository dependencies appear in that capture before the build.
Both Clippy passes and the normal locked release build succeed.

| State | Charged bytes | Limit | Held records | Dirty records |
|---|---:|---:|---:|---:|
| After the 32-key cold read | 261,728 | 262,144 | 0 | 0 |
| After the next key fetch | 261,728 | 262,144 | 0 | 0 |
| After the repeated 32-key read | 261,728 | 262,144 | 0 | 0 |

The independent operation review passes:

- All 457 query responses match the expected keys, values, counts, and metadata.
- All 130 filename entries match the synthetic write data exactly.
- Both distinct delete keys disappear and remain absent after normal reopen.
- Atom and molecule diagnostics return the expected counts and body states.
- All 36 concurrent read actions return a consistent batch state.
- All 29 reads that require no file reads use zero file reads.
- All six node sessions stop normally.

The `accepted` directory contains the revised release and the repeat-run evidence.
The first failed attempt remains separate.
The disjoint main change `afbd0dc9b` affects scoped snapshot restore only; these operations do not check that feature.
