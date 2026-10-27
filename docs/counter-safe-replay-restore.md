# Counter safety during restore

Normal writes change records and counts in one native batch.
Restore must preserve that rule before cleanup can delete data.
The local mutation replay and its raw lifecycle entries are removed.

## Restore

A snapshot restore requires an inactive native target.
A serving process refuses it before any data change.
This preserves the immutable replay floor and the logical memory state.
The restore publishes its blocked state under the existing preparation gate.
It releases that gate so the disk owner can flush each bounded batch.
Normal writes and cleanup refuse the blocked state.
Reads refuse an unverified root.
The restore keeps its existing bounded durable batches.

A logical snapshot restore flushes a pending marker before it clears or replaces records.
The marker stays after failure or cancellation.
Startup refuses an interrupted logical restore, even if the remaining counts agree.
Matching counts cannot establish that every snapshot namespace arrived.
Repeat the complete restore to finish it.

Native file restore marks its inactive target for counter validation.
The first logical open validates that target before it starts cleanup workers.
An invalid target remains blocked.
Startup refuses an unreadable marker, a marker link, or a marker that exceeds its byte limit.
Keep an incomplete restore under a program that supports this validation marker.

Validation discovers scopes from raw keys and reads their values through the correct encryption reader.
It refuses a protected record that the encryption reader discards.
It checks atom references, body/counter pairs, molecule counts, bindings, retained conflicts, and file holds.
It counts file references across every restored scope before it compares the global file counts.
The logical namespace reader also checks that each referenced file exists.
It changes no count.
The pending marker goes only after complete validation and a durable barrier.

The existing verifier has bounded totals.
If atoms, molecules, or retained-record identities exceed those totals, restore refuses incomplete validation.
A zero-fault partial result cannot authorize cleanup.
Missing or malformed molecule count fields remain unknown.

Normal writes need one memory flag check for this restore guard.
They do not recount other keys.
The complete validation runs only for a restore target.
