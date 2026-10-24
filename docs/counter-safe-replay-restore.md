# Counter safety during replay and restore

Normal writes change records and counts in one native batch.
Replay and restore must preserve that rule before cleanup can delete data.

## Replay

Complete mutation envelopes use the normal FoldDB mutation writer.
That writer updates tips, retained conflicts, atoms, molecule counts, and file holds together.

Raw lifecycle entries lack the complete mutation data that this writer requires.
Replay refuses those entries before their first effect.
The refusal covers single operations, batches, and mixed physical changes.
It leaves the replay cursor before the refused entry.
A current snapshot supplies the recovery route.

Opaque metadata keeps its existing replay path.
The legacy raw tip, header, and conflict writers are removed.
Cloud activation stays disabled.

## Restore

A native restore owns the existing preparation gate for its complete operation.
Normal writes and cleanup wait while that owner works.
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
Keep an incomplete restore under a program that supports this validation marker.

Validation discovers scopes from raw keys and reads their values through the correct encryption reader.
It refuses a protected record that the encryption reader discards.
It checks atom references, body/counter pairs, molecule counts, bindings, retained conflicts, and file holds.
It changes no count.
The pending marker goes only after complete validation and a durable barrier.

The existing verifier has bounded totals.
If atoms, molecules, or retained-record identities exceed those totals, restore refuses incomplete validation.
A zero-fault partial result cannot authorize cleanup.
Missing or malformed molecule count fields remain unknown.

Normal writes need one memory flag check for this restore guard.
They do not recount other keys.
The complete validation runs only for a restore target.
