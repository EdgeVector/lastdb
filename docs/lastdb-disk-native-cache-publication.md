# Native schema cache publication

The native writer keeps a complete storage job under one write gate.
The job includes the schema reload and its protein writes.
This change keeps the reload cache private until the complete disk root succeeds.

## Current problem and change

The old scope cloned every stored body into a second complete operation list before disk commit.
The new scope takes the owned operation list and moves each uniquely held body into the journal mutation.
A record still held by another call uses a copy.
This removes the unconditional complete-body copy. It does not establish a process memory-size bound.

The old schema reload also changed the shared schema cache and protein memo before the complete root reached disk.
A later storage error could discard the pending writes but retain those cache changes.
The new scope stores private metadata values and defers the shared cache changes.

Schema reads in the same write scope use the private schema and state.
Protein discovery uses the latest private schema for each name and the other committed schema definitions.
The protein memo follows the same rule.
The durable schema and protein writes stay inside the complete storage batch.
They do not move into a second disk transaction.

## Completion order

1. Hold the native write gate and capture the current complete root.
2. Prepare the storage writes and private metadata.
3. Take ownership of the complete operation list.
4. Publish the complete journal and directory root.
5. Publish the final private schema, state, and protein memo values.
6. Release the write gate and return success.

The blocking disk task owns the gate, bytes, and deferred cache actions after submission.
Caller cancellation cannot release those objects while that task continues.
An error before submission discards the private metadata.
A disk error discards the deferred cache actions.
A cache publication error after disk success closes native reads until recovery.
The same cache key publishes only its final value within one scope.
The scope refuses more than 4,096 metadata publications.

Legacy stores keep their immediate schema cache behavior.
A failed native reload leaves the previous shared cache in place because its pending disk writes also leave the scope.

## Limits

This change covers schema reload cache values and the protein memo.
It does not make every schema administration action transactional.
It does not establish one versioned schema cache for every held query root.
Those read-root and complete admission gates remain open work.

The journal quota remains separate from the filesystem capacity and protected logical-record capacity.
Native resident-first reads and memory-first acknowledgements remain refused.
Cloud-connected native homes remain refused.
The default backend, primary, archive, and shared systems remain unchanged.
