# Native cutover operational plan

The source review and native cutover are in progress. No new node or benchmark runs yet.
The next step requires the final source commit, release identity, and compiler checks.
The primary and its archive stay untouched.

## Product boundary

- Normal startup opens native packed storage without an engine-selection flag.
- Direct FoldDB constructors open the same format. Logical DbOperations refuse other stores before namespace access.
- Startup does not select, convert, or migrate old-format data.
- Native logical schema operations support Hash and HashRange.
- Unsupported range and pattern selectors fail before a generic molecule fallback.
- Cloud activation fails before an engine attach, worker start, remote claim, or cloud intent change.
- The native journal remains. The local capture router has no attached cloud engine.
- Cloud activation requires a later explicit change.

## Ordinary API operations

The new driver imports the unchanged generated-data drivers from `core-owned-writes-20261018`.
Every process receives a new owned synthetic home.
Every process removes `LASTDB_DISK_PACK`, `LASTDB_ENGINE`, `LASTDB_RESIDENT_MODE`, and the proof-engine choice.
The driver then requires native status and the normal native format marker.

| Workflow | Required result |
|---|---|
| Existing logical operations | Correct create, update, delete, separate recreate, mixed operations, range membership, concurrent complete values, and normal reopen. |
| Existing protein operations | Correct shared fields, alternate keys, member moves, deletion contract, and reopen. |
| Existing named database operations | Correct scoped values and independent personal values. |
| CAS supplement | A matching value permits the write. A stale value returns a conflict and preserves the current values. |
| Repeated public payload | Each call uses a fresh author clock and returns the correct values. This does not claim an idempotency hit. |
| Empty public batch | The route returns 400. The sampled drained write frontier stays unchanged. |
| Unsupported schema declarations | Single and standalone Range return explicit errors. |
| Unsupported filters | One query batch contains the unsupported range and pattern shapes. Every item returns an error. |
| Historical query and count | An RFC3339 `as_of` request returns 400 through direct-query and explicit keyed-count shapes. |
| Literal encryption prefixes | Normal creates preserve literal `ENC:`, `ENZ:`, and `ENB:` field strings through the first read and reopen. |
| Warm unique purge | A normal purge removes the key and returns 404 for the selected unique Atom UUIDs after ACK and reopen. This does not measure reclaimed disk bytes. |
| Cloud On | The route returns the disabled error. The known intent files stay unchanged. |

The first create status precedes the first target value read.
It must show `accepted_sequence > completed_sequence`, `accepted_batches > 0`, and `core_warm_dirty > 0`.
The default process has no engine, memory-mode, or proof replay choice.
Literal-prefix records use the normal JSON content codec. The driver removes the binary-content override.
The warm purge proves key absence and public UUID addressable absence after ACK and reopen.
The source review establishes the deletion path. These results do not prove reclaimed disk bytes or zero-file UUID reads.

True cached duplicates, mixed cached/new duplicates, and the internal empty transaction receive source review.
The public mutation request supplies no existing mutation clock or identity. A repeated request receives a new clock.
The public route rejects an empty mutation array before the core transaction.
The disabled replay engine stays disabled. No private fixture replaces that missing trigger.

## Identity and cleanup

`driver-identity-v5.json` names the exact new and reused driver inputs.
The final V5 identity is the only executable driver identity for this pass.
The earlier V1–V4 drafts never ran. Their draft identity files are removed.
V5 includes the default memory behavior, historical refusals, literal-prefix values, and warm unique purge.
Each run preserves the source identity, executable hash, requests, responses, status samples, and normal process stops.
The final review must separate source-only claims from actual API results.
Cleanup requires exact owned paths, no live process, preserved receipts, and the supported reclaim command.

## Benchmark scope

This pass has no new performance comparison.
The prior V6 results remain unchanged at product commit `7876211b6d64dff43ceddd602656017b74860fe9`.
Those results do not measure this native-default cutover or its source cleanup.
