# Native core cutover review

The [final source identity](candidate-final-identity.json) identifies the product source and release.
The [final committed-source check](committed-final-source-verification.json) matches all 1,883 captured inputs to that exact Git tree.
Commit `b391b080` records the first cleanup. Its release remains unrun discovery evidence.
This report describes source behavior. The final results report records the separate ordinary API checks.

## Current model

| Owner | Responsibility |
|---|---|
| `CoreWarmSet` | Current schema and molecule definitions, required controls, sparse tips, confirmed absence, and full atom values. |
| Complete selected key domains | Complete candidate keys for selected HashRange schemas. Values remain partial. |
| Recency list | References to logical records. It selects clean records for purge and contains no second value copy. |
| Captured core state and disk root | One call's consistent values and file lifetime. Earlier values retain their memory reservations. |
| Private native write queue | Prepared file bytes, resources, and completion work until flush. Ordinary reads cannot inspect it. |
| `ResidentMetrics` | Shared counters. The old graph wrapper and its values are removed. |

The normal warm limit is 10,000 logical records.
Core metadata and typed definitions each have a separate 16 MiB allowance.
Selected complete keys have a default 128 MiB allowance.
The normal derived write allowance has a 128 MiB ceiling. An explicit override can change that limit.
Clean read values use the record cap, rather than a separate global body-byte cap.
These limits do not define a whole-process memory maximum.

## Source changes

- Normal startup and direct FoldDB constructors open native packed storage.
- Logical constructors reject other physical engines before namespace access.
- Empty homes initialize directly. Older nonempty formats receive an error, with no automatic conversion.
- Native memory acknowledgement requires no mode flag. Explicit durability and resource refusal still require disk completion.
- The native flush delay must stay between 1 and 500 ms.
- Hash and HashRange logical layouts remain supported. Standalone Range, Single, unsupported selectors, and historical queries receive explicit errors.
- Cloud startup, activation, enrollment, resume, and config activation refuse before their effects.
- The native journal supports recovery. The local capture router remains without an attached cloud engine.

The old graph, logical cache, write publication branch, disabled range helpers, and dummy worker are removed.
The old mode controls, inactive write threshold, conversion tools, and mode-comparison programs are also removed.
The schema atom markers now serve direct reads. A missing old sentinel no longer starts a full atom backfill.

## Correctness details

An empty internal transaction creates no new write sequence.
It still waits for an explicit durability barrier when required.
Duplicate mutation checks occur once under the native write gate.
Held core values keep their reservation owners after flush.

Public processor and field query helpers use the same supported-layout, filter, and current-value checks.
They also capture one core and disk state when no parent scope exists.
Nested calls reuse the parent root. A recorded native read failure remains an error.
This closes the public helper path that could otherwise bypass the normal query executor.

The purge guard reads actual current core heads.
It excludes only the exact scoped slots that the purge removes.
Another current slot can still protect the same atom.
The guard uses each molecule's key codec and fails before destructive work when the key cannot encode.

Keyed atom reads require encrypted content.
Logical text that starts with an encryption marker still receives encryption as user data.
No plaintext migration override or reseal helper remains.
Molecule key wraps also require an encryption marker before the key bundle can open.
The generic offline encryption helper keeps its separate format contract.

Current flat and compound key layouts still require a prefix union.
The physical adapter owns that union once.
Exact typed reads use the requested key first and resolve a current flat alias only after a miss.
Those aliases do not select an older engine or an older collection.

## Limits

Raw physical tools and vendor code remain separate from the logical engine.
Older status fields remain in the response format. They do not report a second active cache.
Cloud transfer and local replay stay disabled for this round.
The primary and its archive stay untouched.

True cached duplicate identities and the internal empty-transaction path receive source review only.
The public mutation route creates a fresh author clock and rejects an empty public batch.
The old-format startup refusal also receives source review only.
Direct embedded helper entry and malformed key-wrap refusal receive source review, rather than a private byte-injection program.
No new performance comparison forms part of this cleanup.
The prior V6 benchmark covers its earlier source, not this cutover.
