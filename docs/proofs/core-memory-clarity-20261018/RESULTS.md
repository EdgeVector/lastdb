# Core memory clarity review

## Result

The current logical engine uses one core model for memory and native packed storage for disk.
The review finds no live alternate logical engine in the inspected constructors, query routes, or mutation routes.
This change removes obsolete alternatives and corrects the normal status sampler.
It does not replace the core model or add another value store.

## Findings and changes

| Finding | Change | Result |
|---|---|---|
| Logical guards accept a native-engine switch even though every database constructor requires that engine. | Require the logical layout, filter, and current-read rules directly. | Hash and HashRange rules no longer have an obsolete bypass. |
| Field scopes can create an empty identity and skip their scope on an older engine. | Remove the empty scope and its mode argument. | Logical field reads always use their canonical identity. |
| Query callers can skip the captured read scope when the engine is absent. | Centralize required capture in `DbOperations::read_scope`. | Queries, counts, field reads, receiver setup, and query batches require the same core/disk boundary. |
| The old graph metrics retain counters and producer methods with no callers. | Replace them with three live background-work measurements. | Logical counts come from the core, without obsolete counter state. |
| Normal status reads unwritten record counters and reports zero while the core contains records. | Read count, limit, held, dirty, purge, tip-only, and lookup measurements directly from native status. | Status describes the actual warm set. |
| Persist envelopes carry an always-empty revision list. | Remove the list, its type, its public exports, and empty diagnostic output. | Background jobs keep only their actual payload and completion controls. |
| A separate enqueue API has no callers. | Remove it and keep reservation followed by fill. | Live queue admission follows one route. |
| Tip admission builds body coordinates before it knows whether they are needed. | Delay those allocations until the relevant branch. | Existing values keep their real source; tip-only admission avoids unnecessary copies. |
| The atom-store constructor also returns unrelated background metrics and suppresses an argument-count lint. | Obtain metrics from the validated runtime and group the three atom namespace handles. | The atom constructor returns the atom store, with no lint suppression. |
| The design describes molecule-wide invalidation for a recognized per-key generation delete. | Describe the current exact-key invalidation and separate structural invalidation. | The document matches the source. |

The Rust source change removes 664 net lines.
The file and function size checks pass.
It removes one lint suppression and adds none.

## Intentional boundaries

The physical store interfaces retain their low-level optional native scope.
Standalone physical atom tools can use those interfaces without constructing a logical database.
The required logical read route cannot select that scope-free alternative.

Current and captured reads remain separate because a call can retain an earlier value while another call writes.
Their immutable owners keep the corresponding memory charges.
They do not provide a history query or another warm set.

Selected-tip receipts retain the winning physical copy and its source controls for one call.
They are not a second reusable tips store.
The core remains the owner of reusable tips and values.

The complete key part establishes range membership.
The sparse warm records establish known tips, absence, and values for selected keys.
Current encoded key forms and physical body fallbacks remain because they identify valid records and protein copies.
The review does not remove them based only on an old comment or name.

Background persist lanes now describe purge work.
The native disk queue retains the prepared output of accepted logical changes.
Ordinary reads cannot inspect that queue.
The removal of the unused envelope revision list does not change queue order, memory reservations, or completion notifications.

Historical status field names remain at the wire boundary for installed clients.
The native sampler leaves retired gauges unavailable and retired integer fields at their existing defaults.
No retired counter producer or old engine attaches to those fields.
New core lookup counters explicitly count internal lookups, rather than whole queries.

## Validation

The product source is commit `09269a911cc24e44dded59d014b4f09cedc3d20b`.
The source inventory captures 1,882 inputs before the release build.
The normal workspace and off-lane Clippy checks pass with warnings denied.
Format, file size, function size, new-allow, source whitespace, and boundary checks pass.
Independent source review finds no blocker in the changed guards, captures, queue fields, metrics, or status lock order.

The normal release build passes with default features.
Its SHA256 is `21a94718204504f9728b044c11cf9bbe91595d1eb1d2042a0aea546ccd2fe6be`.
Independent comparison confirms that all 1,882 captured product inputs match the committed source.

The frozen ordinary API workflows pass 100 keyed value checks across eight normal node sessions.
All 48 required memory-only reads make zero file-read calls.
The checks include batch changes, partial updates, delete and reinsert, shared molecules, normal reopen, and unsupported-route refusals.
The default memory acknowledgement occurs before disk completion, with complete dirty core records visible to the first read.
The raw receipts retain exact values and operation counts; their seal records the file sizes and hashes.
These operations use fresh generated data. They do not establish throughput or primary behavior.

Normal status returns the sampler's saved snapshot, normally refreshed every 60 seconds.
Native status returns current counters. An immediate comparison against the boot snapshot is not an equal-time comparison.
The supplemental status comparison waits for a sample after the warm reads.
The fresh sample matches all eight counters against unchanged native reads before and after the status call.
It reports 34 logical records, 30 tip-only records, 18 lookup hits, and the 10,000-record limit.
Held, dirty, purged, and missed counts are zero in that sample.

Two earlier status attempts remain diagnostic failures in separate receipt directories.
The first observes later accepted work after an explicit flush target.
The second compares the boot snapshot with current counters before the next sample.
The final run waits for quiescence and for a sample after the warm reads; it does not alter the product or its sampler interval.

The supported exact-home cleanup removes the owned synthetic databases after receipt preservation and independent review.
The receipt directories retain the results, source identities, normal exit records, and cleanup outcomes.
No test or coverage commands run.
The primary database and its archive remain untouched.
Cloud activation, migration, and shared CI configuration remain unchanged.
