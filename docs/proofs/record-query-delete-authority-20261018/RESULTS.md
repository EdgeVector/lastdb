# Current record queries and related deletes

## Changes

| Review comment | Change |
|---|---|
| Exact reads skip field conditions and limits. | Controlled exact reads use the common query route. |
| Range conditions use old field values after record compaction. | Resolve current document values before conditions, order, and limits. |
| A current value retains metadata from an older atom. | Replace the complete field result. Unavailable per-field metadata stays unknown. |
| Delete chooses related keys before an update changes them. | Resolve current related keys inside the native write turn, then commit all affected schemas together. |

The read route resolves each key in its actual storage scope.
A complete document supplies its current fields. A partial document preserves omitted field values from their field molecules.
Candidate selection retains the existing fallback for related molecules with no own key-field tips.
Raw field reads remain available for record compaction.

Both user Delete forms check the original caller's conditions before peer expansion.
Compacted records supply current peer-key values and conditional-delete values.
Derived peers do not inherit the original caller's expected-value or must-exist requirement.
Every affected schema retains its cleanup steps. Search receives the deletion after the full disk commit.
Only the original caller's mutation IDs return.

Fixed read, operation, and byte limits return an explicit refusal.
The native write scope retains a typed fixed-limit failure until it discards all private work.
This prevents an oversized peer deletion from an endless lane retry or a partial publication.
Temporary queue and memory pressure retains its existing retry behavior.

## Boundaries

Ordinary Delete retains its existing per-field clock order.
A lower Delete can remove older fields while a newer update survives on other fields.
The synthetic mixed cohort therefore classifies the changed key field separately from full-record projections.
It preserves every outcome and does not retry to obtain a preferred winner.

A mixed update and delete request still has separate phases.
An update can succeed before a later conditional delete receives a refusal.
The explicit Purge route retains its separate destructive rules.

Cloud activation remains disabled.
Before cloud returns, partial mixed requests and rejected deletes need capture selection for each accepted intent.
This change does not alter the existing capture marker or claim cloud replay correctness.
The primary database and its archive remain untouched. No migration code forms part of this change.

## Validation

The frozen product source is `8c26bd4d41cbc1cd6921cac2702646241f0ddb24`.
The source inventory captures 1,885 inputs before the release build.
Workspace Clippy, narrow core/node Clippy, format, file size, function size, new-allow, and write-path checks pass.
The source removes one argument-count suppression and adds none.
Independent source review finds no remaining blocker in the changed read and delete boundaries.

Release and ordinary API results follow before acceptance.
The request inputs are committed in `operation-inputs.json`.
No tests, coverage commands, primary operations, or existing-home loads run.
