# Canonical vocabulary rules

The schema service enforces one field policy for every caller.
No special vocabulary key, operator signature, or vocabulary write endpoint exists.
New fields enter through normal schema publication or field declaration.
The existing app identity, permission, mutation, and quota checks remain active.

## Field validation

A canonical name identifies the subject and its property.
Use lowercase words separated by underscores.
Spell out app abbreviations and key roles.
Names such as `at`, `size`, `klh_h`, and `generic_value` fail.
A name such as `file_size_bytes` states the subject, property, and unit.

A new field requires a specific description, an explicit type, and a positive identity version.
The description contains at least three words and at most 4096 bytes.
An existing name retains its description, type, and version.
A different definition requires a different precise name.
The name needs a meaningful word in its description or name/description embedding similarity of at least 0.3.
The service assigns the name policy version as validation metadata.
That version grants no caller permission.

The complete candidate batch passes validation before its first field write.
Normalized text checks reject duplicate descriptions under different names.
A conditional storage write repeats this check against current durable definitions.
This catches a conflicting concurrent request.

Name token overlap and description token overlap select possible semantic duplicates.
The service compares their description embeddings when vectors are available.
A cosine score of at least 0.96 requires concept reuse or a distinct definition.
Type and qualifier checks keep different units, time roles, and key roles separate.
The response names the existing concept; the service does not silently rename a new field.
Generic text container checks also run for existing names and seed fields.

Model vectors use a process cache.
When the model is unavailable, deterministic name, description, identity, and duplicate checks still run.
The service infers no semantic equivalence without vector evidence.
A name without lexical support fails when the model cannot confirm its meaning.
Similarity scores have limits; precise names and immutable typed definitions remain required.

## Read and restore

`GET /v1/fields/vocabulary` returns the current definitions and policy version.
`schema_service/scripts/canonical-vocabulary --url <service-url> get` reads that endpoint.
The command has no publication action and needs no secret.
Apps keep local names through explicit field maps to registered canonical names.
Declaration handles continue to control shared field access.

Seeds, normal declarations, and snapshot imports use the same admission policy.
An import also checks its schema references, declaration references, release references, and embedding references.
The importer validates the whole snapshot before it changes the active definitions.
Resolver artifacts retain the current format and policy checks.

## Rollout

The DEV definition generation remains `canonical-v1/` with its separate embedding store.
Old definitions stay available for recovery.
App identities, permissions, credentials, and quotas keep their common stores.
The primary home, archive, atoms, files, and daemon stay outside this rollout.

Use real isolated DEV app operations before the separate production phase.
Keep the normal deployment gates, canary period, alarms, and rollback definitions.
No local data remap follows from a canonical name change.
