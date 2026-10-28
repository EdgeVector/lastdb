# Canonical field names and common admission

Tom requires precise canonical names across domains.
The schema service enforces this rule in code for every caller.
The operator approval key and vocabulary publication endpoint are removed.
A normal schema or field declaration can introduce a valid new concept.

## Name and identity

A name identifies the actual subject and property.
Include units and representation details when they affect meaning.
Use `file_size_bytes` instead of `size`.
Spell out app abbreviations and key roles.
A prefix alone does not make a vague property precise.

A new field supplies its description, explicit type, and positive identity version.
One name retains one definition.
A conflicting name fails before a field or schema write.
The name needs lexical support from its description or an embedding score of at least 0.3 for that description.
Field identity still includes the name, description, type, and version.
The policy version records code validation and grants no authority.

## Admission sequence

1. Validate the request and its ordinary identity, permission, mutation, and quota rules.
2. Validate every field name, description, explicit type, and identity version.
3. Compare normalized descriptions with the current vocabulary and the rest of the candidate batch.
4. Compare likely duplicates through name tokens, description tokens, and description embeddings.
5. Preserve different units and roles; require reuse when the candidate duplicates an existing concept.
6. After logical schema rejection checks, write validated definitions at schema persistence and make them visible in memory.

The semantic candidate needs name token overlap of at least 0.5 and description token overlap of at least 0.35.
A type match and compatible qualifiers are also required.
A description cosine score of at least 0.96 requires reuse or a distinct definition.
The result identifies the existing concept and does not silently rename the field.
Exact normalized description duplicates fail even when no embedding model is available.
The backend repeats that deterministic check against current durable definitions to cover concurrent requests.

Generic text container checks have no approved-name exception.
The same code checks seeds, ordinary declarations, and imported field definitions.
Read-only proposal validation does not create canonical fields.
A failed request cannot use a special key or a policy marker to bypass validation.

## App behavior

An app can keep local command names through explicit maps to precise canonical names.
A new canonical identity does not move an existing record.
Existing-home data conversion remains a separate operation.
The current app manifests remain source inputs to ordinary declarations.
They require no signed publication bundle.

## Storage and rollout

The reset uses the existing `canonical-v1/` definition generation and its separate embedding store.
App identities, permissions, credentials, and quotas remain in their common stores.
Old definitions remain available for recovery and rollback.
The primary home and its archive remain outside the operation.
The DEV phase precedes the separately reviewed production phase and its normal canary gates.

See [the seed manifest](canonical_field_seed_manifest.md) and [the common field policy](canonical_vocabulary_operations.md).
