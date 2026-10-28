# Canonical schema properties

Tom requested these scoped tests on 2026-10-28.
The general test retirement remains in force outside this target.
The initial source under review is `04a5a906c48b40028fa57b6c5b1bc94e19bc4407`.
That revision is the DEV schema-service pin and the head of LastDB PR 622.

## Run boundary

Run the target in GitHub Actions:

```sh
cargo test --locked -p schema_service_core --test canonical_properties -- --test-threads=1 --nocapture
```

The explicit Cargo target leaves automatic test discovery and library tests disabled.
It adds no dependency or model download.
The suite uses the real schema-service APIs and the production memory persistence adapter.
Separate service states share one isolated memory backend for the concurrency cases.
The cases use fixed finite inputs and explicit thread barriers.
They need no primary home, service URL, app credential, or AWS credential.

The test process runs one case at a time because the anti-field vector cache is process-global.
Each concurrency case creates its own bounded set of threads.
The CI command has a 20-minute limit; the job has a 25-minute limit.
The CI artifact records the source revision, complete result, and checksums for seven days.
The CI summary links the artifact and the run.

## Meaning and data identity

A canonical field hash uses its name, description, type, and identity version.
The name and description receive trim normalization. Case remains significant.
A matching name alone cannot establish semantic equivalence.

A declared identity uses its declaration handle identity, name, type, and version.
The owner and description do not form part of that hash.
Separate declarations of one concept must keep separate data identities.
The declaration permission check determines which app can reference an existing declaration.

Schema identity uses the owner, readable schema name, field-name set, and key layout.
It does not include every field definition or declaration reference.
The identity tests state these rules explicitly; they do not infer them from field names.

## Properties and limits

| Area | Automated check | Limit |
| --- | --- | --- |
| Canonical identity | Generated name, meaning, type, version, and trim combinations | A finite set cannot prove absence of a hash collision. |
| Declaration identity | Generated declaration and field combinations; separate apps and permissions | Does not prove node molecule or protein state. |
| Schema identity | Field permutations, owner, key layouts, and algorithm downgrade | Includes an adversarial raw-definition delimiter case. |
| Vocabulary admission | Valid batches, exact conflicts, normalized duplicates, units, roles, invalid batch rollback | Disabled embeddings cannot measure semantic model quality. |
| Concurrency | Conflicting names and 16 declarations through independent service states | Memory locks do not prove S3 conditional-write behavior. |
| Semantic consistency | Same injected vectors through warm and stale caches | The vectors exercise admission control, not language understanding. |
| Schema publication | Repost, field order, definition conflict, supplied field hash | Does not execute HTTP identity, quota, or proof-of-work gates. |
| Reload and import | Reload consistency and invalid snapshot rejection | Memory reload is not a process crash or disk recovery. |

Safety assertions remain active when source review predicts a failure.
No ignored test or expected-failure wrapper converts an invariant violation into success.
A failure needs a decision about the rule or a product correction before the suite can pass.

## Separate operational work

The following checks need a separate execution surface:

1. S3 conflicts, bounded retry exhaustion, timeout-after-commit, and fresh Lambda cache behavior.
2. Failure after each expansion commit and concurrent disjoint schema expansions.
3. Snapshot import failure after backend clear and midway through later writes.
4. A new node key layout that joins after records already exist.
5. Protein membership, both pointers, counters, and atom identity after a reopen.
6. Concurrent writes, rekey, delete, and replay through each protein member.
7. Joins between molecules that already belong to separate proteins.
8. A canonical-name or version change with an explicit existing-data conversion.
9. Actual FastEmbed paraphrases, distinct units and roles, model failure, and version changes.

Use fresh isolated homes for node operations. Never copy the primary or open its archive.
Service tests cannot establish a migration of existing data.
Definition generation changes do not move existing atoms or files.
Shared DEV load, cloud fault checks, deployment, and production activation require separate approval.
