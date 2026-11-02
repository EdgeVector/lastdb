# Accepted cloud backup retention

Status: implementation contract. Provider deletion remains disabled.
The TTL recovery operation and native CAS recovery have separate PRs.
This document does not claim that cloud physical cleanup works now.

## Completion rule

Cloud cleanup completes for one frozen request only when all these facts hold:

1. The owner selects the recovery points to keep under the accepted retention policy.
2. Every current stream head remains in that selection.
3. The complete recovery dependency closure remains available.
4. Active restores, uploads, verification jobs, and uncertain provider obligations retain their files.
5. Every eligible provider object is absent at its exact saved provider identity and version.
6. The allocation ledger releases each confirmed deletion exactly once.
7. The service saves a terminal receipt for that exact request, directory, and plan.

Local blob counters and TTL cannot authorize cloud object deletion.
Old accepted snapshots can still need bytes that the local node no longer needs.
Cleanup cannot promise removal from a backup that the owner keeps.
It also cannot promise erasure of an independent exported copy.

## Recovery dependencies

Publication order and recovery dependencies serve separate purposes.
The existing `predecessor` controls conditional publication order.
It does not say whether a later full snapshot needs an older backup.

The optional `Begin.recovery_dependencies` becomes part of the existing begin and commit hashes.
An independent full snapshot declares an empty list.
A mutation-log checkpoint declares its base snapshot and previous checkpoint, once each, in ordinal order.
The client authenticates that declaration against its encrypted recovery root.
The service verifies each named accepted identity in the same account, generation, and stream.
Each dependency has an earlier ordinal.

Omission means legacy behavior, not an independent snapshot.
Legacy backups conservatively depend on their publication predecessor.
No old hash changes because the absent field remains absent on serialization.
No current client emits the new field before service capability admission.
An older service rejects the unsupported field instead of ignoring it.

The pure `recovery_closure` operation classifies the frozen accepted directory.
It checks exact identities, complete dependencies, resource bounds, and all selected roots.
It returns a deterministic directory digest, retained points, and candidate points.
It grants no provider deletion authority.

The owner can inspect one exact accepted point with this command:

```text
lastdb --data-dir <credentials-home> cloud backup-v2 inspect-dependencies --recovery-phrase-stdin --stream <uuid> --generation <n> --commit <uuid> --commit-sha256 <sha256> --json
```

The phrase enters protected stdin through the existing recovery command path.
The command opens no node, requests no provider URL, and changes no accepted backup.
It follows exact accepted dependencies through the real service with four concurrent requests and explicit count, byte, and time limits.
It checks every accepted hash and exact predecessor identity.
Its result describes one recovery closure, not a complete frozen account directory.
It explicitly reports that provider deletion lacks authorization.
The protected DEV operation inspects its actual accepted log and later snapshot before their source-free restores.
Those results can verify legacy wire hashes with real accepted data, without a synthetic fixture.

## Freeze and plan

The service saves a request ID, expected account generation, and the owner selection.
It then fences new cloud effects through a scope-level retention revision.
Every begin, transfer grant, verification, metadata write, and acceptance transaction checks that revision.
The plan includes every stream and its exact head under that fence.
An omitted page, active admission, changed revision, or unknown obligation blocks the plan.
Local writes continue under the existing bounded journal rules.

The service closes the union of the owner selection, current heads, and restore holds through recovery dependencies.
It verifies the complete transport inventory of each retained point.
It marks the exact saved provider bindings for their root, index, pages, and file objects.
It also marks bindings that active jobs or uncertain allocations own.
Repeated receipts for one physical allocation count as one allocation.
Any missing, malformed, incomplete, or conflicting inventory blocks deletion.

Only an allocation outside the complete marked set can enter the deletion plan.
The plan binds its scope, generation, directory digest, selection, candidate backups, and exact provider allocations.
The owner reviews the byte totals and recovery points before the first destructive effect.
The default remains retention until the owner selects retirement, unless Tom chooses another policy.

## Read holds and provider effects

Read grants currently have an expiry but no durable restore lease.
The service must save a bounded restore hold before it issues a read grant.
The client renews that hold before its expiry and releases it after its complete restore.
The retirement fence and hold acquisition use the same conditional revision.
An expired restore reacquires authority before another provider read.
The rollout waits for earlier grants to expire before it permits destructive retention work.

A transfer grant can leave an uncertain provider effect.
A closed job or an expired URL alone does not prove that its allocation is absent.
The worker preserves that obligation until exact provider reconciliation resolves it.
It never uses bucket deletion, account deletion, or a quota increase as cleanup.

Before deletion, the worker saves an intent for the exact resource, target, object key, and provider version.
R2 removal uses the frozen allocation identity. B2 removal names the exact saved file version.
The worker verifies absence after each effect.
An unknown or failed response remains retryable under the same intent.
It never retargets an object after provider configuration changes.

Only confirmed absence permits an atomic allocation-state change and quota decrement.
A repeated response cannot release bytes twice.
A conflicting provider version or unknown actual length stops reconciliation.
Accepted retirement markers remain available so an old restore selection returns a clear retired result.
It cannot silently select a newer backup.

## Delivery and operational evidence

1. Merge the hash-bound dependency contract with legacy behavior unchanged.
2. Add service capability admission, the retention fence, dependency validation, restore holds, and saved deletion reconciliation.
3. Add owner plan/status/commit controls and capability-gated client declarations.
4. Obtain separate deployment approval because an exemem-infra main merge deploys the service.
5. Use the protected DEV account and real providers for the complete operation.

The DEV operation keeps the pinned CI base and every unrelated recovery point.
It creates its own snapshot, TTL deletion, log checkpoint, and later locally cleaned snapshot.
It verifies both old and new selected restores before retirement.
An active restore or upload must block deletion of its objects.
A shared file must remain exact after another backup retires.
An exclusively retired object must become absent at its exact saved version.
Restart and repeated requests must retain the same plan and release quota once.
The final kept snapshot plus its later log must restore the live values and preserve the TTL deletion after two boots.

Normal source checks and a pure graph calculation do not prove these provider effects.
The cloud gap remains open until the protected DEV operation and owner gates pass.
