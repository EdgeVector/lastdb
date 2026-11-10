# Accepted cloud backup retention

Status: active. Superseded recovery points are deleted immediately when a new commit is accepted on a stream.

## Immediate pruning on commit

When the service accepts a new commit on a backup stream (`commits/accept`), it immediately prunes superseded recovery points on that stream (`commits/prune`):

1. **Snapshot commits** (`recovery_dependencies: []`) declare `covered_logs` — a flat array sorted by `(user_id, device_id, stream_id)` naming the most recent commit (`through_commit_id`, `through_ordinal`, `through_sha256`) included from each covered `(user_id, device_id)` writer stream. Accepting an independent snapshot immediately prunes all superseded snapshots and all covered mutation-log commits with `ordinal <= through_ordinal` across every covered `(user_id, device_id)` stream.
2. **Mutation-log commits** (`recovery_dependencies: [base_snapshot, previous_log]`, `covered_logs: []`) retain their full active log chain (`Ln -> Ln-1 -> ... -> L1 -> S`) until an independent full snapshot supersedes them.
3. For every superseded point, the service deletes any R2 objects (hot database runs, mutation logs, inline files, and metadata pages/index/root) that are not referenced by a retained point, and removes the superseded `accepted#`, `submission#`, and `job#` records from `ExememCloudControl`.
4. Any pending cold blob releases (`blobs/release`) are deleted from B2 immediately on commit, and the account's R2 and B2 byte counters in `ExememBilling` are reconciled by prefix (`ListObjectsV2`).

## Recovery dependencies

Publication order and recovery dependencies serve separate purposes.
The existing `predecessor` controls conditional publication order.
It does not say whether a later full snapshot needs an older backup.

Every `Begin` carries `recovery_dependencies`, and the field is part of the begin and commit hashes.
An independent full snapshot declares an empty list.
A mutation-log checkpoint declares its base snapshot and previous checkpoint, once each, in ordinal order.
The client authenticates that declaration against its encrypted recovery root.
The service verifies each named accepted identity in the same account, generation, and stream.
Each dependency has an earlier ordinal.
The protocol has no form without the field: a job that omits it does not decode.

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
