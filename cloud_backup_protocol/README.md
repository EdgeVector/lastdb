# cloud_backup_protocol

Canonical version-one native cloud backup wire types, unreleased draft revision 3.
The [protocol specification](../docs/designs/native-cloud-backup-protocol-v1.md) defines the required authority and provider effects.

The crate contains strict scalars, byte limits, job results, object receipts, manifest and recovery types, and one accepted backup result.
Pure validators check local consistency and exact request/result linkage.
They do not verify provider bytes, account authority, durable effects, complete native inventories, or successful recovery.

The initial manifest read binds its grant to the accepted receipt ID through `ReadGrantResponse::manifest_index_grant_for`.
Private upload targets use the complete frozen transfer digest, so separate jobs keep separate physical allocations.

The dependency-light `lastdb_storage_policy` crate supplies native V1 sizes and shared source resource values.
The [resource policy](../docs/designs/native-backup-resource-policy-v1.md) separates fixed eight MiB native parts from editable buffer and metadata allowances.
These source values are not runtime memory settings.
Cloud objects remain at most 64 MiB, with at most 512 ordered extents per external ciphertext and complete manifest byte bounds.
The new disk-stream source path removes the former backup authentication size guards without a wire or encryption format change.

Runtime support remains disabled.
The native backup capture code, Mini importer, and service Lambda consume this source with runtime activation disabled.
Private worker records, provider locations, multipart uploads, external retention holds, and future sync types stay outside this wire contract.

The service consumes this exact package through its existing `fold` submodule in its S1 DEV worktree.
Do not duplicate its types in the service.
Compile the storage Lambda explicitly through its manifest path.
The [source status](../docs/designs/native-cloud-backup-wave-two.md) records the current boundaries and remaining work.

Allowed local checks:

```sh
cargo fmt -p cloud_backup_protocol --check
scripts/ci/with-fold-host-cargo-lock.sh --timeout 60 -- cargo build -p cloud_backup_protocol --lib
scripts/ci/with-fold-host-cargo-lock.sh --timeout 60 -- cargo clippy -p cloud_backup_protocol --lib -- -D warnings
```
