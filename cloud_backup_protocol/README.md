# cloud_backup_protocol

Canonical version-one native cloud backup wire types, unreleased draft revision 2.
The [protocol specification](../docs/designs/native-cloud-backup-protocol-v1.md) defines the required authority and provider effects.

The crate contains strict scalars, byte limits, job results, object receipts, manifest and recovery types, and one accepted backup result.
Pure validators check local consistency and exact request/result linkage.
They do not verify provider bytes, account authority, durable effects, complete native inventories, or successful recovery.

Runtime support remains disabled.
No production crate imports this package at stage one.
Private worker records, provider locations, multipart uploads, external retention holds, and future sync types stay outside this wire contract.

The service later consumes this exact package through its existing `fold` submodule after the canonical source merges.
Do not duplicate its types in the service.
Compile the eventual storage Lambda explicitly through its manifest path.

Allowed local checks:

```sh
cargo fmt -p cloud_backup_protocol --check
scripts/ci/with-fold-host-cargo-lock.sh --timeout 60 -- cargo build -p cloud_backup_protocol --lib
scripts/ci/with-fold-host-cargo-lock.sh --timeout 60 -- cargo clippy -p cloud_backup_protocol --lib -- -D warnings
```
