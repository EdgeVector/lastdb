# cloud_backup_protocol

Canonical version-one native cloud backup wire types.
The [protocol specification](../docs/designs/native-cloud-backup-protocol-v1.md) defines the required authority and provider effects.

The crate contains strict scalars, byte limits, manifest and recovery types, transfer states, reference metadata, and head publication types.
Pure validators check local consistency and immutable transitions.
They do not verify provider bytes, account authority, durable effects, complete native inventories, or successful recovery.

All runtime capabilities remain disabled.
No production crate imports this package at stage one.
Logical sync types reserve identities only; they provide no replay implementation.

The service later consumes this exact package through its existing `fold` submodule after the canonical source merges.
Do not duplicate its types in the service.
Compile the eventual storage Lambda explicitly through its manifest path.

Allowed local checks:

```sh
cargo fmt -p cloud_backup_protocol --check
scripts/ci/with-fold-host-cargo-lock.sh --timeout 60 -- cargo build -p cloud_backup_protocol --lib
scripts/ci/with-fold-host-cargo-lock.sh --timeout 60 -- cargo clippy -p cloud_backup_protocol --lib -- -D warnings
```
