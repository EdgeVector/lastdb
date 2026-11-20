# cloud_backup_protocol

Scalar and wire helpers shared by LastDB clients. Prefer the cloud API v3 design:

- [`docs/designs/cloud-api.md`](../docs/designs/cloud-api.md)
- [`docs/designs/relay-protocol.md`](../docs/designs/relay-protocol.md)

ExeMem account/billing paths may still use types in this crate. Grant
authorization and snapshots are **not** part of the live contract: grants live
only as signed chain entries; relays are grant-blind; there are no snapshots.

`src/v2/share_access` still holds legacy share-settings wire shapes used by
People-facing invitation code that has not yet moved. Do not add new ExeMem
grant side-channel calls. Do not restore snapshot or retention reclaim types.

Builds and required checks run in GitHub CI. Do not run local builds, clippy,
or benchmarks. Local formatting remains permitted:

```sh
cargo fmt -p cloud_backup_protocol --check
```
