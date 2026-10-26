# cloud_backup_protocol

The crate supplies canonical cloud backup types to the client and service.
It performs no I/O and enables no runtime capability.
The [V2 contract](../docs/designs/cloud-byte-transport-v2.md) defines the active backup protocol.
The service consumes this source through its pinned LastDB submodule.
Do not duplicate the wire types in the service.

Shared recovery types and saved V1 state validators remain where V2 still uses them.
Those types do not enable a V1 capture, upload, restore, or scheduler.
Keep completion checks, retained bytes, and unresolved provider obligations until their exact saved state proves release is safe.

Builds, lint, and required benchmarks run in GitHub CI.
Tests are retired. Do not run local builds, clippy, benchmarks, or host build-lock helpers.
Local Rust formatting remains permitted:

```sh
cargo fmt -p cloud_backup_protocol --check
```
