# cloud_backup_protocol

The crate supplies canonical cloud backup types to the client and service.
It performs no I/O and enables no runtime capability.
The `src/v2` module defines the active backup protocol.
The service consumes this source through its pinned LastDB submodule.
Do not duplicate the wire types in the service.

The `src/shared` module defines shared snapshot and per-writer log commitments.
The service adapter supplies authenticated ownership, verified immutable objects,
and conditional metadata updates. The node verifies and interprets sealed data.

The crate root holds the scalar, recovery and limit types that `src/v2` uses.
Keep completion checks, retained bytes, and unresolved provider obligations until their exact saved state proves release is safe.

Builds, lint, and required benchmarks run in GitHub CI.
Tests are retired. Do not run local builds, clippy, benchmarks, or host build-lock helpers.
Local Rust formatting remains permitted:

```sh
cargo fmt -p cloud_backup_protocol --check
```
