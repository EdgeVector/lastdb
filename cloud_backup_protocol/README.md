# cloud_backup_protocol

The crate supplies canonical cloud backup types to the client and service.
It performs no I/O and enables no runtime capability.
The `src/v2` module defines the active backup protocol.
The service consumes this source through its pinned LastDB submodule.
Do not duplicate the wire types in the service.

The active backup and share transport is `src/v2`. The legacy shared HTTP
routes and their wire module are gone.

`src/v2/share_access` is the shared wire for grants and signed requests.
`NodeReadProof::digest` binds the database id, the HTTP action, the raw body, and a nonce.
The domain string is `lastdb:share:node-read-authority:v1`.
A revoked member stays on the signed member list until a later revision sets `regrant`.
The signing key is the node key. DEV and PROD use the same mutation-log paths.
`NodeReadProof::digest` hashes the raw body octets and the service realm.
Data routes use `authorize_request` once the database is native or committed.
Native create is only `POST /api/cloud/backups/v2/mutation-log/create-database`.
That request authorizes and creates the database in one transaction, with no migration evidence.
The LastDB client does not post `POST /api/cloud/backups/v2/mutation-log/cutover`.
The member-list read uses `member_list_read_view`.
A node-key revoke covers every device of that key. The body-less proof remains only while that database is open.
Snapshot and delivery-cursor types in that module name objects. They do not fetch bytes.

The crate root holds the scalar, recovery and limit types that `src/v2` uses.
Keep completion checks, retained bytes, and unresolved provider obligations until their exact saved state proves release is safe.

Builds, lint, and required benchmarks run in GitHub CI.
Tests are retired. Do not run local builds, clippy, benchmarks, or host build-lock helpers.
Local Rust formatting remains permitted:

```sh
cargo fmt -p cloud_backup_protocol --check
```
