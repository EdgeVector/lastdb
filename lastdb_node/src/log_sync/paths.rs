//! Client routes for the mutation log. Dev and prod use this same prefix.
//!
//! Data routes are `append`, `head`, `list`, `ack`, and `snapshot`.
//! `create-database` and `access` share the prefix and are not data routes.
//! `revoke` is G's route. `grant`, `writer-tip`, and `reject` are not routes.
//! `list` records the device. A cursor below the floor is a list error.

pub(crate) const CREATE: &str = "/api/cloud/backups/v2/mutation-log/create-database";
pub(crate) const ACCESS: &str = "/api/cloud/backups/v2/mutation-log/access";
pub(crate) const APPEND: &str = "/api/cloud/backups/v2/mutation-log/append";
pub(crate) const HEAD: &str = "/api/cloud/backups/v2/mutation-log/head";
pub(crate) const LIST: &str = "/api/cloud/backups/v2/mutation-log/list";
pub(crate) const ACK: &str = "/api/cloud/backups/v2/mutation-log/ack";
pub(crate) const REVOKE: &str = "/api/cloud/backups/v2/mutation-log/revoke";
pub(crate) const SNAPSHOT: &str = "/api/cloud/backups/v2/mutation-log/snapshot";
pub(crate) const SNAPSHOT_BYTES: &str = "/api/cloud/backups/v2/mutation-log/snapshot/bytes";
/// Signed chain walk and part fetch. Not the snapshot cut.
pub(crate) const DELIVERY_SNAPSHOT: &str = "/api/cloud/backups/v2/mutation-log/delivery/snapshot";
/// One ciphertext range of a record file. Standard base64, not URL-safe.
pub(crate) const DELIVERY_OBJECT: &str = "/api/cloud/backups/v2/mutation-log/delivery/object";
