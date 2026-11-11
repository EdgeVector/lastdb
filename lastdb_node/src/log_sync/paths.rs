//! Client routes for the mutation log. Dev and prod use this same prefix.
//!
//! G publishes `create-database`, `append`, `head`, `list`, `ack`, `revoke`,
//! `snapshot`, and `trim`. `list` records the device. A cursor below the floor
//! is a list error, not a `recover` call. `register-reader` is not a route.

pub(crate) const CREATE: &str = "/api/cloud/backups/v2/mutation-log/create-database";
pub(crate) const APPEND: &str = "/api/cloud/backups/v2/mutation-log/append";
pub(crate) const HEAD: &str = "/api/cloud/backups/v2/mutation-log/head";
pub(crate) const LIST: &str = "/api/cloud/backups/v2/mutation-log/list";
pub(crate) const ACK: &str = "/api/cloud/backups/v2/mutation-log/ack";
pub(crate) const GRANT: &str = "/api/cloud/backups/v2/mutation-log/grant";
pub(crate) const REVOKE: &str = "/api/cloud/backups/v2/mutation-log/revoke";
pub(crate) const SNAPSHOT: &str = "/api/cloud/backups/v2/mutation-log/snapshot";
pub(crate) const WRITER_TIP: &str = "/api/cloud/backups/v2/mutation-log/writer-tip";
pub(crate) const REJECT: &str = "/api/cloud/backups/v2/mutation-log/reject";
pub(crate) const SNAPSHOT_BYTES: &str = "/api/cloud/backups/v2/mutation-log/snapshot/bytes";
