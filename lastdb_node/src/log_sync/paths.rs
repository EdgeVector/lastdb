//! Client routes for the mutation log. Dev and prod use this same prefix.
//! G, H, and I bind the handlers. `head` is the signed-only confirmation read.
//! A cursor body is not a commit. A commit is D's `CutoverReceipt` on that reply.

pub(crate) const APPEND: &str = "/api/cloud/backups/v2/mutation-log/append";
pub(crate) const HEAD: &str = "/api/cloud/backups/v2/mutation-log/head";
pub(crate) const LIST: &str = "/api/cloud/backups/v2/mutation-log/list";
pub(crate) const ACK: &str = "/api/cloud/backups/v2/mutation-log/ack";
pub(crate) const RECOVER: &str = "/api/cloud/backups/v2/mutation-log/recover";
pub(crate) const REGISTER_READER: &str = "/api/cloud/backups/v2/mutation-log/register-reader";
pub(crate) const GRANT: &str = "/api/cloud/backups/v2/mutation-log/grant";
pub(crate) const REVOKE: &str = "/api/cloud/backups/v2/mutation-log/revoke";
pub(crate) const SNAPSHOT: &str = "/api/cloud/backups/v2/mutation-log/snapshot";
pub(crate) const WRITER_TIP: &str = "/api/cloud/backups/v2/mutation-log/writer-tip";
pub(crate) const REJECT: &str = "/api/cloud/backups/v2/mutation-log/reject";
pub(crate) const SNAPSHOT_BYTES: &str = "/api/cloud/backups/v2/mutation-log/snapshot/bytes";
