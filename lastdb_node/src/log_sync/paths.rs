//! Client routes for the mutation log. Dev and prod use this same prefix.
//!
//! Data routes are `append`, `head`, and `list`.
//! `create-database` and `access` share the prefix and are not data routes.
//! `list` takes a local download position. A cursor below the floor is a list error.

pub(crate) const CREATE: &str = "/api/cloud/backups/v2/mutation-log/create-database";
pub(crate) const ACCESS: &str = "/api/cloud/backups/v2/mutation-log/access";
pub(crate) const APPEND: &str = "/api/cloud/backups/v2/mutation-log/append";
pub(crate) const HEAD: &str = "/api/cloud/backups/v2/mutation-log/head";
pub(crate) const LIST: &str = "/api/cloud/backups/v2/mutation-log/list";
pub(crate) use cloud_backup_protocol::v2::share_access::{
    MUTATION_LOG_DELIVERY_OBJECT_PATH as DELIVERY_OBJECT,
    MUTATION_LOG_DELIVERY_SNAPSHOT_PATH as DELIVERY_SNAPSHOT,
};
