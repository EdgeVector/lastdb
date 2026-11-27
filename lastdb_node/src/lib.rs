//! Library surface of the minimal `lastdbd` daemon.

pub(crate) mod author_identity;
pub mod cloud;
pub mod cloud_backup_client;
pub(crate) mod database_birth;
pub(crate) mod device_identity;
pub mod exec;
pub mod host;
pub mod jobs;
pub mod lastdb_uds;
pub mod log_sync;
pub mod offline_home;
pub(crate) mod personal_author;
pub(crate) mod personal_identity;
pub(crate) mod personal_seal;
pub mod profile;
pub(crate) mod registered_share;
pub(crate) mod share_cache;
pub mod share_invitation;
pub(crate) mod share_link;
pub(crate) mod shared_seal;
pub mod version;
