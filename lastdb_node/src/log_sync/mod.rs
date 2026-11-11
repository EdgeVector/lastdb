//! Daemon adapter for the cloud mutation log.
//!
//! Exemem's authenticated mutation-log reply is the only signed-only switch.
//! Each owned database has its own reply. A local file does not start sync or
//! stop byte backup. This module does not change backup policy and does not
//! install a snapshot. E2 owns installation through the normal replay path.
//! `byte_cloud_restore::native::restore_at` restores an owner byte-backup
//! file set and is not that installer. An empty client gets the original
//! database id from `registered_share::accept` or `RestoreSession::restore`,
//! not from an edit of `registry.json`.

mod accepted;
mod apply;
mod cloud;
mod cutover;
mod disk;
mod http;
mod native;
mod outbox;
mod paths;
mod payload;
mod registry;
mod run;
mod session;
mod sign;
mod status;
mod turn;
mod wire;

pub use cutover::legacy_resume_forbidden;
pub use turn::spawn;
