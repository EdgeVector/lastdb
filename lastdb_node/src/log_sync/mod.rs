//! Daemon adapter for the cloud mutation log.
//!
//! Exemem's authenticated mutation-log reply is the only signed-only switch.
//! Each owned database has its own reply. A local file does not start sync or
//! stop byte backup. This module does not change backup policy. A named
//! database installs a snapshot with `named_snapshot_restore` after
//! `delivery/snapshot`. That install does not acknowledge. Local capture
//! stays blocked until `accepted_writes` is a committed reader.
//! `byte_cloud_restore::native::restore_at` restores an owner byte-backup
//! file set and is not the named installer. This module does not edit
//! `registry.json`.

mod accepted;
mod account;
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
mod restore;
mod run;
mod session;
mod sign;
mod status;
mod turn;
mod wire;

pub use cutover::legacy_resume_forbidden;
pub use turn::spawn;
