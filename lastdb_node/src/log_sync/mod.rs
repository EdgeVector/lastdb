//! Daemon adapter for the cloud mutation log.
//!
//! Exemem's signed mutation-log reply is the only signed-only switch. Each
//! owned database has its own reply. A local file does not start sync or stop
//! byte backup. This module does not change backup policy. It does not install
//! a snapshot: `byte_cloud_restore::native::restore_at` restores an owner
//! byte-backup file set, and I's `mutation-log/delivery/snapshot` parts have
//! no installer for `databases/<id>/data`.

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
mod turn;
mod wire;

pub use cutover::legacy_resume_forbidden;
pub use outbox::{capture_mutations, store_outbox};
pub use turn::spawn;
