//! Daemon adapter for the cloud mutation log.
//!
//! A home with no signed-only cutover record keeps byte backup. This module
//! does not change backup policy and does not install a snapshot into the live store.

mod apply;
mod cloud;
mod cutover;
mod disk;
mod http;
mod outbox;
mod paths;
mod payload;
mod registry;
mod session;
mod sign;
mod turn;
mod wire;

pub use cutover::legacy_resume_forbidden;
pub use outbox::{capture_mutations, store_outbox};
pub use turn::spawn;
