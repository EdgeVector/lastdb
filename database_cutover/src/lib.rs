//! Read-only evidence check for one existing database.
//!
//! The same checks apply in `dev` and in `prod`. Exemem is the only authority
//! that commits signed-only. This crate reads no clock, opens no file, and
//! contacts neither the cloud nor Exemem. The shell passes evidence bytes.
//! [`verify`] returns the digest those bytes bind. It does not write a
//! request, cache a receipt, or change access.
//!
//! A fresh named database does not use this crate. The normal signed create
//! registers it. This crate does not accept a local file as the service commit.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

mod check;
mod digest;
mod error;
mod parse;
mod wire;

pub use error::{Reason, Refusal};
pub use parse::{parse_bundle, EvidenceBytes};
pub use wire::{
    Bundle, DaemonReady, DeliveryReady, FinalWrites, LegacyPosition, MemberGrant, NodeKeyGrants,
    SnapshotReceipt, WriterTip, DAEMON_READY_FILE, DELIVERY_READY_FILE, FINAL_WRITES_FILE,
    GRANTS_FILE, KIND_PERSONAL, KIND_SHAREABLE, LEGACY_POSITION_FILE, MAX_EVIDENCE_BYTES, SCHEMA,
    SNAPSHOT_FILE,
};

/// Check one bundle for the named database and realm.
///
/// The returned string is the evidence digest. It is not a signed-only commit.
pub fn verify(
    expected_database_id: &str,
    expected_environment: &str,
    bundle: &wire::Bundle,
) -> Result<String, Refusal> {
    if let Some(refusal) = Refusal::new(check::problems(
        expected_database_id,
        expected_environment,
        bundle,
    )) {
        return Err(refusal);
    }
    Ok(digest::digest(bundle))
}
