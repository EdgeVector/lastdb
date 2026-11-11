//! DEV-only preparation for moving one database from legacy bearer reads to
//! signed node-key reads.
//!
//! Exemem is the only authority that commits signed-only. This crate reads no
//! clock, opens no file, and contacts neither the cloud nor Exemem. The shell
//! passes evidence bytes. [`prepare`] writes an inactive request. It does not
//! set `signed_only`, forbid legacy resume, or allow retirement.
//! [`resume_unchanged`] confirms that a readable request leaves access as it
//! was. [`check_proof`] checks a second device's ack and matching keyed reads
//! and stores nothing.
//!
//! A local file is not a service receipt. `service_receipt` is null until E
//! and I publish an authenticated receipt interface. Production evidence does
//! not parse.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

mod check;
mod digest;
mod error;
mod parse;
mod prepare;
mod record;
mod wire;

pub use error::{Reason, Refusal};
pub use parse::{parse_bundle, parse_proof, parse_request, EvidenceBytes};
pub use prepare::{check_proof, prepare, resume_unchanged, verify, PrepareOutcome};
pub use record::{Request, REQUEST_FILE};
pub use wire::{
    Bundle, DaemonReady, DeliveryReady, FinalWrites, LegacyPosition, MemberGrant, NodeKeyGrants,
    SecondDeviceProof, SnapshotReceipt, WriterTip, DAEMON_READY_FILE, DELIVERY_READY_FILE,
    FINAL_WRITES_FILE, GRANTS_FILE, KIND_PERSONAL, KIND_SHAREABLE, LEGACY_POSITION_FILE,
    MAX_EVIDENCE_BYTES, SCHEMA, SNAPSHOT_FILE,
};
