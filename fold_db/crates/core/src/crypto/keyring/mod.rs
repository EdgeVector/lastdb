//! In-memory keyring (Gap G5, at-rest threat model §5.5/§5.6,
//! `docs/security/at-rest-threat-model.md`).
//!
//! A keyring holds per-purpose data-encryption keys (DEKs). Seal and open
//! use the envelope-v2 codec from [`super::envelope`].
//!
//! The module is split by responsibility:
//! - [`types`] owns the public data model.
//! - [`open`] owns the seal/open paths and current key-id lookup.
//!
//! ## No-silent-mint
//!
//! Per the discipline carried from `secure_store.rs` and extended to the
//! keyring (threat model §5.6): resolving a key or purpose that is absent
//! returns an error rather than fabricating one.

mod open;
mod types;

pub use types::{Dek, KeyPurpose, Keyring};
