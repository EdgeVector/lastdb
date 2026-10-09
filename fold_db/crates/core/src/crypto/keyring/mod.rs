//! Wrapped-DEK keyring (Gap G5, at-rest threat model §5.5/§5.6,
//! `docs/security/at-rest-threat-model.md`).
//!
//! A keyring holds per-purpose **data-encryption keys** (DEKs), each
//! sealed ("wrapped") under the **key-encryption key** (KEK = the master
//! key) using the envelope-v2 codec from [`super::envelope`]. The
//! plaintext DEK never touches disk; only the KEK-wrapped form persists
//! in `keyring.enc`.
//!
//! The module is split by responsibility:
//! - [`types`] owns the public data model and purpose wire tags.
//! - [`mint`] owns DEK allocation and active-key rotation.
//! - [`open`] owns the seal/open paths and current key-id lookup.
//! - [`persist`] owns the `keyring.enc` wire format.
//!
//! ## No-silent-mint
//!
//! Per the discipline carried verbatim from `secure_store.rs` and
//! extended to the keyring (threat model §5.6): **loading never mints**.
//! [`Keyring::deserialize`] only unwraps what is on disk; resolving a
//! key/purpose that is absent returns an error rather than fabricating
//! one. Minting a DEK is always an explicit [`Keyring::mint_dek`] call.

mod mint;
mod open;
mod persist;
mod types;

pub use types::{Dek, KeyPurpose, Keyring};

use types::Entry;
