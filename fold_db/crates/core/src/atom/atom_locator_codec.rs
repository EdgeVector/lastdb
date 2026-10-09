//! `uuid → partition` locator — the point-read index that lets a caller who
//! knows only an atom UUID find a partition-prefixed body.
//!
//! ## Why this module exists
//!
//! [`crate::atom::atom_key_codec`] can place an atom body in the same LastStore
//! partition as the tip that points at it, but only where the owning slot is in
//! scope. `GET /api/atom/{atom_uuid}` names an atom without naming its slot —
//! and it is not a rare path, it is the bulk object read surface (how a lastgit
//! pack object is read back). Under
//! [`AtomKeyEncoding::PartitionPrefix`](crate::atom::AtomKeyEncoding::PartitionPrefix)
//! that handler would build the flat key and miss every prefixed body.
//!
//! The locator is one small row per prefixed body:
//!
//! ```text
//! aloc:{uuid}  ->  "{partition}"      // e.g. "mk:{M}:{esc(hash)}\0"
//! ```
//!
//! One extra point read, on the uuid-only path only. Callers that already hold
//! the slot (the hot tip-driven read) build the prefixed key directly and never
//! touch this collection.
//!
//! ## Its own collection, deliberately
//!
//! Locator rows route to `atom_locators`, **not** `atoms`. `atoms` is the
//! collection whose cold group loads this whole exercise exists to bound (5.4 GB
//! on the primary); adding a per-atom index row to it would put index churn back
//! on the path the partition prefix just made cheap. The locator key carries no
//! `\0`, so it is full-key hashed within its own collection — which is fine
//! precisely because that collection holds nothing but these small rows.
//!
//! ## At rest
//!
//! A locator value is the partition prefix of a tip key: a molecule UUID
//! (`sha256(schema:field)`) and the **storage-form** hash segment. Both already
//! appear in plaintext in the `mk:` tip key the row is derived from — LastStore
//! catalogs are plaintext by design (see `fold_db/docs/`, PR #880), while atom
//! *content* is sealed. So the locator exposes nothing the tip catalog did not
//! already expose, and it never contains atom content.
//!
//! ## Degradation
//!
//! A missing locator resolves to the flat key, never to an error. That is what
//! makes the collection safe to build incrementally: a home mid-migration, an
//! atom written before the locator existed, and an orphan with no owning slot
//! all read exactly as they do today.

use crate::atom::atom_key_codec::AtomPartition;

/// The LastStore collection locator rows live in.
pub const LOCATOR_COLLECTION: &str = "atom_locators";

/// `aloc\0{uuid}` — the locator row for one atom body.
///
/// BASE key: callers add `{storage_prefix}:` via
/// [`crate::schema::types::field::build_storage_key`], so a share namespace's
/// locators stay inside that namespace exactly like its bodies do.
/// Dual-read still resolves the colon form `aloc:{uuid}`.
#[must_use]
pub fn locator_key(atom_uuid: &str) -> String {
    crate::kind_partition::anchored("aloc", atom_uuid)
}

/// The atom UUID a locator row is for. `None` if `base_key` is not a locator.
#[must_use]
pub fn uuid_of(base_key: &str) -> Option<&str> {
    crate::kind_partition::rest_of(base_key, "aloc")
}

/// The stored form of a locator value.
///
/// Deliberately a bare string rather than a struct: the value *is* the
/// partition prefix, byte for byte, so there is no shape to drift and no
/// version to negotiate. Anything that fails to parse as a partition is treated
/// as a miss.
#[must_use]
pub fn encode_value(partition: &AtomPartition) -> serde_json::Value {
    serde_json::Value::String(partition.as_str().to_string())
}

/// Inverse of [`encode_value`]. `None` for a missing, non-string, or
/// structurally impossible value — every one of which degrades to the flat key.
#[must_use]
pub fn decode_value(raw: &serde_json::Value) -> Option<AtomPartition> {
    AtomPartition::from_prefix(raw.as_str()?)
}
