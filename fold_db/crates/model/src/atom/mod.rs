mod atom_def;
mod atom_entry;
pub mod atom_key_codec;
pub mod content_limit;
pub mod delete_barrier;
pub mod file_pointer;
mod molecule_hash_range;
pub use molecule_hash_range::keys::{CompactRangeKey, CoreStoredKey, KeyVisibility, MoleculeKeys};
pub mod molecule_key_codec;
pub mod molecule_slot;
pub mod molecule_uuid;
pub mod mutation_event;
pub mod provenance;
pub mod tombstone;

pub use atom_def::Atom;
pub use atom_entry::AtomEntry;
pub use content_limit::{
    atom_content_byte_len, ABSOLUTE_MAX_ATOM_CONTENT_BYTES, ATOM_CONTENT_TOO_LARGE_HINT,
    DEFAULT_MAX_ATOM_CONTENT_BYTES, HEADROOM_ALARM_FRACTION, MAX_ATOM_CONTENT_BYTES_ENV,
    MIN_MAX_ATOM_CONTENT_BYTES, RECORD_TOO_LARGE_HINT,
};
pub use molecule_hash_range::MoleculeHashRange;
pub use molecule_key_codec::{MoleculeKeyCodec, MoleculeKeyCodecError};
pub use molecule_uuid::encode_molecule_uuid_bytes;
pub use mutation_event::{
    FieldKey, MutationEvent, MutationEventKind, SourceMutationOrder, SuppressedByDelete,
    SuppressedByTip,
};
pub use provenance::{MoleculeRef, Provenance};
pub use tombstone::{is_tombstone_value, TOMBSTONE_KEY};

/// Deterministic LWW total order for sync merges (minimal tip design).
///
/// Incoming wins iff `(written_at, logical_counter, device_id, mutation_uuid,
/// atom_uuid)` is strictly greater than local (tuple lexicographic order).
/// When `device_id` is empty, [`AtomEntry::lww_device`] uses the optional
/// `writer_pubkey` as the device key.
///
/// `written_at` is the original device's signed write time and is the primary
/// order field. The signed logical counter breaks equal-time ties. `device_id`
/// breaks ties across writers, `mutation_uuid` breaks
/// ties within one writer, and `atom_uuid` is the final stable total-order
/// tiebreak (content-addressed).
///
/// A missing logical counter uses zero and keeps its time order.
///
/// Build both operands with [`lww_order_key`] or [`AtomEntry::lww_key`] — never
/// spell the tuple out at a call site, or the two orderings drift apart.
pub fn incoming_wins_lww(
    incoming: (u64, u64, &str, &str, &str),
    local: (u64, u64, &str, &str, &str),
) -> bool {
    incoming > local
}

/// Build the LWW total-order key from loose tip parts.
///
/// The one place that fixes field order for [`incoming_wins_lww`]. Callers pass
/// the tip fields in their natural (storage) order; this returns them in
/// comparison order, origin time first.
#[must_use]
pub fn lww_order_key<'a>(
    written_at: u64,
    logical_counter: u64,
    device_id: &'a str,
    mutation_uuid: &'a str,
    atom_uuid: &'a str,
) -> (u64, u64, &'a str, &'a str, &'a str) {
    (
        written_at,
        logical_counter,
        device_id,
        mutation_uuid,
        atom_uuid,
    )
}

/// Generates a deterministic molecule UUID from schema name and field name.
///
/// Identity is SHA-256 of the framed domain, schema and field tuple from
/// [`molecule_uuid::molecule_uuid_digest`], encoded as unpadded base64url.
/// Logical reads and writes use that exact string, without alternate spellings.
pub fn deterministic_molecule_uuid(schema_name: &str, field_name: &str) -> String {
    molecule_uuid::encode_molecule_uuid_bytes(&molecule_uuid::molecule_uuid_digest(
        schema_name,
        field_name,
    ))
}

/// Write-time metadata stored per-key on the molecule.
/// Survives atom deduplication because it lives on the key-to-atom
/// association, not on the content-addressed atom itself.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default, PartialEq, Eq)]
pub struct KeyMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
    /// `true` when this key's current atom is a tombstone (written by
    /// `MutationType::Delete`). Persisted in the molecule KEY index
    /// (`mk:{M}:{key}` → `PerKeyRecord.meta`), so a default read can skip
    /// the deleted key at the *enumeration* boundary — before dereferencing
    /// its atom body — instead of fetching + deserializing every dead
    /// molecule only to discard it by content. This is what makes a default
    /// full scan's atom-fetch cost track the LIVE record count, not
    /// live+tombstoned. `#[serde(default)]` ⇒ legacy `mk:` records written
    /// before this field default to `false`; the content-based
    /// [`crate::atom::is_tombstone_value`] predicate stays as a correctness
    /// backstop for them at the resolution boundary.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub tombstoned: bool,
}
