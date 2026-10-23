mod atom_def;
mod atom_entry;
pub mod atom_key_codec;
pub mod content_at_rest;
pub(crate) mod delete_barrier;
pub mod file_pointer;
pub mod history_event_memo;
pub mod merkle;
mod molecule_hash_range;
pub(crate) use molecule_hash_range::keys::{
    CompactRangeKey, CoreStoredKey, KeyVisibility, MoleculeKeys,
};
pub mod molecule_key_codec;
pub mod molecule_uuid;
pub mod mutation_event;
pub mod origin_device;
pub mod provenance;
pub mod size_limit;
pub mod tombstone;

pub use atom_def::Atom;
pub use atom_entry::AtomEntry;
pub use content_at_rest::{
    atom_binary_row_parts, atom_content_binary_enabled, atom_row_header, open_atom_binary_row,
    open_atom_json, open_content_value, parse_atom_binary_row, seal_atom_binary_row,
    seal_atom_json, seal_content_value, ATOM_BINARY_ROW_PREFIX,
};
pub use molecule_hash_range::{
    ImportedTip, MoleculeHashRange, PendingTipConflict, TipAuthor, TipConflictKind,
};
pub use molecule_key_codec::{
    HashKeyEncoding, MoleculeKeyCodec, MoleculeKeyCodecError, RangeKeyEncoding,
};
pub use molecule_uuid::encode_molecule_uuid_bytes;
pub use mutation_event::{
    FieldKey, MutationEvent, MutationEventKind, SourceMutationOrder, SuppressedByDelete,
    SuppressedByTip,
};
pub use origin_device::{bounded_origin_device, MAX_ORIGIN_DEVICE_BYTES};
pub use provenance::{MoleculeRef, Provenance};
pub use size_limit::{
    atom_content_byte_len, enforce_atom_content_limit, is_over_default_limit,
    max_atom_content_bytes, observe_atom_content_over_default, parse_max_atom_content_bytes,
    ABSOLUTE_MAX_ATOM_CONTENT_BYTES, ATOM_CONTENT_TOO_LARGE_HINT, DEFAULT_MAX_ATOM_CONTENT_BYTES,
    HEADROOM_ALARM_FRACTION, MAX_ATOM_CONTENT_BYTES_ENV, MIN_MAX_ATOM_CONTENT_BYTES,
};
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

/// Returns the current time in nanoseconds since the Unix epoch.
fn now_nanos() -> u64 {
    crate::clock::unix_nanos()
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

/// Records a same-key LWW conflict that **changed the local state** during
/// a molecule merge. By construction `winner_atom = peer's atom` and
/// `loser_atom = local's prior atom`: the merge replaced local's value
/// with the peer's because the peer's `written_at` was at least as new.
///
/// Self-won "conflicts" (peer's atom strictly older, local keeps its value)
/// are **not** recorded. The downstream `MutationEvent` written by
/// `SyncEngine::store_merge_conflicts` is shaped
/// `{ old: loser_atom, new: winner_atom, is_conflict: true }`, and that
/// shape only matches reality when local actually transitioned from
/// `loser_atom` to `winner_atom`. For self-wins, `loser_atom` would be the
/// peer's never-local atom, so the event would describe a local transition
/// that never occurred. The peer's own local audit records its
/// loss symmetrically when it merges with us, so dropping self-won
/// conflicts here loses no information.
///
/// The `field_key` field carries the conflict's per-field key in typed form
/// (hash-only / range-only / full hash+range / empty Single slots on
/// `MoleculeHashRange`). This is the authoritative shape that downstream
/// consumers — chiefly `SyncEngine::store_merge_conflicts`, which embeds it
/// into the `MutationEvent` record used by history inspection and purge
/// — read to build their `FieldKey`. Before this field existed, the typed
/// key was reconstructed by `split_once(':')` from a `:`-joined string
/// representation, which mangled both hash and range whenever either
/// contained a `:` (URLs, ISO timestamps); the typed channel makes the
/// round-trip exact regardless of the values.
#[derive(Debug, Clone)]
pub struct MergeConflict {
    /// Typed per-field key for this conflict. Authoritative.
    pub field_key: FieldKey,
    /// Display string form for human-readable identifiers (e.g. the
    /// `SyncConflict.conflict_key` UI column, log/trace fields). LOSSY for
    /// `HashRange` when either component contains `:`; do not parse this
    /// back into hash + range — use `field_key` instead.
    pub key: String,
    pub winner_atom: String,
    pub loser_atom: String,
    pub winner_written_at: u64,
    pub loser_written_at: u64,
}

impl MergeConflict {
    /// Render `field_key` to the lossy display string used historically for
    /// `MergeConflict.key` and `SyncConflict.conflict_key`. Kept in one
    /// place so every producer agrees on the format and so we never bring
    /// back a divergent inline `format!` site.
    #[must_use]
    pub fn display_key(field_key: &FieldKey) -> String {
        match (&field_key.hash, &field_key.range) {
            (None, None) => "single".to_string(),
            (Some(hash), None) => hash.clone(),
            (None, Some(range)) => range.clone(),
            (Some(hash), Some(range)) => format!("{hash}:{range}"),
        }
    }
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
