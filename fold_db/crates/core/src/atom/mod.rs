mod atom_def;
pub mod atom_key_codec;
pub mod atom_locator_codec;
pub mod content_at_rest;
pub(crate) mod delete_barrier;
pub mod file_pointer;
pub mod legacy_history_memo;
pub mod merkle;
mod molecule_hash_range;
pub mod molecule_key_codec;
pub mod molecule_uuid;
pub mod mutation_event;
pub mod provenance;
pub mod size_limit;
pub mod tombstone;
pub mod trinity_bar;

pub use atom_def::Atom;
pub use atom_key_codec::{AtomKeyEncoding, AtomKeyEncodingMarker, AtomPartition};
pub use content_at_rest::{
    atom_content_binary_enabled, atom_content_dual_read_enabled, atom_row_header,
    open_atom_binary_row, open_atom_json, open_content_value, parse_atom_binary_row,
    reseal_atom_json_if_plain, seal_atom_binary_row, seal_atom_json, seal_content_value,
    ATOM_BINARY_ROW_PREFIX,
};
pub use molecule_hash_range::MoleculeHashRange;
pub use molecule_key_codec::{
    HashKeyEncoding, MoleculeKeyCodec, MoleculeKeyCodecError, RangeKeyEncoding,
};
pub use molecule_uuid::{
    encode_molecule_uuid_bytes, encode_molecule_uuid_hex, legacy_hex_molecule_uuid,
    molecule_uuid_alt_encoding, molecule_uuid_read_candidates, MOLECULE_UUID_B64URL_LEN,
    MOLECULE_UUID_HEX_LEN,
};
pub use mutation_event::{
    FieldKey, MutationEvent, MutationEventKind, SourceMutationOrder, SuppressedByDelete,
};
pub use provenance::{ImportedFieldProvenance, MoleculeRef, Provenance};
pub use size_limit::{
    atom_content_byte_len, enforce_atom_content_limit, ensure_atom_content_within_limit,
    is_over_default_limit, max_atom_content_bytes, observe_atom_content_over_default,
    parse_max_atom_content_bytes, ABSOLUTE_MAX_ATOM_CONTENT_BYTES, DEFAULT_MAX_ATOM_CONTENT_BYTES,
    HEADROOM_ALARM_FRACTION, MAX_ATOM_CONTENT_BYTES, MAX_ATOM_CONTENT_BYTES_ENV,
    MIN_MAX_ATOM_CONTENT_BYTES,
};
pub use tombstone::{is_tombstone_value, tombstone_content, TOMBSTONE_KEY};

/// Deterministic LWW total order for sync merges (minimal tip design).
///
/// Incoming wins iff `(written_at, logical_counter, device_id, mutation_uuid,
/// atom_uuid)` is strictly greater than local (tuple lexicographic order).
/// Empty `device_id` is allowed for
/// legacy fat tips (callers should pass `writer_pubkey` as the device key
/// when `device_id` is empty — see [`AtomEntry::lww_device`]).
///
/// `written_at` is the original device's signed write time and is the primary
/// order field. The signed logical counter breaks equal-time ties. `device_id`
/// breaks ties across writers, `mutation_uuid` breaks
/// ties within one writer, and `atom_uuid` is the final stable total-order
/// tiebreak (content-addressed).
///
/// Pre-clock data carries `logical_counter == 0` and keeps its time order.
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

fn is_zero_u8(v: &u8) -> bool {
    *v == 0
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

/// Per-key tip binding: which atom is current at this molecule slot.
///
/// **Thin tips (default write path):** `atom_uuid`, `written_at`, `device_id`,
/// and no history link. Crypto fields stay empty and are omitted from JSON
/// (`skip_serializing_if`).
///
/// **Optional history model (tip version chain):** when explicitly enabled and
/// a tip is overwritten, the prior value is archived under `tv:{prev_tip_id}`.
/// Walking the chain yields full per-slot history for `as_of`. No separate
/// `history:` MutationEvent log is written.
///
/// **Legacy fat tips:** may still contain `writer_pubkey` / `signature` /
/// `provenance` from older binaries. Dual-read: deserialize either shape;
/// [`AtomEntry::to_thin`] strips crypto for migration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct AtomEntry {
    pub atom_uuid: String,
    #[serde(default)]
    pub written_at: u64, // nanos since epoch
    /// Durable per-device logical author counter. Legacy tips use zero.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub logical_counter: u64,
    /// Mutation identity used after the author clock as a stable tie-break.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub mutation_uuid: String,
    /// Base64-encoded public key (legacy fat tips). Empty on thin tips.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub writer_pubkey: String,
    /// Base64-encoded Ed25519 signature (legacy fat tips). Empty on thin tips.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub signature: String,
    /// Signature scheme version (1 = signed fat tip). 0 on thin tips.
    #[serde(default, skip_serializing_if = "is_zero_u8")]
    pub signature_version: u8,
    /// Writer identity (legacy fat tips). None on thin tips.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<Provenance>,
    /// Device identity for LWW tie-break (thin tips). Stable node/device id
    /// (typically the writer's public key base64). Empty on pre-thin tips.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub device_id: String,
    /// Id of the previous tip version at this slot (`tv:{id}`). Empty on first
    /// write. Walk this chain for real per-slot history / `as_of`.
    ///
    /// Serde alias `prev_atom_uuid` reads short-lived depth-1 tips from an
    /// earlier cutover (those values were atom ids, not `tv:` ids — walk stops
    /// if the version node is missing).
    #[serde(
        default,
        skip_serializing_if = "String::is_empty",
        alias = "prev_atom_uuid"
    )]
    pub prev_tip_id: String,
}

impl AtomEntry {
    /// Construct a thin tip with no previous link.
    #[must_use]
    pub fn thin(atom_uuid: String, written_at: u64, device_id: String) -> Self {
        Self::thin_with_prev(atom_uuid, written_at, device_id, String::new())
    }

    /// Construct a thin tip, optionally linking to a previous tip version id.
    #[must_use]
    pub fn thin_with_prev(
        atom_uuid: String,
        written_at: u64,
        device_id: String,
        prev_tip_id: String,
    ) -> Self {
        Self {
            atom_uuid,
            written_at,
            logical_counter: 0,
            mutation_uuid: String::new(),
            writer_pubkey: String::new(),
            signature: String::new(),
            signature_version: 0,
            provenance: None,
            device_id,
            prev_tip_id,
        }
    }

    /// Construct a thin tip with a signed mutation author identity.
    #[must_use]
    pub fn thin_with_author(
        atom_uuid: String,
        written_at: u64,
        logical_counter: u64,
        device_id: String,
        mutation_uuid: String,
        prev_tip_id: String,
    ) -> Self {
        Self {
            atom_uuid,
            written_at,
            logical_counter,
            mutation_uuid,
            writer_pubkey: String::new(),
            signature: String::new(),
            signature_version: 0,
            provenance: None,
            device_id,
            prev_tip_id,
        }
    }

    /// True when this entry has no per-tip crypto payload (thin format).
    #[must_use]
    pub fn is_thin(&self) -> bool {
        self.signature.is_empty()
            && self.writer_pubkey.is_empty()
            && self.provenance.is_none()
            && self.signature_version == 0
    }

    /// Strip crypto fields for on-disk migration to thin tips. Preserves
    /// `atom_uuid`, `written_at`, `prev_tip_id`, and derives `device_id`
    /// from existing `device_id` or legacy `writer_pubkey`.
    #[must_use]
    pub fn to_thin(&self) -> Self {
        let device_id = if self.device_id.is_empty() {
            self.writer_pubkey.clone()
        } else {
            self.device_id.clone()
        };
        Self::thin_with_author(
            self.atom_uuid.clone(),
            self.written_at,
            self.logical_counter,
            device_id,
            self.mutation_uuid.clone(),
            self.prev_tip_id.clone(),
        )
    }

    /// Device key used for LWW: explicit `device_id`, else legacy `writer_pubkey`.
    #[must_use]
    pub fn lww_device(&self) -> &str {
        if self.device_id.is_empty() {
            &self.writer_pubkey
        } else {
            &self.device_id
        }
    }

    /// LWW tuple for this tip: origin time, author counter, device,
    /// mutation identity, then atom identity.
    #[must_use]
    pub fn lww_key(&self) -> (u64, u64, &str, &str, &str) {
        lww_order_key(
            self.written_at,
            self.logical_counter,
            self.lww_device(),
            self.mutation_uuid.as_str(),
            self.atom_uuid.as_str(),
        )
    }
}

/// Returns the current time in nanoseconds since the Unix epoch.
fn now_nanos() -> u64 {
    crate::clock::unix_nanos()
}

/// Generates a deterministic molecule UUID from schema name and field name.
///
/// Identity is SHA-256(`{schema}:{field}`). The **write** spelling is unpadded
/// base64url (43 chars). Homes that still store the 64-char hex spelling are
/// readable via [`molecule_uuid_read_candidates`].
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
/// peer's never-local atom, and `FieldVariant::rewind_to` walking the
/// history would resurrect that never-local atom on any `as_of` query
/// crossing the event timestamp. The peer's own local audit records its
/// loss symmetrically when it merges with us, so dropping self-won
/// conflicts here loses no information.
///
/// The `field_key` field carries the conflict's per-field key in typed form
/// (hash-only / range-only / full hash+range / empty Single slots on
/// `MoleculeHashRange`). This is the authoritative shape that downstream
/// consumers — chiefly `SyncEngine::store_merge_conflicts`, which embeds it
/// into the `MutationEvent` history row that powers `FieldVariant::rewind_to`
/// — read to build their `FieldKey`. Before this field existed, the typed
/// key was reconstructed by `split_once(':')` from a `:`-joined string
/// representation, which mangled both hash and range whenever either
/// contained a `:` (URLs, ISO timestamps); the typed channel makes the
/// round-trip exact regardless of the values. See the
/// `merge_conflict_field_key_preserves_hash_range_with_colons` test in
/// `molecule_hash_range.rs` for the pinned regression.
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
