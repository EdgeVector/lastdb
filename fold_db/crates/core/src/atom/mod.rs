mod atom_def;
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
pub use content_at_rest::{
    atom_content_binary_enabled, atom_row_header, open_atom_binary_row, open_atom_json,
    open_content_value, parse_atom_binary_row, seal_atom_binary_row, seal_atom_json,
    seal_content_value, ATOM_BINARY_ROW_PREFIX,
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

fn is_zero_u8(v: &u8) -> bool {
    *v == 0
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

fn is_empty_str(v: &&str) -> bool {
    v.is_empty()
}

/// Current atom and author identity at one molecule key.
///
/// Current constructors leave the optional crypto fields empty. A tip has no
/// archived-value query or history chain. Separate `history\0` mutation events
/// retain conflict and replay outcomes with their own atom references.
///
/// The signer fields and the reserved link are almost always empty, so they
/// live in one boxed part that is `None` when all of them are empty. The warm
/// set holds one entry per read field, and five empty strings and an empty
/// provenance cost about 128 bytes in each (measured 2026-10-20). The stored
/// JSON is unchanged: [`AtomEntryWire`] keeps the flat field list.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
#[serde(from = "AtomEntryWire")]
pub struct AtomEntry {
    pub atom_uuid: String,
    pub written_at: u64, // nanos since epoch
    /// Durable per-device logical author counter. Zero when no counter is supplied.
    pub logical_counter: u64,
    /// Mutation identity used after the author clock as a stable tie-break.
    pub mutation_uuid: String,
    /// Device identity for the LWW tie-break, usually the writer's public key.
    pub device_id: String,
    /// Sync device that wrote this tip (the sync `writer_id` of its log
    /// stream). Not part of the LWW order: `device_id` above is the owner key,
    /// which own devices share. Empty means "in doubt": an old tip, a write
    /// without a sync device id, or an import whose writer is unknown.
    pub origin_device: String,
    /// Signer fields and the reserved link. `None` when all are empty.
    extra: Option<Box<AtomEntryExtra>>,
}

/// The rarely set part of an [`AtomEntry`].
#[derive(Debug, Clone, PartialEq, Default)]
struct AtomEntryExtra {
    writer_pubkey: String,
    signature: String,
    signature_version: u8,
    provenance: Option<Provenance>,
    prev_tip_id: String,
}

impl AtomEntryExtra {
    fn is_empty(&self) -> bool {
        self.writer_pubkey.is_empty()
            && self.signature.is_empty()
            && self.signature_version == 0
            && self.provenance.is_none()
            && self.prev_tip_id.is_empty()
    }
}

/// The stored form of an [`AtomEntry`]: one flat object.
#[derive(serde::Deserialize)]
struct AtomEntryWire {
    atom_uuid: String,
    #[serde(default)]
    written_at: u64,
    #[serde(default)]
    logical_counter: u64,
    #[serde(default)]
    mutation_uuid: String,
    #[serde(default)]
    writer_pubkey: String,
    #[serde(default)]
    signature: String,
    #[serde(default)]
    signature_version: u8,
    #[serde(default)]
    provenance: Option<Provenance>,
    #[serde(default)]
    device_id: String,
    #[serde(default)]
    origin_device: String,
    #[serde(default)]
    prev_tip_id: String,
}

/// Borrowed stored form, so serialization does not clone the entry.
#[derive(serde::Serialize)]
struct AtomEntryWireRef<'a> {
    atom_uuid: &'a str,
    written_at: u64,
    #[serde(skip_serializing_if = "is_zero_u64")]
    logical_counter: u64,
    #[serde(skip_serializing_if = "is_empty_str")]
    mutation_uuid: &'a str,
    #[serde(skip_serializing_if = "is_empty_str")]
    writer_pubkey: &'a str,
    #[serde(skip_serializing_if = "is_empty_str")]
    signature: &'a str,
    #[serde(skip_serializing_if = "is_zero_u8")]
    signature_version: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    provenance: Option<&'a Provenance>,
    #[serde(skip_serializing_if = "is_empty_str")]
    device_id: &'a str,
    #[serde(skip_serializing_if = "is_empty_str")]
    origin_device: &'a str,
    #[serde(skip_serializing_if = "is_empty_str")]
    prev_tip_id: &'a str,
}

impl From<AtomEntryWire> for AtomEntry {
    fn from(wire: AtomEntryWire) -> Self {
        let mut entry = Self {
            atom_uuid: wire.atom_uuid,
            written_at: wire.written_at,
            logical_counter: wire.logical_counter,
            mutation_uuid: wire.mutation_uuid,
            device_id: wire.device_id,
            origin_device: wire.origin_device,
            extra: None,
        };
        entry.set_extra(AtomEntryExtra {
            writer_pubkey: wire.writer_pubkey,
            signature: wire.signature,
            signature_version: wire.signature_version,
            provenance: wire.provenance,
            prev_tip_id: wire.prev_tip_id,
        });
        entry
    }
}

impl serde::Serialize for AtomEntry {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        AtomEntryWireRef {
            atom_uuid: &self.atom_uuid,
            written_at: self.written_at,
            logical_counter: self.logical_counter,
            mutation_uuid: &self.mutation_uuid,
            writer_pubkey: self.writer_pubkey(),
            signature: self.signature(),
            signature_version: self.signature_version(),
            provenance: self.provenance(),
            device_id: &self.device_id,
            origin_device: &self.origin_device,
            prev_tip_id: self.prev_tip_id(),
        }
        .serialize(serializer)
    }
}

impl AtomEntry {
    /// Construct a current tip with the reserved link field empty.
    #[must_use]
    pub fn thin(atom_uuid: String, written_at: u64, device_id: String) -> Self {
        Self::thin_with_prev(atom_uuid, written_at, device_id, String::new())
    }

    /// Construct a current tip with the supplied reserved link field.
    #[must_use]
    pub fn thin_with_prev(
        atom_uuid: String,
        written_at: u64,
        device_id: String,
        prev_tip_id: String,
    ) -> Self {
        Self::thin_with_author(
            atom_uuid,
            written_at,
            0,
            device_id,
            String::new(),
            prev_tip_id,
        )
    }

    /// Construct a current tip with a signed mutation author identity.
    #[must_use]
    pub fn thin_with_author(
        atom_uuid: String,
        written_at: u64,
        logical_counter: u64,
        device_id: String,
        mutation_uuid: String,
        prev_tip_id: String,
    ) -> Self {
        let mut entry = Self {
            atom_uuid,
            written_at,
            logical_counter,
            mutation_uuid,
            device_id,
            origin_device: String::new(),
            extra: None,
        };
        entry.set_prev_tip_id(prev_tip_id);
        entry
    }

    /// Return this entry with the sync device that wrote it.
    #[must_use]
    pub fn with_origin_device(mut self, origin_device: String) -> Self {
        self.origin_device = origin_device;
        self
    }

    /// Optional base64-encoded public key. Current constructors leave it empty.
    #[must_use]
    pub fn writer_pubkey(&self) -> &str {
        self.extra.as_ref().map_or("", |extra| &extra.writer_pubkey)
    }

    /// Optional base64-encoded signature. Current constructors leave it empty.
    #[must_use]
    pub fn signature(&self) -> &str {
        self.extra.as_ref().map_or("", |extra| &extra.signature)
    }

    /// Optional signature scheme version. Current constructors use zero.
    #[must_use]
    pub fn signature_version(&self) -> u8 {
        self.extra
            .as_ref()
            .map_or(0, |extra| extra.signature_version)
    }

    /// Optional writer provenance. Current constructors leave it absent.
    #[must_use]
    pub fn provenance(&self) -> Option<&Provenance> {
        self.extra
            .as_ref()
            .and_then(|extra| extra.provenance.as_ref())
    }

    /// Retained serialized field. Current writes leave it empty; no archived
    /// tip reader or historical query follows this value.
    #[must_use]
    pub fn prev_tip_id(&self) -> &str {
        self.extra.as_ref().map_or("", |extra| &extra.prev_tip_id)
    }

    /// Set the signer fields. Empty values leave no boxed part behind.
    pub fn set_signer(
        &mut self,
        writer_pubkey: String,
        signature: String,
        signature_version: u8,
        provenance: Option<Provenance>,
    ) {
        let prev_tip_id = self.take_extra().prev_tip_id;
        self.set_extra(AtomEntryExtra {
            writer_pubkey,
            signature,
            signature_version,
            provenance,
            prev_tip_id,
        });
    }

    /// Set the reserved link field.
    pub fn set_prev_tip_id(&mut self, prev_tip_id: String) {
        let mut extra = self.take_extra();
        extra.prev_tip_id = prev_tip_id;
        self.set_extra(extra);
    }

    /// Heap bytes of the boxed part, for the warm set gauge: its strings, its
    /// provenance strings, and the box itself.
    #[must_use]
    pub fn extra_heap_bytes(&self) -> usize {
        self.extra.as_ref().map_or(0, |extra| {
            let provenance = extra.provenance.as_ref().map_or(0, Provenance::heap_bytes);
            std::mem::size_of::<AtomEntryExtra>()
                + extra.writer_pubkey.capacity()
                + extra.signature.capacity()
                + extra.prev_tip_id.capacity()
                + provenance
        })
    }

    fn take_extra(&mut self) -> AtomEntryExtra {
        self.extra.take().map(|extra| *extra).unwrap_or_default()
    }

    fn set_extra(&mut self, extra: AtomEntryExtra) {
        self.extra = (!extra.is_empty()).then(|| Box::new(extra));
    }

    /// Device key used for LWW: explicit `device_id`, otherwise `writer_pubkey`.
    #[must_use]
    pub fn lww_device(&self) -> &str {
        if self.device_id.is_empty() {
            self.writer_pubkey()
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
