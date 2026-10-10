//! Construction and per-key materialization.

use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, HashMap};

use super::MoleculeHashRange;
use crate::atom::{deterministic_molecule_uuid, now_nanos, AtomEntry, KeyMetadata};

impl MoleculeHashRange {
    /// Creates a new empty MoleculeHashRange with a deterministic UUID.
    #[must_use]
    pub fn new(schema_name: &str, field_name: &str) -> Self {
        Self {
            uuid: deterministic_molecule_uuid(schema_name, field_name),
            atom_uuids: HashMap::new(),
            pending_tip_versions: Vec::new(),
            pending_replaced_tips: Vec::new(),
            tip_history_enabled: false,
            updated_at: Utc::now(),
            order_is_tail: false,
            version: 0,
            key_metadata: HashMap::new(),
        }
    }

    /// Creates a new empty MoleculeHashRange under an **already-assigned**
    /// molecule UUID.
    ///
    /// Use this when the caller knows which molecule the field is bound to and
    /// that binding is not `deterministic(this_schema, this_field)` — the
    /// FieldMapper case, where `populate_runtime_fields` already resolved the
    /// field to its chain root's molecule. [`Self::new`] derives the UUID from
    /// the pair it is handed, so calling it with the *mapped* field's own
    /// (schema, field) silently re-anchors the field onto a second molecule.
    #[must_use]
    pub fn with_uuid(uuid: String) -> Self {
        Self {
            uuid,
            atom_uuids: HashMap::new(),
            pending_tip_versions: Vec::new(),
            pending_replaced_tips: Vec::new(),
            tip_history_enabled: false,
            updated_at: Utc::now(),
            order_is_tail: false,
            version: 0,
            key_metadata: HashMap::new(),
        }
    }

    /// Creates a new MoleculeHashRange with existing atom UUIDs.
    #[must_use]
    pub fn with_atoms(
        schema_name: &str,
        field_name: &str,
        atom_uuids: HashMap<String, BTreeMap<String, String>>,
    ) -> Self {
        let ts = now_nanos();
        let entries: HashMap<String, BTreeMap<String, AtomEntry>> = atom_uuids
            .into_iter()
            .map(|(hash, range_map)| {
                let entry_map: BTreeMap<String, AtomEntry> = range_map
                    .into_iter()
                    .map(|(range, atom_uuid)| {
                        (range, AtomEntry::thin(atom_uuid, ts, String::new()))
                    })
                    .collect();
                (hash, entry_map)
            })
            .collect();

        Self {
            uuid: deterministic_molecule_uuid(schema_name, field_name),
            atom_uuids: entries,
            updated_at: Utc::now(),
            order_is_tail: false,
            version: 0,
            key_metadata: HashMap::new(),
            pending_tip_versions: Vec::new(),
            pending_replaced_tips: Vec::new(),
            tip_history_enabled: false,
        }
    }

    /// Explode into per-key records `(hash, range, entry, metadata)` for
    /// per-key storage. The molecule-level `(uuid, version, updated_at)`
    /// travel in the header alongside these records.
    ///
    /// **Cost:** materialises a `Vec` of the ENTIRE molecule, cloning every
    /// hash, range and [`AtomEntry`]. That is what the persist path wants — it
    /// writes all of them. It is the wrong tool for finding ONE slot: use
    /// [`Self::get_atom_entry`], which is a hashmap lookup. Calling this per key
    /// is how bulk purge stayed `O(R)`-per-record after being batched in `N`;
    /// see `collect_target_chain`.
    pub(crate) fn per_key_records(&self) -> Vec<(String, String, AtomEntry, Option<KeyMetadata>)> {
        let mut out = Vec::new();
        for (hash, ranges) in &self.atom_uuids {
            for (range, entry) in ranges {
                let meta = self
                    .key_metadata
                    .get(hash)
                    .and_then(|m| m.get(range))
                    .cloned();
                out.push((hash.clone(), range.clone(), entry.clone(), meta));
            }
        }
        out
    }

    /// Whether this molecule is a write-only tail rather than a full snapshot.
    ///
    /// The persist path refuses a full rewrite when this is true, and a tail
    /// takes the shared append guard. The marker is not an order log.
    pub(crate) fn order_is_tail(&self) -> bool {
        self.order_is_tail
    }

    /// Rebuild from per-key records. Entries are restored **verbatim** — no
    /// re-signing. The result is a full snapshot (`order_is_tail` is false).
    pub(crate) fn from_per_key_records(
        uuid: String,
        version: u64,
        updated_at: DateTime<Utc>,
        records: Vec<(String, String, AtomEntry, Option<KeyMetadata>)>,
    ) -> Self {
        let mut atom_uuids: HashMap<String, BTreeMap<String, AtomEntry>> = HashMap::new();
        let mut key_metadata: HashMap<String, BTreeMap<String, KeyMetadata>> = HashMap::new();
        for (hash, range, entry, meta) in records {
            if let Some(meta) = meta {
                key_metadata
                    .entry(hash.clone())
                    .or_default()
                    .insert(range.clone(), meta);
            }
            atom_uuids.entry(hash).or_default().insert(range, entry);
        }
        Self {
            uuid,
            atom_uuids,
            updated_at,
            order_is_tail: false,
            version,
            key_metadata,
            pending_tip_versions: Vec::new(),
            pending_replaced_tips: Vec::new(),
            tip_history_enabled: false,
        }
    }

    /// Rebuild a write-only molecule from just the touched records.
    ///
    /// [`Self::order_is_tail`] is true. A tail takes the shared append guard.
    /// A full rewrite of a tail molecule is refused. This constructor does not
    /// read `moc:` and does not build an order vector.
    pub(crate) fn from_write_records(
        uuid: String,
        version: u64,
        updated_at: DateTime<Utc>,
        records: Vec<(String, String, AtomEntry, Option<KeyMetadata>)>,
    ) -> Self {
        let mut atom_uuids: HashMap<String, BTreeMap<String, AtomEntry>> = HashMap::new();
        let mut key_metadata: HashMap<String, BTreeMap<String, KeyMetadata>> = HashMap::new();
        for (hash, range, entry, meta) in records {
            if let Some(meta) = meta {
                key_metadata
                    .entry(hash.clone())
                    .or_default()
                    .insert(range.clone(), meta);
            }
            atom_uuids.entry(hash).or_default().insert(range, entry);
        }
        Self {
            uuid,
            atom_uuids,
            updated_at,
            order_is_tail: true,
            version,
            key_metadata,
            pending_tip_versions: Vec::new(),
            pending_replaced_tips: Vec::new(),
            tip_history_enabled: false,
        }
    }
}
