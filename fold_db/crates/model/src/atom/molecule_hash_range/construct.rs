//! Construction and per-key materialization.

use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, HashMap};

use super::MoleculeHashRange;
use crate::atom::{deterministic_molecule_uuid, AtomEntry, KeyMetadata};

impl MoleculeHashRange {
    /// Creates a new empty MoleculeHashRange with a deterministic UUID.
    ///
    /// The caller reads the clock: `updated_at` is the creation time.
    #[must_use]
    pub fn new_at(schema_name: &str, field_name: &str, updated_at: DateTime<Utc>) -> Self {
        Self::empty(
            deterministic_molecule_uuid(schema_name, field_name),
            updated_at,
        )
    }

    fn empty(uuid: String, updated_at: DateTime<Utc>) -> Self {
        Self {
            uuid,
            atom_uuids: HashMap::new(),
            updated_at,
            order_is_tail: false,
            version: 0,
            key_metadata: HashMap::new(),
        }
    }

    /// Whether this molecule is a write-only tail rather than a full snapshot.
    ///
    /// The persist path refuses a full rewrite when this is true, and a tail
    /// takes the shared append guard. The marker is not an order log.
    pub fn order_is_tail(&self) -> bool {
        self.order_is_tail
    }

    /// Rebuild from per-key records. Entries are restored **verbatim** — no
    /// re-signing. The result is a full snapshot (`order_is_tail` is false).
    pub fn from_per_key_records(
        uuid: String,
        version: u64,
        updated_at: DateTime<Utc>,
        records: Vec<(String, String, AtomEntry, Option<KeyMetadata>)>,
    ) -> Self {
        Self::from_records(uuid, version, updated_at, records, false)
    }

    fn from_records(
        uuid: String,
        version: u64,
        updated_at: DateTime<Utc>,
        records: Vec<(String, String, AtomEntry, Option<KeyMetadata>)>,
        order_is_tail: bool,
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
            order_is_tail,
            version,
            key_metadata,
        }
    }
}
