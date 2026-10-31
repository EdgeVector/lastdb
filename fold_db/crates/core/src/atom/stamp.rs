//! Clock stamps for atom and molecule changes.
//!
//! The model types take each time as an argument and read no clock. These
//! extension traits keep the old call shape for core callers: each one reads
//! the system clock (and, for a local tip, the signer's public key) and passes
//! the values to the model. Import the trait at the call site.

use chrono::Utc;
use serde_json::Value;

use crate::security::Ed25519KeyPair;
use lastdb_model::atom::{
    Atom, AtomEntry, ChangeTime, ImportedTip, KeyMetadata, LocalTip, MoleculeHashRange,
};

/// Returns the current time in nanoseconds since the Unix epoch.
#[must_use]
pub fn now_nanos() -> u64 {
    crate::clock::unix_nanos()
}

/// Read the clock once for one molecule change: write time first, then the
/// molecule's `updated_at`, the same order the model methods used to read it.
fn change_time() -> ChangeTime {
    let written_at = now_nanos();
    ChangeTime {
        written_at,
        updated_at: Utc::now(),
    }
}

/// Stamp an [`Atom`] with the current time.
pub trait AtomClock {
    /// A new atom with a content-based UUID and the current time.
    fn new(source_schema_name: String, content: Value) -> Self;
}

impl AtomClock for Atom {
    fn new(source_schema_name: String, content: Value) -> Self {
        Self::new_at(source_schema_name, content, Utc::now())
    }
}

/// Stamp [`MoleculeHashRange`] changes with the current time and the local
/// signer.
pub trait MoleculeClock: Sized {
    /// A new empty molecule with a deterministic UUID.
    fn new(schema_name: &str, field_name: &str) -> Self;

    /// A new empty molecule under an already-assigned UUID.
    fn with_uuid(uuid: String) -> Self;

    /// Add a local tip with the mutation's signed author-clock identity. The
    /// LWW device is the signer's public key.
    fn set_atom_uuid_from_values_with_author(
        &mut self,
        hash_value: String,
        range_value: String,
        atom_uuid: String,
        keypair: &Ed25519KeyPair,
        logical_counter: u64,
        mutation_uuid: String,
    );

    /// Import a tip while preserving its signed author-clock identity. A tip
    /// without a signed write time takes the local clock.
    fn set_atom_uuid_from_values_imported_with_author(
        &mut self,
        hash_value: String,
        range_value: String,
        tip: ImportedTip,
    ) -> bool;

    /// Remove the reference at one key.
    fn remove_atom_uuid(&mut self, hash_value: &str, range_value: &str) -> Option<String>;

    /// Force-install a resident tip and its optional metadata.
    fn overlay_resident_tip(
        &mut self,
        hash: String,
        range: String,
        entry: AtomEntry,
        metadata: Option<KeyMetadata>,
    );
}

impl MoleculeClock for MoleculeHashRange {
    fn new(schema_name: &str, field_name: &str) -> Self {
        Self::new_at(schema_name, field_name, Utc::now())
    }

    fn with_uuid(uuid: String) -> Self {
        Self::with_uuid_at(uuid, Utc::now())
    }

    fn set_atom_uuid_from_values_with_author(
        &mut self,
        hash_value: String,
        range_value: String,
        atom_uuid: String,
        keypair: &Ed25519KeyPair,
        logical_counter: u64,
        mutation_uuid: String,
    ) {
        let tip = LocalTip {
            atom_uuid,
            device_id: keypair.public_key_base64(),
            logical_counter,
            mutation_uuid,
        };
        self.set_local_tip_at(hash_value, range_value, tip, change_time());
    }

    fn set_atom_uuid_from_values_imported_with_author(
        &mut self,
        hash_value: String,
        range_value: String,
        tip: ImportedTip,
    ) -> bool {
        self.set_imported_tip_at(hash_value, range_value, tip, change_time())
    }

    fn remove_atom_uuid(&mut self, hash_value: &str, range_value: &str) -> Option<String> {
        self.remove_atom_uuid_at(hash_value, range_value, Utc::now())
    }

    fn overlay_resident_tip(
        &mut self,
        hash: String,
        range: String,
        entry: AtomEntry,
        metadata: Option<KeyMetadata>,
    ) {
        self.overlay_resident_tip_at(hash, range, entry, metadata, Utc::now());
    }
}
