use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::schema::types::declarative_schemas::FieldMapper;

/// Bundles all write-time provenance for a field mutation.
/// Contains the atom plus optional metadata that should be stored
/// per-key on the molecule (surviving atom dedup).
pub struct WriteContext {
    pub atom: crate::atom::Atom,
    pub pub_key: String,
    pub source_file_name: Option<String>,
    pub metadata: Option<std::collections::HashMap<String, String>>,
    pub schema_name: String,
    pub field_name: String,
    /// The signing keypair for molecule signatures. Used when
    /// `writer_override` is `None`.
    pub signer: std::sync::Arc<crate::security::Ed25519KeyPair>,
    /// Replay/import override. When `Some(Provenance::User { .. })`, the
    /// molecule's per-key write path stamps the caller-supplied
    /// `(pubkey, signature, signature_version)` directly onto the
    /// `AtomEntry` instead of re-signing locally with `signer`. This is
    /// how an inbound `data_share` from another node preserves the
    /// original sender's `writer_pubkey` on the receiver's AtomEntry,
    /// so the assertion `record.author_pub_key == sender.pub_key` can
    /// pass on HashRange schemas. `None` (the default) keeps the local
    /// signing path used by every first-party mutation.
    pub writer_override: Option<crate::atom::provenance::Provenance>,
    /// The `written_at` the original author SIGNED, for the import path.
    /// Only meaningful when `writer_override` is `Some`: the molecule
    /// canonical bytes include `written_at`, so preserving the sender's
    /// value is what keeps the imported signature verifiable at rest
    /// (`verify` / `verify_key`). `None` falls back to stamping the local
    /// clock — attribution preserved, signature unverifiable (legacy
    /// behavior).
    pub imported_written_at: Option<u64>,
    /// Signed per-device logical author counter. Zero on legacy writes.
    pub logical_counter: u64,
    /// Device identity that signed the mutation author clock.
    pub author_clock_writer_id: String,
    /// Stable mutation identity used by the final LWW tie-break.
    pub mutation_uuid: String,
    /// The `version` the original author SIGNED, for the import path.
    /// Only consumed by `SingleField` — the single-atom `Molecule` is the
    /// one variant whose canonical bytes include `version`. Ignored by the
    /// per-key variants (their entries do not sign a version).
    pub imported_version: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldCommon {
    pub molecule_uuid: Option<String>,
    pub field_mappers: HashMap<String, FieldMapper>,
    #[serde(default = "default_writable")]
    pub writable: bool,
    /// Org hash inherited from the parent schema.
    /// When set, all Sled keys for this field's data are prefixed with `{storage_prefix}:`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_prefix: Option<String>,
}

fn default_writable() -> bool {
    true
}

impl FieldCommon {
    pub fn new(field_mappers: HashMap<String, FieldMapper>) -> Self {
        Self {
            molecule_uuid: None,
            field_mappers,
            writable: true,
            storage_prefix: None,
        }
    }

    // Convenience methods to avoid repetition
    pub fn molecule_uuid(&self) -> Option<&String> {
        self.molecule_uuid.as_ref()
    }

    pub fn set_molecule_uuid(&mut self, uuid: String) {
        self.molecule_uuid = Some(uuid);
    }

    pub fn field_mappers(&self) -> &HashMap<String, FieldMapper> {
        &self.field_mappers
    }

    pub fn set_field_mappers(&mut self, mappers: HashMap<String, FieldMapper>) {
        self.field_mappers = mappers;
    }

    pub fn writable(&self) -> bool {
        self.writable
    }

    /// Build a storage key, prepending the storage-scope prefix when present.
    ///
    /// - Personal: `base_key` (e.g. `atom:{uuid}`, `mk:…`)
    /// - Scoped: `{prefix}:{base_key}` — share receive namespaces use
    ///   `from:{sender_hash}`; historical org hashes used the same mechanism
    pub fn storage_key(&self, base_key: &str) -> String {
        build_storage_key(self.storage_prefix.as_deref(), base_key)
    }

    pub fn storage_prefix(&self) -> Option<&str> {
        self.storage_prefix.as_deref()
    }

    pub fn set_storage_prefix(&mut self, storage_prefix: Option<String>) {
        self.storage_prefix = storage_prefix;
    }
}

/// Build a storage key, prepending an optional storage-scope prefix.
///
/// Used for personal (no prefix), share receive (`from:{sender}`), and
/// residual historical org-prefixed keys. Prefix is an isolation boundary —
/// readers must not dual-read bare keys when a prefix is set.
pub fn build_storage_key(storage_prefix: Option<&str>, base_key: &str) -> String {
    match storage_prefix {
        Some(hash) => format!("{hash}:{base_key}"),
        None => base_key.to_string(),
    }
}
