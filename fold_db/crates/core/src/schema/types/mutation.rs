use super::aggregate::AggregateSet;
use super::cas::CasExpectation;
use super::{key_value::KeyValue, operations::MutationType};
use crate::atom::provenance::Provenance;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use uuid::Uuid;

pub(crate) fn is_zero_u8(value: &u8) -> bool {
    *value == 0
}

pub(crate) fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Mutation {
    pub uuid: String,
    pub schema_name: String,
    pub fields_and_values: HashMap<String, Value>,
    pub key_value: KeyValue,
    pub pub_key: String,
    pub mutation_type: MutationType,
    pub synchronous: Option<bool>,
    /// Optional source filename for atoms created from file uploads
    pub source_file_name: Option<String>,
    /// General-purpose metadata (e.g., file_hash, provenance info).
    /// Excluded from content_hash — metadata doesn't affect deduplication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    /// Writer identity and verifiability info. Additive during the
    /// `projects/molecule-provenance-dag` migration: `None` on mutations
    /// constructed before provenance wire-through; `Some(Provenance::User{..})`
    /// once a signature is available at construction. Kept alongside
    /// `pub_key` (not in place of) until the full wire-through lands and
    /// a follow-up PR removes `pub_key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<Provenance>,
    /// The `written_at` the original author SIGNED, carried alongside
    /// `provenance` on the import path (inbound `data_share`). The molecule
    /// canonical bytes include `written_at`, so the field layer must stamp
    /// the SIGNED value — not a local clock — for the imported signature to
    /// stay verifiable at rest. `None` (with `provenance: Some`) keeps the
    /// legacy attribution-only import.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imported_written_at: Option<u64>,
    /// Original UUID from a MutationIntent envelope, including an empty
    /// legacy UUID. Suppressed history uses it without changing replay LWW.
    /// API input cannot forge this internal import marker.
    #[cfg(feature = "cloud-sync")]
    #[serde(skip)]
    pub(crate) replayed_source_mutation_uuid: Option<String>,
    /// Durable per-device logical author counter. Legacy mutations use zero.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub logical_counter: u64,
    /// Device key that signed the mutation author clock. This is separate from
    /// `pub_key`, which remains the request/content attribution identity.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub author_clock_writer_id: String,
    /// Ed25519 signature over the mutation content, id, and author clock.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub author_clock_signature: String,
    /// Zero for legacy envelopes; two for the author-clock signature scheme.
    #[serde(default, skip_serializing_if = "is_zero_u8")]
    pub author_clock_signature_version: u8,
    /// The `version` the original author SIGNED — consumed only by single
    /// (non-keyed) fields, whose `Molecule` canonical bytes include
    /// `version`. Per-key entries don't sign a version and ignore this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imported_version: Option<u64>,
    /// Optional compare-and-set precondition. When `Some`, the node applies
    /// this mutation atomically **iff** the current state at `key_value`
    /// matches the expectation (same-node atomicity); on a mismatch it writes
    /// nothing and returns
    /// [`SchemaError::CasConflict`](super::errors::SchemaError::CasConflict).
    ///
    /// Additive, like `provenance` above: `None` on every mutation constructed
    /// before CAS existed, and excluded from [`Self::content_hash`] so the
    /// idempotency cache and sync log keep their pre-existing hashes. A CAS
    /// precondition is a decision about *when* to apply, not part of the
    /// written content, so two mutations that differ only in `expected` share
    /// a content hash by design.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected: Option<CasExpectation>,
    /// Local request policy for [`MutationType::Delete`] and
    /// [`MutationType::Update`]: `Some(true)` is loud-miss (must-exist).
    /// Never captured on `MutationIntent` / CDC. Excluded from
    /// [`Self::content_hash`] like `expected`.
    ///
    /// On `Delete` it refuses a missing erasure target. On `Update` it refuses
    /// a key that carries no live value in this schema yet — the guard against
    /// a narrow update MINTING a row. Without it, `update` is a silent upsert,
    /// and a narrow update at a key that turns out not to exist creates a row
    /// carrying only the fields it sent. A query returns a row only when every
    /// projected field has an atom on it, so that row is invisible to every
    /// wide reader while sitting in the partition.
    ///
    /// The flag is deliberately opt-in rather than the `Update` default:
    /// refusing every missing-row update would break existing callers that
    /// rely on today's upsert. Additive here, breaking there.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub must_exist: Option<bool>,
    /// Optional replacement of this source row's contribution to one derived
    /// aggregate. The cloud log carries this concrete vector with the source
    /// row's winner clock. It never carries a computed total as authoritative
    /// state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate_set: Option<AggregateSet>,
    /// Internal-only capability for derived aggregate member/summary writes.
    /// Serde always clears it, so API or replay input cannot forge the bypass.
    #[serde(skip)]
    pub(crate) aggregate_derived_internal: bool,
}

impl Mutation {
    #[must_use]
    pub fn new(
        schema_name: String,
        fields_and_values: HashMap<String, Value>,
        key_value: KeyValue,
        pub_key: String,
        mutation_type: MutationType,
    ) -> Self {
        Self {
            uuid: Uuid::new_v4().to_string(),
            schema_name,
            fields_and_values,
            key_value,
            pub_key,
            mutation_type,
            synchronous: None,
            source_file_name: None,
            metadata: None,
            provenance: None,
            imported_written_at: None,
            #[cfg(feature = "cloud-sync")]
            replayed_source_mutation_uuid: None,
            logical_counter: 0,
            author_clock_writer_id: String::new(),
            author_clock_signature: String::new(),
            author_clock_signature_version: 0,
            imported_version: None,
            expected: None,
            must_exist: None,
            aggregate_set: None,
            aggregate_derived_internal: false,
        }
    }

    #[must_use]
    pub fn with_source_file_name(mut self, file_name: String) -> Self {
        self.source_file_name = Some(file_name);
        self
    }

    #[must_use]
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    #[must_use]
    pub fn with_provenance(mut self, provenance: Provenance) -> Self {
        self.provenance = Some(provenance);
        self
    }

    /// Attach a compare-and-set precondition. The node will apply this
    /// mutation only if the current state at its key matches `expected`,
    /// atomically with respect to other same-key writes on the node; on a
    /// mismatch it returns
    /// [`SchemaError::CasConflict`](super::errors::SchemaError::CasConflict)
    /// and writes nothing.
    #[must_use]
    pub fn with_expected(mut self, expected: CasExpectation) -> Self {
        self.expected = Some(expected);
        self
    }

    /// Local must-exist policy for a user `Delete` or `Update`. Not replayed.
    #[must_use]
    pub fn with_must_exist(mut self, must_exist: bool) -> Self {
        self.must_exist = Some(must_exist);
        self
    }

    /// Attach a concrete aggregate-member replacement to this source write.
    #[must_use]
    pub fn with_aggregate_set(mut self, aggregate_set: AggregateSet) -> Self {
        self.aggregate_set = Some(aggregate_set);
        self
    }

    /// Request-shape check: `must_exist` is legal on delete and update (or as
    /// a no-op `true` on the purge alias). Illegal combos are `InvalidData`.
    ///
    /// `Create` stays refused because it is already an upsert by contract —
    /// "create this, and fail if it is absent" has no meaning. Use
    /// `expected: Absent` for create-if-absent.
    pub fn reject_illegal_must_exist(&self) -> Result<(), crate::schema::SchemaError> {
        use super::operations::MutationType;
        match (&self.mutation_type, self.must_exist) {
            (MutationType::Create, Some(_)) => Err(crate::schema::SchemaError::InvalidData(
                "must_exist is only valid on delete or update".to_string(),
            )),
            (MutationType::Purge, Some(false)) => Err(crate::schema::SchemaError::InvalidData(
                "purge cannot set must_exist: false".to_string(),
            )),
            _ => Ok(()),
        }
    }

    /// Compute a deterministic content hash of this mutation's semantic fields.
    /// Excludes uuid (random), synchronous (execution mode), source_file_name, metadata.
    ///
    /// `provenance` contributes to the hash only when `Some`. When `None`, the
    /// output bytes are identical to pre-PR-4 mutations (`molecule-provenance-dag`
    /// PR 4 additive field). This is a non-negotiable backward-compatibility
    /// guarantee — the idempotency cache and sync log hold pre-PR-4 hashes, and
    /// breaking them breaks deduplication and replay.
    #[must_use]
    pub fn content_hash(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.schema_name.as_bytes());
        hasher.update(
            serde_json::to_string(&self.mutation_type)
                .expect("MutationType is always serializable")
                .as_bytes(),
        );
        hasher.update(
            serde_json::to_string(&self.key_value)
                .expect("KeyValue is always serializable")
                .as_bytes(),
        );
        // Sort keys for deterministic ordering of HashMap
        let mut sorted_fields: Vec<_> = self.fields_and_values.iter().collect();
        sorted_fields.sort_by_key(|(k, _)| (*k).clone());
        for (k, v) in sorted_fields {
            hasher.update(k.as_bytes());
            hasher.update(
                serde_json::to_string(v)
                    .expect("serde_json::Value is always serializable")
                    .as_bytes(),
            );
        }
        hasher.update(self.pub_key.as_bytes());
        if let Some(p) = &self.provenance {
            hasher.update(
                serde_json::to_string(p)
                    .expect("Provenance is always serializable")
                    .as_bytes(),
            );
        }
        // Same additive contract as `provenance`: contribute only when Some,
        // so every pre-existing mutation's hash bytes are unchanged.
        if let Some(w) = self.imported_written_at {
            hasher.update(w.to_be_bytes());
        }
        if self.logical_counter != 0 {
            hasher.update(self.logical_counter.to_be_bytes());
        }
        if !self.author_clock_writer_id.is_empty() {
            hasher.update(self.author_clock_writer_id.as_bytes());
        }
        if let Some(v) = self.imported_version {
            hasher.update(v.to_be_bytes());
        }
        // The contribution is part of the source mutation's signed semantic
        // content. Without this, a peer could retain the source winner while
        // changing the member value that derives its local summary.
        if let Some(aggregate_set) = &self.aggregate_set {
            hasher.update(
                serde_json::to_string(aggregate_set)
                    .expect("AggregateSet is always serializable")
                    .as_bytes(),
            );
        }
        let result = hasher.finalize();
        format!("{result:x}")
    }
}
