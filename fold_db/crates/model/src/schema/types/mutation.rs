use super::aggregate::AggregateSet;
use super::cas::CasExpectation;
use super::{key_value::KeyValue, operations::MutationType};
use crate::provenance::Provenance;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

mod content_hash;

pub fn is_zero_u8(value: &u8) -> bool {
    *value == 0
}

pub fn is_zero_u64(value: &u64) -> bool {
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
    /// Optional source filename for records created from file uploads
    pub source_file_name: Option<String>,
    /// Per-key metadata is part of the signed mutation, not the record UUID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    /// Writer signature of a signed import. A record names no writer
    /// (decision D7), so a record write refuses a mutation that carries one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<Provenance>,
    /// The `written_at` the original author SIGNED, carried alongside
    /// `provenance` on the import path (inbound `data_share`). The field key
    /// canonical bytes include `written_at`, so the field layer must stamp
    /// the SIGNED value — not a local clock — for the imported signature to
    /// stay verifiable at rest. `None` (with `provenance: Some`) keeps the
    /// attribution-only import.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imported_written_at: Option<u64>,
    /// Original UUID from a MutationIntent envelope, including an empty
    /// UUID. Suppressed history uses it without changing replay LWW.
    /// API input cannot forge this internal import marker.
    #[serde(skip)]
    pub replayed_source_mutation_uuid: Option<String>,
    /// Internal replay provenance; wire input cannot skip the durable floor.
    #[serde(skip)]
    pub replay_cutoff_required: bool,
    /// Sync device that wrote this mutation: this node's sync device id for a
    /// local write, the log writer for a replayed one. Empty when unknown.
    /// Internal and never on the wire: API input cannot forge it, and it is
    /// not part of [`Self::content_hash`].
    #[serde(skip)]
    pub origin_device: String,
    /// Durable per-device logical author counter. A mutation without one uses zero.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub logical_counter: u64,
    /// Device key that signed the mutation author clock. This is separate from
    /// `pub_key`, which remains the request/content attribution identity.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub author_clock_writer_id: String,
    /// Ed25519 signature over the mutation content, id, and author clock.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub author_clock_signature: String,
    /// Zero for unsigned input; see `MUTATION_AUTHOR_SIGNATURE_VERSION` for signed input.
    #[serde(default, skip_serializing_if = "is_zero_u8")]
    pub author_clock_signature_version: u8,
    /// The `version` the original author SIGNED — consumed only by single
    /// (non-keyed) fields, whose `Field key` canonical bytes include
    /// `version`. Per-key entries don't sign a version and ignore this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imported_version: Option<u64>,
    /// Optional compare-and-set precondition. When `Some`, the node applies
    /// this mutation atomically **iff** the current state at `key_value`
    /// matches the expectation (same-node atomicity); on a mismatch it writes
    /// nothing and returns
    /// [`SchemaError::CasConflict`](super::errors::SchemaError::CasConflict).
    ///
    /// A CAS precondition is local acceptance policy, not written content.
    /// Mutations that differ only in `expected` share a content hash so a
    /// successful signed retry can return its prior acknowledgement.
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
    /// projected field has a record on it, so that row is invisible to every
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
    pub aggregate_derived_internal: bool,
    /// Local audit tag for the delete ledger row of a `Delete` that a node job
    /// issues, for example the TTL sweep. `None` writes the default caller.
    /// Never serialized, so it is never captured, synced or replayed.
    #[serde(skip)]
    pub erase_caller: Option<&'static str>,
    /// Local guard for a `Delete` that a node job issues on a key whose age is
    /// its write time (the TTL sweep on a Hash or Single schema): a record
    /// written at or after this time, in unix nanoseconds, is not erased. The
    /// write scope reads the record's newest field stamp and puts the delete
    /// floor at this time, so a rewrite that is acked but not yet in the age
    /// rows survives. Ordinary API serialization omits it;
    /// the signed mutation-intent envelope carries it explicitly for replay.
    #[serde(skip)]
    pub erase_written_before_nanos: Option<u64>,
}

impl Mutation {
    /// A new mutation with a caller-chosen id. The model makes no random id;
    /// core's `MutationIds::new` supplies a uuid v4.
    #[must_use]
    pub fn with_uuid(
        uuid: String,
        schema_name: String,
        fields_and_values: HashMap<String, Value>,
        key_value: KeyValue,
        pub_key: String,
        mutation_type: MutationType,
    ) -> Self {
        Self {
            uuid,
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
            replayed_source_mutation_uuid: None,
            replay_cutoff_required: false,
            origin_device: String::new(),
            logical_counter: 0,
            author_clock_writer_id: String::new(),
            author_clock_signature: String::new(),
            author_clock_signature_version: 0,
            imported_version: None,
            expected: None,
            must_exist: None,
            aggregate_set: None,
            aggregate_derived_internal: false,
            erase_caller: None,
            erase_written_before_nanos: None,
        }
    }

    /// Name the node job that issues this `Delete`. The delete ledger row
    /// carries the name as its caller, so an audit read can tell the job from
    /// a client delete. Local only: it is not captured or synced.
    #[must_use]
    pub fn with_erase_caller(mut self, caller: &'static str) -> Self {
        self.erase_caller = Some(caller);
        self
    }

    /// Erase only records written before `nanos` (unix nanoseconds). See the field.
    #[must_use]
    pub fn with_erase_written_before_nanos(mut self, nanos: u64) -> Self {
        self.erase_written_before_nanos = Some(nanos);
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
}
