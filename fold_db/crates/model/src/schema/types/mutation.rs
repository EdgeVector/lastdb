use super::cas::CasExpectation;
use super::{key_value::KeyValue, operations::MutationType};
use crate::record_replacement::RecordTarget;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

mod content_hash;
mod file_write;
pub use file_write::{FileWrite, FileWriteIssuer, LocalFileReference, SealedRecordFile};

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
    #[serde(rename = "record")]
    pub record_fields: HashMap<String, Value>,
    pub key_value: KeyValue,
    pub alternates: BTreeMap<String, Option<KeyValue>>,
    pub file: Option<crate::record_value::FileMarker>,
    pub pub_key: String,
    pub mutation_type: MutationType,
    pub synchronous: Option<bool>,
    /// Optional source filename for records created from file uploads
    pub source_file_name: Option<String>,
    /// Per-key metadata is part of the signed mutation, not the record UUID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    /// The author-clock write time. A local write gets it from the node
    /// clock. A mutation that arrives with one must carry a valid author-clock
    /// signature, and the record keeps that time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imported_written_at: Option<u64>,
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
    /// Required acceptance policy for the entire record.
    pub expected: CasExpectation,
    /// Local audit tag for the delete ledger row of a `Delete` that a node job
    /// issues, for example the TTL sweep. `None` writes the default caller.
    /// Never serialized, so it is never captured, synced or replayed.
    #[serde(skip)]
    pub erase_caller: Option<&'static str>,
    /// Local guard for a `Delete` that a node job issues on a key whose age is
    /// its write time (the TTL sweep on a Hash or Single schema): a record
    /// written at or after this time, in unix nanoseconds, is not erased. The
    /// write scope checks its current record stamp and revision, so a rewrite
    /// that is acked but not yet in the age rows survives. API serialization omits it;
    /// the signed mutation-intent envelope carries it explicitly for replay.
    #[serde(skip)]
    pub erase_written_before_nanos: Option<u64>,
    /// The file write of `put-blob` or `delete-blob`. Never serialized, so it is
    /// never captured, synced or replayed, and API input cannot forge it.
    #[serde(skip)]
    pub file_write: Option<FileWrite>,
    /// The inline file write of `put-blob` (its thumbnail), `put-inline` or
    /// `delete-blob`. Never serialized, like `file_write`. `None` keeps the
    /// record's current inline file.
    #[serde(skip)]
    pub inline_write: Option<crate::record_value::InlineWrite>,
}

impl Mutation {
    /// A new mutation with a caller-chosen id. The model makes no random id.
    #[must_use]
    pub fn with_uuid(
        uuid: String,
        target: RecordTarget,
        record_fields: HashMap<String, Value>,
        pub_key: String,
        mutation_type: MutationType,
        expected: CasExpectation,
    ) -> Self {
        Self {
            uuid,
            schema_name: target.schema,
            record_fields,
            key_value: target.key,
            alternates: target.alternates,
            file: None,
            pub_key,
            mutation_type,
            synchronous: None,
            source_file_name: None,
            metadata: None,
            imported_written_at: None,
            logical_counter: 0,
            author_clock_writer_id: String::new(),
            author_clock_signature: String::new(),
            author_clock_signature_version: 0,
            expected,
            erase_caller: None,
            erase_written_before_nanos: None,
            file_write: None,
            inline_write: None,
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

    /// Data-only target; it conveys no storage authority.
    pub fn target(&self) -> RecordTarget {
        RecordTarget {
            schema: self.schema_name.clone(),
            key: self.key_value.clone(),
            alternates: self.alternates.clone(),
        }
    }

    pub fn validate_record_intent(&self) -> Result<(), crate::schema::SchemaError> {
        use super::operations::MutationType;
        let invalid = |message: &str| crate::schema::SchemaError::InvalidData(message.into());
        if self.uuid.is_empty() || self.uuid.len() > 128 {
            return Err(invalid(
                "a mutation requires a stable id of at most 128 bytes",
            ));
        }
        match (&self.mutation_type, &self.expected) {
            (MutationType::Create, CasExpectation::Absent) => (),
            (
                MutationType::Replace | MutationType::Delete | MutationType::Purge,
                CasExpectation::Revision { revision },
            ) if !revision.is_empty() => (),
            _ => {
                return Err(invalid(
                    "create requires absent; replace/delete/purge require a record revision",
                ))
            }
        }
        if matches!(
            self.mutation_type,
            MutationType::Delete | MutationType::Purge
        ) && (!self.record_fields.is_empty()
            || self.file.is_some()
            || self.metadata.is_some()
            || self.source_file_name.is_some()
            || self.file_write.is_some()
            || self.inline_write.is_some())
        {
            return Err(invalid(
                "a deletion carries keys and revision, without record content",
            ));
        }
        Ok(())
    }
}
