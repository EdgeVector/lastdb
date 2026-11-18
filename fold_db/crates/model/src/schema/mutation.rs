//! Mutation intents, wire operations, and sealed capability tokens for record file writes.

use super::types::{cas::CasExpectation, key_value::KeyValue};
use super::SchemaError;
use crate::portable_mutation::record_replacement::RecordTarget;
use crate::record_value::FileMarker;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Default)]
pub struct FileWriteIssuer {
    identity: Arc<()>,
}

struct Issued<T> {
    issuer: Arc<()>,
    marker: FileMarker,
    data: T,
}

impl<T: PartialEq> PartialEq for Issued<T> {
    fn eq(&self, o: &Self) -> bool {
        Arc::ptr_eq(&self.issuer, &o.issuer) && self.marker == o.marker && self.data == o.data
    }
}

#[derive(Clone, PartialEq)]
pub struct SealedRecordFile {
    inner: Arc<Issued<Vec<u8>>>,
}

impl SealedRecordFile {
    #[must_use]
    pub fn marker(&self) -> &FileMarker {
        &self.inner.marker
    }
}

#[derive(Clone, PartialEq)]
pub struct LocalFileReference {
    inner: Arc<Issued<PathBuf>>,
}

impl LocalFileReference {
    #[must_use]
    pub fn marker(&self) -> &FileMarker {
        &self.inner.marker
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.data
    }
}

impl FileWriteIssuer {
    #[must_use]
    pub fn issue(&self, marker: FileMarker, data: Vec<u8>) -> FileWrite {
        FileWrite::Put(SealedRecordFile {
            inner: Arc::new(Issued {
                issuer: Arc::clone(&self.identity),
                marker,
                data,
            }),
        })
    }

    #[must_use]
    pub fn issue_local(&self, marker: FileMarker, data: PathBuf) -> FileWrite {
        FileWrite::LocalPath(LocalFileReference {
            inner: Arc::new(Issued {
                issuer: Arc::clone(&self.identity),
                marker,
                data,
            }),
        })
    }

    #[must_use]
    pub fn admit(&self, file: SealedRecordFile) -> Option<Vec<u8>> {
        Arc::ptr_eq(&self.identity, &file.inner.issuer)
            .then(|| Arc::try_unwrap(file.inner).map_or_else(|p| p.data.clone(), |p| p.data))
    }

    #[must_use]
    pub fn admit_local<'a>(&self, file: &'a LocalFileReference) -> Option<&'a Path> {
        Arc::ptr_eq(&self.identity, &file.inner.issuer).then_some(file.path())
    }
}

#[derive(Clone, PartialEq)]
pub enum FileWrite {
    Put(SealedRecordFile),
    LocalPath(LocalFileReference),
    Clear,
}

impl FileWrite {
    #[must_use]
    pub fn row_len(&self) -> usize {
        self.sealed_row().map_or(0, <[u8]>::len)
    }

    #[must_use]
    pub fn sealed_row(&self) -> Option<&[u8]> {
        match self {
            Self::Put(file) => Some(&file.inner.data),
            Self::LocalPath(_) | Self::Clear => None,
        }
    }

    #[must_use]
    pub fn local_reference(&self) -> Option<&LocalFileReference> {
        match self {
            Self::LocalPath(file) => Some(file),
            Self::Put(_) | Self::Clear => None,
        }
    }
}

impl fmt::Debug for FileWrite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Put(file) => f
                .debug_struct("Put")
                .field("marker", file.marker())
                .field("row_bytes", &self.row_len())
                .finish(),
            Self::LocalPath(file) => f
                .debug_struct("LocalPath")
                .field("marker", file.marker())
                .finish_non_exhaustive(),
            Self::Clear => f.write_str("Clear"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContributionAdmission {
    pub definition_id: String,
    pub receipt_id: [u8; 32],
    pub intent_digest: [u8; 32],
    pub writer: String,
    pub schema_scope: bool,
    /// True when the authenticated operation originally created the record.
    pub creates_record: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MutationType {
    Create,
    Replace,
    Delete,
    Purge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MutationConvergence {
    Sync,
    #[default]
    Async,
}

impl MutationConvergence {
    #[must_use]
    pub fn waits(self) -> bool {
        matches!(self, Self::Sync)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MutationDurability {
    Durable,
    #[default]
    Queued,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Operation {
    #[serde(rename = "mutation")]
    Mutation {
        schema: String,
        #[serde(rename = "record")]
        record_fields: HashMap<String, Value>,
        key_value: KeyValue,
        alternates: BTreeMap<String, Option<KeyValue>>,
        mutation_type: MutationType,
        /// Ignored. Old proposal bodies may still carry it. The node does not store it.
        #[serde(default)]
        source_file_name: Option<String>,
        /// Ignored. Old proposal bodies may still carry it. The node does not store it.
        #[serde(default)]
        metadata: Option<HashMap<String, String>>,
        file: Option<FileMarker>,
        expected: CasExpectation,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        convergence: Option<MutationConvergence>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        durability: Option<MutationDurability>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        definition_id: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Mutation {
    pub uuid: String,
    pub schema_name: String,
    pub record_fields: HashMap<String, Value>,
    pub key_value: KeyValue,
    pub alternates: BTreeMap<String, Option<KeyValue>>,
    pub file: Option<FileMarker>,
    pub mutation_type: MutationType,
    pub source_file_name: Option<String>,
    pub metadata: Option<HashMap<String, String>>,
    /// Lowercase hex of the writer. The node sets this. It is not on the wire.
    pub writer: Option<String>,
    /// Set only when installing a saved record. Both values are copied, including when absent.
    pub copied_authorship: Option<(Option<String>, Option<String>)>,
    pub imported_written_at: Option<u64>,
    pub expected: CasExpectation,
    pub erase_caller: Option<&'static str>,
    pub erase_written_before_nanos: Option<u64>,
    pub file_write: Option<FileWrite>,
    pub inline_write: Option<crate::record_value::InlineWrite>,
    pub definition_id: Option<String>,
    pub contribution: Option<ContributionAdmission>,
}

impl Mutation {
    #[must_use]
    pub fn with_uuid(
        uuid: String,
        target: RecordTarget,
        record_fields: HashMap<String, Value>,
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
            mutation_type,
            source_file_name: None,
            metadata: None,
            writer: None,
            copied_authorship: None,
            imported_written_at: None,
            expected,
            erase_caller: None,
            erase_written_before_nanos: None,
            file_write: None,
            inline_write: None,
            definition_id: None,
            contribution: None,
        }
    }

    #[must_use]
    pub fn target(&self) -> RecordTarget {
        RecordTarget {
            schema: self.schema_name.clone(),
            key: self.key_value.clone(),
            alternates: self.alternates.clone(),
        }
    }

    pub fn validate_record_intent(&self) -> Result<(), SchemaError> {
        let invalid = |msg: &str| SchemaError::InvalidData(msg.into());
        if !(1..=128).contains(&self.uuid.len()) {
            return Err(invalid(
                "a mutation requires a stable id of at most 128 bytes",
            ));
        }
        let valid_cas = match (&self.mutation_type, &self.expected) {
            (MutationType::Create, CasExpectation::Absent) => true,
            (
                MutationType::Replace | MutationType::Delete | MutationType::Purge,
                CasExpectation::Revision { revision },
            ) => !revision.is_empty(),
            _ => false,
        };
        if !valid_cas {
            return Err(invalid(
                "create requires absent; replace/delete/purge require a record revision",
            ));
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
