//! Reserved logical-sync wire shapes. They do not enable logical publication.

use crate::{CloudScope, DecimalU64, Digest, Id, ProtocolError, ReferenceRoot};
use serde::{Deserialize, Serialize};

/// Source stream identity, separate from the publisher's fenced ownership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalStream {
    pub scope: CloudScope,
    pub database_id: Digest,
    pub target_id: Digest,
    pub writer_id: Digest,
    pub writer_epoch: DecimalU64,
}

impl LogicalStream {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        if self.writer_epoch.get() == 0 {
            return Err(ProtocolError::invalid(
                "logical writer epoch must be positive",
            ));
        }
        Ok(())
    }
}

/// Reserved source payload codec. Origin signatures remain opaque and unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OriginCodec {
    LastdbMutationIntentV1,
}

/// Reserved families require their complete core and authority contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationFamily {
    RecordTransaction,
    SchemaAuthority,
    ProteinChange,
    FileDependency,
    OrgAuthority,
}

/// One complete source commit; a transport file does not define this boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalUnitReference {
    pub id: Id,
    pub sequence: DecimalU64,
    pub payload: Digest,
    pub origin_codec: OriginCodec,
    pub operation_family: OperationFamily,
    /// Includes the encrypted payload object itself, even with no external files.
    pub dependencies: ReferenceRoot,
}

impl LogicalUnitReference {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.dependencies.validate()?;
        if self.sequence.get() == 0 {
            return Err(ProtocolError::invalid(
                "logical unit sequence must be positive",
            ));
        }
        Ok(())
    }
}

/// Durable count within one publication, stored with the corresponding data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppliedCheckpoint {
    pub stream: LogicalStream,
    pub publication_id: Id,
    pub publication_digest: Digest,
    pub durable_unit_count: DecimalU64,
    pub native_current: Digest,
}

impl AppliedCheckpoint {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()
    }
}
