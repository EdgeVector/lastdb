use super::{error, validate_object_list, LogHead, SealedObject, WriterStream, MAX_WRITERS};
use crate::{input_digest, limits, Digest, ErrorCode, Id, ProtocolError};
use serde::{Deserialize, Serialize};

/// Exact accepted prefix absorbed by a snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamCover {
    /// Origin stream inside the manifest's database.
    pub source_stream: Id,
    /// An accepted batch end and its prefix commitment.
    pub through: LogHead,
}

/// Public snapshot commitment. The encrypted manifest carries the native file paths.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotManifest {
    /// Stable shared database identity.
    pub database_id: Id,
    /// Stable id for publication and lost-response reconciliation.
    pub snapshot_id: Id,
    /// Expected predecessor id, absent for the first snapshot.
    pub replaces: Option<Id>,
    /// The disk reader's format identifier. Zero is reserved.
    pub disk_format: u32,
    /// Absorbed prefixes, sorted by unique source stream ids.
    pub covers: Vec<StreamCover>,
    /// Encrypted manifest with the exact paths and file dependencies.
    pub encrypted_manifest: SealedObject,
    /// Number of distinct exact objects needed in addition to the manifest.
    pub object_count: u32,
    /// Commitment to the ordered inventory of those exact sealed objects.
    pub inventory_sha256: Digest,
}

impl SnapshotManifest {
    /// Check the format marker, unique ordered covers, and metadata limits.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.disk_format == 0 || self.replaces.as_ref() == Some(&self.snapshot_id) {
            return Err(ProtocolError::invalid("shared.snapshot.identity"));
        }
        if self.covers.len() > MAX_WRITERS || self.object_count as usize > limits::NATIVE_FILES_MAX
        {
            return Err(ProtocolError::limit("shared.snapshot.inventory"));
        }
        if self
            .covers
            .windows(2)
            .any(|pair| pair[0].source_stream >= pair[1].source_stream)
        {
            return Err(ProtocolError::invalid("shared.snapshot.cover_order"));
        }
        for cover in &self.covers {
            cover.through.validate()?;
        }
        self.encrypted_manifest.validate()?;
        limits::encoded_json_bytes(self, limits::REQUEST_JSON_MAX_BYTES)?;
        Ok(())
    }

    /// Find the snapshot's starting position for a stream after validating the manifest.
    #[must_use]
    pub fn cover(&self, stream: &Id) -> Option<&LogHead> {
        let index = self
            .covers
            .binary_search_by(|cover| cover.source_stream.cmp(stream))
            .ok()?;
        Some(&self.covers[index].through)
    }

    /// Derive the publication identity for comparison with a service receipt.
    pub fn receipt(&self) -> Result<SnapshotReceipt, ProtocolError> {
        self.validate()?;
        Ok(SnapshotReceipt {
            database_id: self.database_id.clone(),
            snapshot_id: self.snapshot_id.clone(),
            manifest_sha256: input_digest("lastdb:shared-log:snapshot:v1", self)?,
        })
    }

    /// Verify that the exact object inventory matches the manifest commitment.
    pub fn validate_inventory(&self, objects: &[SealedObject]) -> Result<(), ProtocolError> {
        if objects.len() != self.object_count as usize
            || inventory_sha256(objects)? != self.inventory_sha256
            || objects
                .iter()
                .any(|object| object.version_id == self.encrypted_manifest.version_id)
        {
            return Err(error(
                ErrorCode::IntegrityMismatch,
                "shared.snapshot.objects",
            ));
        }
        Ok(())
    }
}

/// Commit a bounded exact inventory, sorted by unique sealed object version ids.
pub fn inventory_sha256(objects: &[SealedObject]) -> Result<Digest, ProtocolError> {
    if objects.len() > limits::NATIVE_FILES_MAX {
        return Err(ProtocolError::limit("shared.snapshot.objects"));
    }
    validate_object_list(objects)?;
    input_digest("lastdb:shared-log:inventory:v1", &objects)
}

/// Immutable identity returned from the service's accepted publication store.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotReceipt {
    /// Database that owns the snapshot.
    pub database_id: Id,
    /// Stable publication id.
    pub snapshot_id: Id,
    /// Commitment to the entire public manifest.
    pub manifest_sha256: Digest,
}

/// Authoritative facts from the transaction that conditionally publishes a snapshot.
pub struct PublicationContext<'a> {
    /// Database from the service authorization and metadata lookup.
    pub database_id: &'a Id,
    /// Current designated publisher recorded for this database.
    pub publisher_device: &'a Id,
    /// Device derived from the request's authenticated principal.
    pub authenticated_device: &'a Id,
    /// Currently accepted manifest, absent before the first publication.
    pub current: Option<&'a SnapshotManifest>,
    /// Durable receipt lookup at the candidate publication id in this database.
    pub accepted_at_id: Option<&'a SnapshotReceipt>,
}

/// Publication outcomes before the enclosing service transaction commits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublicationDecision {
    /// Pin the exact inventory, save this receipt and replace the expected head atomically.
    Commit(SnapshotReceipt),
    /// Return the durable receipt for the identical previously accepted publication.
    AlreadyAccepted(SnapshotReceipt),
}

impl PublicationContext<'_> {
    /// Check publisher, predecessor, monotonic covers and exact durable inventory.
    /// Callbacks look up accepted batch ends and verified versions in this database.
    pub fn decide(
        &self,
        manifest: &SnapshotManifest,
        objects: &[SealedObject],
        mut accepted_end: impl FnMut(&WriterStream, &LogHead) -> Result<bool, ProtocolError>,
        mut confirm_object: impl FnMut(&Id, &SealedObject) -> Result<(), ProtocolError>,
    ) -> Result<PublicationDecision, ProtocolError> {
        if self.database_id != &manifest.database_id
            || self.authenticated_device != self.publisher_device
        {
            return Err(error(ErrorCode::Unauthorized, "shared.snapshot.publisher"));
        }
        let receipt = manifest.receipt()?;
        if let Some(accepted) = self.accepted_at_id {
            if *accepted != receipt {
                return Err(error(
                    ErrorCode::IdentityConflict,
                    "shared.snapshot.receipt",
                ));
            }
            return Ok(PublicationDecision::AlreadyAccepted(receipt));
        }
        if self.current.map(|current| &current.snapshot_id) != manifest.replaces.as_ref() {
            return Err(error(
                ErrorCode::PredecessorConflict,
                "shared.snapshot.replaces",
            ));
        }
        validate_covers(manifest, self.current, &mut accepted_end)?;
        manifest.validate_inventory(objects)?;
        for object in std::iter::once(&manifest.encrypted_manifest).chain(objects) {
            confirm_object(self.database_id, object)?;
        }
        Ok(PublicationDecision::Commit(receipt))
    }
}

fn validate_covers(
    candidate: &SnapshotManifest,
    current: Option<&SnapshotManifest>,
    mut accepted_end: impl FnMut(&WriterStream, &LogHead) -> Result<bool, ProtocolError>,
) -> Result<(), ProtocolError> {
    if let Some(current) = current {
        current.validate()?;
        if current.database_id != candidate.database_id {
            return Err(error(
                ErrorCode::IntegrityMismatch,
                "shared.snapshot.predecessor_scope",
            ));
        }
        for previous in &current.covers {
            let next = candidate.cover(&previous.source_stream).ok_or_else(|| {
                error(
                    ErrorCode::PredecessorConflict,
                    "shared.snapshot.missing_cover",
                )
            })?;
            if next.sequence < previous.through.sequence
                || (next.sequence == previous.through.sequence && *next != previous.through)
            {
                return Err(error(
                    ErrorCode::PredecessorConflict,
                    "shared.snapshot.cover",
                ));
            }
        }
    }
    for cover in &candidate.covers {
        if current.and_then(|current| current.cover(&cover.source_stream)) == Some(&cover.through) {
            continue;
        }
        let stream = WriterStream {
            database_id: candidate.database_id.clone(),
            source_stream: cover.source_stream.clone(),
        };
        if !accepted_end(&stream, &cover.through)? {
            return Err(error(
                ErrorCode::MissingDependency,
                "shared.snapshot.batch_end",
            ));
        }
    }
    Ok(())
}
