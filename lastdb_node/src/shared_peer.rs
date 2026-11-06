//! Explicit protected catch-up for an already enrolled shared writer.

use crate::byte_cloud_restore::RestoreError;
use crate::shared_restore::recovery::{
    exact_file_versions, open_log_plaintext, SnapshotSchema, MAX_SEALED_LOG_BYTES,
};
use crate::shared_restore::{resolve_schemas, schema_id};
use cloud_backup_client::shared_peer::{BackupTransportError, SharedPeerClient};
use cloud_backup_protocol::shared::peer::{PeerHeads, PeerHeadsRequest, PeerNextRequest};
use cloud_backup_protocol::shared::{StreamCut, WriterStream};
use cloud_backup_protocol::{DecimalU64, ErrorCode, Id};
use fold_db::fold_db_core::mutation_manager::portable_replay::{ReplayBinding, ReplayCandidate};
use fold_db::fold_db_core::MutationManager;
use fold_db::portable_mutation::StreamId;
use fold_db::portable_replay::{LogRangeId, VerifiedLogRange};
use fold_db::schema::SchemaError;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::disk_pack::NativeCloudWorkGuard;
use fold_db::FoldDB;
use schema_service_client::SchemaServiceClient;
use schema_types::PortableSchemaId;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};
mod files;
use files::read_exact;

const PROOF_MS: u64 = 4 * 60 * 1_000;

pub type Result<T> = std::result::Result<T, PeerError>;

#[derive(Debug)]
pub enum PeerOutcome {
    /// No unapplied range in the bounded group selected by this pass.
    Idle,
    Applied {
        stream: Id,
        through: u64,
    },
    /// The local position predates the accepted stream's retained prefix.
    NeedsCheckpoint {
        stream: Id,
    },
}

/// One admitted writer's device proof and content key.
pub struct PeerIdentity<'a> {
    pub device_signer: &'a Ed25519KeyPair,
    pub content_key: &'a [u8; 32],
}

/// Explicitly invoked, round-robin shared catch-up. Creating a worker starts no task.
pub struct PeerWorker<'a> {
    db: &'a FoldDB,
    client: &'a SharedPeerClient,
    schema_service: &'a SchemaServiceClient,
    identity: PeerIdentity<'a>,
    schemas: BTreeMap<PortableSchemaId, SnapshotSchema>,
}

impl<'a> PeerWorker<'a> {
    pub fn new(
        db: &'a FoldDB,
        client: &'a SharedPeerClient,
        schema_service: &'a SchemaServiceClient,
        identity: PeerIdentity<'a>,
        schemas: BTreeMap<PortableSchemaId, SnapshotSchema>,
    ) -> Self {
        Self {
            db,
            client,
            schema_service,
            identity,
            schemas,
        }
    }

    /// Verify and stage one retained range, then atomically apply its winning
    /// records, file versions and fully accounted progress.
    pub async fn step(&mut self) -> Result<(PeerOutcome, u64)> {
        let _admission = NativeCloudWorkGuard::acquire()
            .map_err(|_| RestoreError::Limit("active native cloud operation"))?;
        let db = self.db;
        let native = db.mutation_manager();
        let reader = native.portable_outgoing().await?.stream;
        let heads = self
            .client
            .heads(&PeerHeadsRequest { reader }, proof_expires()?, |bytes| {
                Ok(self.identity.device_signer.sign(bytes).to_bytes())
            })
            .await?;
        let outcome = self.apply_one(native, &heads).await?;
        let reclaimed = if matches!(outcome, PeerOutcome::NeedsCheckpoint { .. }) {
            0
        } else {
            native.reclaim_portable_deletes(&heads).await?
        };
        Ok((outcome, reclaimed))
    }

    async fn apply_one(
        &mut self,
        native: &MutationManager,
        heads: &PeerHeads,
    ) -> Result<PeerOutcome> {
        let reader = &heads.reader;
        let peers = heads
            .streams
            .iter()
            .map(|cut| Ok((cut, stream_id(&cut.source_stream)?)))
            .collect::<Result<Vec<_>>>()?;
        let streams = peers.iter().map(|(_, stream)| stream.clone()).collect();
        let progress = native.portable_peer_selection(&streams).await?;
        let start = peers.partition_point(|(_, stream)| {
            progress
                .last_applied_peer
                .as_ref()
                .is_some_and(|last| stream <= last)
        });
        for (cut, stream) in peers[start..].iter().chain(&peers[..start]) {
            let cursor = progress.cursors.get(stream);
            let after = cursor.map_or(0, |cursor| cursor.applied_through);
            let through = cut.head.as_ref().map_or(0, |head| head.sequence.get());
            if after > through {
                return Err(RestoreError::Invalid("peer head predates the local position").into());
            }
            if after == through {
                continue;
            }
            if cursor.is_some() {
                match self.apply(native, reader, stream).await {
                    Ok(Some(through)) => {
                        return Ok(PeerOutcome::Applied {
                            stream: cut.source_stream.clone(),
                            through,
                        })
                    }
                    Ok(None) => {}
                    Err(PeerError::Transport(BackupTransportError::Protocol(error)))
                        if error.code == ErrorCode::SourceUnavailable
                            && error.field == "shared.peer.reclaimed" =>
                    {
                        return Ok(PeerOutcome::NeedsCheckpoint {
                            stream: cut.source_stream.clone(),
                        });
                    }
                    Err(error) => return Err(error),
                }
            }
            if after < cut.reclaimed_through.get() {
                return Ok(PeerOutcome::NeedsCheckpoint {
                    stream: cut.source_stream.clone(),
                });
            }
            self.stage(native, reader, cut, stream.clone(), after, cursor.is_none())
                .await?;
            let through =
                self.apply(native, reader, stream)
                    .await?
                    .ok_or(RestoreError::Invalid(
                        "staged peer range did not reach replay",
                    ))?;
            return Ok(PeerOutcome::Applied {
                stream: cut.source_stream.clone(),
                through,
            });
        }
        Ok(PeerOutcome::Idle)
    }

    async fn stage(
        &self,
        native: &MutationManager,
        reader: &WriterStream,
        cut: &StreamCut,
        stream: StreamId,
        after: u64,
        admit: bool,
    ) -> Result<()> {
        let through = cut
            .head
            .clone()
            .ok_or(RestoreError::Invalid("missing peer head"))?;
        let request = PeerNextRequest {
            reader: reader.clone(),
            source_stream: cut.source_stream.clone(),
            after_sequence: DecimalU64::new(after),
            through,
        };
        let sign = |bytes: &[u8]| Ok(self.identity.device_signer.sign(bytes).to_bytes());
        let batch = self.client.next(&request, proof_expires()?, sign).await?;
        let id = LogRangeId::new(stream, after + 1, batch.through.get())
            .map_err(|_| RestoreError::Invalid("peer log range"))?;
        let object = self
            .client
            .segment(reader, &batch, MAX_SEALED_LOG_BYTES, proof_expires()?, sign)
            .await?;
        let sealed = read_exact(object, batch.segment.bytes.get()).await?;
        let mut plaintext =
            open_log_plaintext(self.identity.content_key, &reader.database_id, &sealed)?;
        let range = VerifiedLogRange::decode(id.clone(), &plaintext)
            .map_err(|_| RestoreError::Invalid("authenticated peer log range"))?;
        exact_file_versions(&range, batch)?;
        if admit {
            let first = range
                .entries()
                .first()
                .ok_or(RestoreError::Invalid("empty peer genesis"))?;
            native
                .admit_portable_peer(id.source_stream(), first.stamp.authored_at_ms)
                .await?;
        }
        native
            .receive_portable_range(id, std::mem::take(&mut *plaintext))
            .await?;
        Ok(())
    }

    async fn apply(
        &mut self,
        native: &MutationManager,
        reader: &WriterStream,
        stream: &StreamId,
    ) -> Result<Option<u64>> {
        let streams = BTreeSet::from([stream.clone()]);
        let entries = native.portable_peer_candidates(&streams).await?;
        if entries.is_empty() {
            return Ok(None);
        }
        let refs = entries.iter().collect::<Vec<_>>();
        resolve_schemas(self.db, self.schema_service, &mut self.schemas, &refs).await?;
        let candidates = entries
            .iter()
            .map(|entry| {
                let schema = self
                    .schemas
                    .get(&schema_id(&entry.schema)?)
                    .ok_or(RestoreError::Invalid("missing peer schema binding"))?;
                Ok(ReplayCandidate {
                    entry,
                    local_schema: &schema.local_schema,
                    definition: &schema.definition,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let needed = native.portable_replay_needed(&candidates).await?;
        let files = self.files(reader, &entries, needed).await?;
        let definitions = self
            .schemas
            .values()
            .map(|schema| {
                (
                    schema.definition.id().to_string(),
                    ReplayBinding {
                        local_schema: &schema.local_schema,
                        definition: &schema.definition,
                    },
                )
            })
            .collect();
        native
            .apply_portable_inbox(&streams, &definitions, files)
            .await?;
        let cursor = native.portable_peer_progress(&streams).await?;
        Ok(Some(
            cursor
                .get(stream)
                .ok_or(RestoreError::Invalid("missing peer cursor"))?
                .applied_through,
        ))
    }
}

fn proof_expires() -> Result<DecimalU64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|time| u64::try_from(time.as_millis()).ok())
        .and_then(|now| now.checked_add(PROOF_MS))
        .map(DecimalU64::new)
        .ok_or_else(|| RestoreError::Invalid("peer proof clock").into())
}

fn stream_id(id: &Id) -> Result<StreamId> {
    id.as_str()
        .parse()
        .map_err(|_| RestoreError::Invalid("peer stream identity").into())
}

#[derive(Debug)]
pub enum PeerError {
    Restore(RestoreError),
    Native(SchemaError),
    Transport(BackupTransportError),
}

impl fmt::Display for PeerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Restore(error) => fmt::Display::fmt(error, formatter),
            Self::Native(error) => fmt::Display::fmt(error, formatter),
            Self::Transport(error) => fmt::Display::fmt(error, formatter),
        }
    }
}

impl std::error::Error for PeerError {}

impl From<RestoreError> for PeerError {
    fn from(value: RestoreError) -> Self {
        Self::Restore(value)
    }
}

impl From<SchemaError> for PeerError {
    fn from(value: SchemaError) -> Self {
        Self::Native(value)
    }
}

impl From<BackupTransportError> for PeerError {
    fn from(value: BackupTransportError) -> Self {
        Self::Transport(value)
    }
}
