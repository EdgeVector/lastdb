//! Apply an owner-only log without replacing an unpublished local edit.

use cloud_backup_protocol::canonical_bytes;
use fold_db::fold_db_core::mutation_manager::ReplayStamp;
use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::{KeyValue, SchemaError};
use sha2::{Digest, Sha256};

use super::decode::{EntryDecoder, Prepared};
use super::payload::{Change, RecordChange, RecordFile, SchemaChange};
use super::{apply, DeliveredEntry, RestoreError, SnapshotEntry, SnapshotFileSource};
use crate::Host;

mod record;
use record::{RecordFetch, RecordReplay};

const DETAIL: &str = "owned log entry";

pub(crate) async fn replay_owned(
    host: &Host,
    database_id: &str,
    local_device: Option<&str>,
    delivered: &[DeliveredEntry],
    files: &mut (dyn SnapshotFileSource + Send),
) -> Result<(), RestoreError> {
    let target = super::resolve_target(host, database_id)?;
    if !target.private || !matches!(&target.place, super::Place::Owned(_)) {
        return Err(RestoreError::Scope("owner database store"));
    }
    let owner = hex::encode(host.keypair.public_key_bytes());
    super::page::check_page(database_id, &owner, delivered)?;
    let store = super::open_store(host, database_id, &target, true).await?;
    let mut replay = OwnerReplay {
        store: &store,
        decoder: target.decoder(database_id),
        owner: &owner,
        local_device,
        files,
    };
    for item in delivered {
        replay.one(&item.entry).await?;
    }
    Ok(())
}

pub(super) struct OwnerReplay<'a> {
    pub store: &'a FoldDB,
    pub decoder: EntryDecoder<'a>,
    pub owner: &'a str,
    pub local_device: Option<&'a str>,
    pub files: &'a mut (dyn SnapshotFileSource + Send),
}

impl OwnerReplay<'_> {
    pub(super) async fn one(&mut self, entry: &SnapshotEntry) -> Result<(), RestoreError> {
        if entry.writer_key != self.owner || entry.incorporated_from.is_some() {
            return Err(RestoreError::Scope(DETAIL));
        }
        let prepared = self.decoder.open(entry)?;
        let identity = Receipt::new(self.decoder.database_id, entry, &prepared)?;
        let is_local = self.local_device == Some(entry.writer_device.as_str());
        match prepared.change {
            Change::Record(record) => {
                self.record(entry, identity, &prepared.id, *record, is_local)
                    .await
            }
            Change::Schema(schema) => self.schema(identity, &prepared.id, *schema, is_local).await,
        }
    }

    async fn record(
        &mut self,
        entry: &SnapshotEntry,
        identity: Receipt,
        mutation_id: &str,
        mut change: RecordChange,
        is_local: bool,
    ) -> Result<(), RestoreError> {
        let manager = self.store.mutation_manager();
        let mut replay = RecordReplay {
            entry,
            mutation_id,
            is_local,
            schema: change.mutation.schema_name.clone(),
            key: change.mutation.key_value.clone(),
            stamp: stamp(entry),
            explicit_attachment: !matches!(change.file, RecordFile::Keep)
                || change.inline.is_some(),
            incoming_ready: !matches!(change.file, RecordFile::Fetch(_)),
            prior: None,
        };
        if replay.incoming_ready {
            self.stage(entry, &mut change).await?;
        }
        loop {
            let mut fetch = None;
            let result = manager
                .write_replay_once(
                    &identity.id,
                    &identity.digest,
                    replay.attempt(manager, &change.mutation, &mut fetch),
                )
                .await;

            // Preparation moves slow file I/O outside the native write scope.
            // The retry reevaluates the current record, pending marker, and receipt.
            match fetch {
                Some(RecordFetch::Incoming) => {
                    self.stage(entry, &mut change).await?;
                    replay.incoming_ready = true;
                }
                Some(RecordFetch::Prior(saved)) => {
                    let change = self
                        .prepare_prior(&replay.schema, &replay.key, &saved, &replay.stamp)
                        .await?
                        .ok_or(RestoreError::Conflict(DETAIL))?;
                    replay.prior = Some((saved, change.mutation));
                }
                None => return result.map(|_| ()).map_err(|error| replay_error(&error)),
            }
        }
    }

    async fn prepare_prior(
        &mut self,
        schema: &str,
        key: &KeyValue,
        bytes: &[u8],
        incoming: &ReplayStamp,
    ) -> Result<Option<RecordChange>, RestoreError> {
        let saved: SnapshotEntry =
            serde_json::from_slice(bytes).map_err(|_| RestoreError::Durable(DETAIL))?;
        if stamp(&saved) <= *incoming {
            return Ok(None);
        }
        let Prepared {
            change: Change::Record(mut change),
            ..
        } = self.decoder.open(&saved)?
        else {
            return Err(RestoreError::Conflict(DETAIL));
        };
        if change.mutation.schema_name != schema
            || !self
                .store
                .mutation_manager()
                .replay_record_key_matches(schema, key, &change.mutation.key_value)
                .map_err(|error| replay_error(&error))?
        {
            return Err(RestoreError::Conflict(DETAIL));
        }
        self.stage(&saved, &mut change).await?;
        Ok(Some(*change))
    }

    async fn stage(
        &mut self,
        entry: &SnapshotEntry,
        change: &mut RecordChange,
    ) -> Result<(), RestoreError> {
        apply::stage_file(
            self.store,
            self.decoder.key,
            self.decoder.database_id,
            entry,
            change,
            self.files,
        )
        .await
    }

    async fn schema(
        &mut self,
        identity: Receipt,
        mutation_id: &str,
        change: SchemaChange,
        is_local: bool,
    ) -> Result<(), RestoreError> {
        let manager = self.store.mutation_manager();
        if change.definition.is_none() && !is_local {
            return self.drop_schema(identity, change).await;
        }
        manager
            .write_replay_once(&identity.id, &identity.digest, async {
                if is_local {
                    manager
                        .clear_pending_replay_schema(&change.name, mutation_id)
                        .await
                } else if manager.pending_replay_schema(&change.name).await?.is_none() {
                    apply::apply_schema(self.store, change)
                        .await
                        .map_err(schema_error)
                } else if let Some(definition) = change.definition.as_ref() {
                    if manager.pending_replay_schema_matches(definition).await? {
                        Ok(())
                    } else {
                        Err(SchemaError::Blocked(
                            "local schema change awaits cloud delivery".into(),
                        ))
                    }
                } else {
                    Err(SchemaError::Blocked(
                        "local schema change awaits cloud delivery".into(),
                    ))
                }
            })
            .await
            .map_err(|error| replay_error(&error))?;
        Ok(())
    }

    async fn drop_schema(
        &mut self,
        identity: Receipt,
        change: SchemaChange,
    ) -> Result<(), RestoreError> {
        self.store
            .schema_manager()
            .drop_replay_schema(&change.name, &identity.id, &identity.digest)
            .await
            .map_err(|error| replay_error(&error))
    }
}

fn stamp(entry: &SnapshotEntry) -> ReplayStamp {
    ReplayStamp {
        ordering_ms: entry.authored_at_ms.min(entry.accepted_at_ms),
        writer_key: entry.writer_key.clone(),
        writer_device: entry.writer_device.clone(),
        writer_seq: entry.writer_seq,
    }
}

struct Receipt {
    id: [u8; 32],
    digest: [u8; 32],
}

impl Receipt {
    fn new(
        database: &str,
        entry: &SnapshotEntry,
        prepared: &Prepared,
    ) -> Result<Self, RestoreError> {
        let identity = canonical_bytes(&(
            database,
            &entry.writer_key,
            &entry.writer_device,
            entry.writer_seq,
        ))
        .map_err(|_| RestoreError::Entry(DETAIL))?;
        let proof = canonical_bytes(&(&prepared.hash, stamp(entry)))
            .map_err(|_| RestoreError::Entry(DETAIL))?;
        Ok(Self {
            id: Sha256::digest(identity).into(),
            digest: Sha256::digest(proof).into(),
        })
    }
}

fn replay_error(error: &SchemaError) -> RestoreError {
    tracing::warn!(target: "lastdbd::log_sync", error = error_class(error), "could not replay an owned change");
    RestoreError::Apply(DETAIL)
}

fn error_class(error: &SchemaError) -> &'static str {
    match error {
        SchemaError::CasConflict { .. } => "revision",
        SchemaError::Blocked(_) => "change_blocked",
        SchemaError::StorageRead { .. } => "storage_read",
        SchemaError::StorageFull { .. } => "storage_full",
        _ => "invalid_or_unavailable",
    }
}

fn schema_error(_: RestoreError) -> SchemaError {
    SchemaError::InvalidData("owned schema replay failed".into())
}
