//! Apply checked portable winners through the native record writer.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use cloud_backup_protocol::shared::{LogHead, WriterStream};
use cloud_backup_protocol::Id;
use lastdb_model::portable_mutation::{
    MutationEntry, MutationOperation, Position, SchemaIdentity, Stamp, StreamId, StreamProgress,
};
use lastdb_model::record_value::{FileMarker, InlineWrite, StoredRecord, RECORD_VALUE_VERSION};
use schema_types::{PortableSchemaDefinition, PortableSchemaId};
use serde_json::{Map, Value};

use super::portable_checkpoint::PortableReplaySchema;
use super::portable_journal::schema_fingerprint;
use super::MutationManager;
use crate::db_operations::record_store::RecordChange;
use crate::schema::{Schema, SchemaError};
use crate::storage::disk_pack::transaction;
use crate::storage::format::metadata::JournalSchema;
use crate::storage::format::records::{PortableEffect, RecordAddress};

mod peer;
pub use peer::PortablePeerWindow;

const MAX_CANDIDATES: usize = 256;

pub struct ReplayCandidate<'a> {
    pub entry: &'a MutationEntry,
    pub definition: &'a PortableSchemaDefinition,
    pub local_schema: &'a str,
}

struct Prepared {
    schemas: BTreeMap<String, Schema>,
    addresses: Vec<RecordAddress>,
    before: Vec<Option<Arc<StoredRecord>>>,
    deletes: Vec<Option<Stamp>>,
}

impl Prepared {
    fn winners<'a>(
        &'a self,
        candidates: &'a [ReplayCandidate<'_>],
    ) -> impl Iterator<Item = usize> + 'a {
        candidates
            .iter()
            .zip(self.before.iter().zip(&self.deletes))
            .enumerate()
            .filter_map(|(index, (candidate, (live, delete)))| {
                let previous = live
                    .as_ref()
                    .map(|record| &record.stamp)
                    .into_iter()
                    .chain(delete.as_ref())
                    .max();
                previous
                    .is_none_or(|previous| candidate.entry.stamp > *previous)
                    .then_some(index)
            })
    }
}

impl MutationManager {
    /// Verify the staged disk checkpoint against its authenticated manifest.
    pub async fn validate_portable_restore_checkpoint(
        &self,
        progress: &BTreeMap<StreamId, StreamProgress>,
        cutoff: Option<u64>,
    ) -> Result<(), SchemaError> {
        Ok(self
            .db_ops
            .metadata()
            .validate_portable_restore_checkpoint(progress, cutoff)
            .await?)
    }

    /// Resolve winners before acquiring their external file dependencies.
    /// The native commit repeats the comparison against its own write view.
    pub async fn portable_replay_needed(
        &self,
        candidates: &[ReplayCandidate<'_>],
    ) -> Result<Vec<usize>, SchemaError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(async {
                let prepared = self.prepare_portable_replay(candidates).await?;
                Ok(prepared.winners(candidates).collect())
            })
            .await
    }

    /// Apply checked winners inside a staged home. The restore coordinator
    /// persists the fully accounted replay cut before it publishes the home.
    pub async fn apply_portable_candidates(
        &self,
        candidates: &[ReplayCandidate<'_>],
        files: BTreeMap<Position, Vec<u8>>,
    ) -> Result<(), SchemaError> {
        transaction::request_scope(
            self.db_ops.native_runtime(),
            self.stage_portable_candidates(candidates, files),
        )
        .await
    }

    async fn stage_portable_candidates(
        &self,
        candidates: &[ReplayCandidate<'_>],
        mut files: BTreeMap<Position, Vec<u8>>,
    ) -> Result<(), SchemaError> {
        let prepared = self.prepare_portable_replay(candidates).await?;
        let mut groups = BTreeMap::<&str, Vec<RecordChange>>::new();
        let records = self.db_ops.records();
        for index in prepared.winners(candidates) {
            let input = &candidates[index];
            let before = prepared.before[index].clone();
            let after = replay_record(input.entry)?;
            let file_write = match (&after.file, &after.file_version) {
                (Some(marker), Some(version))
                    if before
                        .as_ref()
                        .and_then(|record| record.file_version.as_ref())
                        != Some(version) =>
                {
                    let bytes = files
                        .remove(version)
                        .ok_or_else(|| invalid("replay file version is missing"))?;
                    Some(records.admit_portable_file(marker.clone(), bytes)?)
                }
                _ => None,
            };
            let inline_write = replay_inline_write(input.entry, before.as_deref());
            groups
                .entry(input.local_schema)
                .or_default()
                .push(RecordChange {
                    key: prepared.addresses[index].clone(),
                    before,
                    after,
                    file_write,
                    inline_write,
                    portable: PortableEffect::SharedReplay,
                });
        }
        for (schema, changes) in groups {
            self.stage_replayed_record_group(&prepared.schemas[schema], changes)
                .await?;
        }
        Ok(())
    }

    /// Seal the private staged restore before its owning coordinator activates it.
    pub async fn finish_portable_restore(
        &self,
        progress: &BTreeMap<StreamId, StreamProgress>,
        cutoff: Option<u64>,
        fresh_stream: &StreamId,
        database: &Id,
        device: &Id,
    ) -> Result<(), SchemaError> {
        Ok(transaction::request_scope(
            self.db_ops.native_runtime(),
            self.db_ops.metadata().stage_portable_restore(
                progress,
                cutoff,
                fresh_stream,
                database,
                device,
            ),
        )
        .await?)
    }

    /// In the private stage, discard file references derived from the old shared roots.
    pub async fn reset_portable_rejoin_file_references(&self) -> Result<(), SchemaError> {
        let runtime = self.db_ops.native_runtime();
        runtime.require_staged_shared_rejoin()?;
        let metadata = self.db_ops.metadata();
        let mut after = None;
        loop {
            after = transaction::request_scope(
                Arc::clone(&runtime),
                metadata.stage_reset_rejoin_file_references(after.as_deref()),
            )
            .await?;
            if after.is_none() {
                return Ok(());
            }
        }
    }

    /// Seal complete replay under the original writer at its exact authenticated cloud head.
    pub async fn finish_portable_rejoin(
        &self,
        progress: &BTreeMap<StreamId, StreamProgress>,
        cutoff: Option<u64>,
        writer: &WriterStream,
        device: &Id,
        expected: Option<&LogHead>,
    ) -> Result<(), SchemaError> {
        let runtime = self.db_ops.native_runtime();
        runtime.require_staged_shared_rejoin()?;
        let metadata = self.db_ops.metadata();
        let mut after = None;
        loop {
            after = transaction::request_scope(
                Arc::clone(&runtime),
                metadata.stage_reset_rejoin_inbox(after.as_deref()),
            )
            .await?;
            if after.is_none() {
                break;
            }
        }
        transaction::request_scope(Arc::clone(&runtime), async {
            let bound = metadata.portable_bound_schemas().await?;
            runtime.require_shared_schema_subset(bound.values().map(String::as_str))?;
            let schemas =
                futures::future::try_join_all(bound.into_iter().map(|(id, alias)| async move {
                    let schema = self.load_schema_for_delta(&alias).await?;
                    let local_sha256 = schema_fingerprint(&schema)?;
                    Ok::<_, SchemaError>((alias, JournalSchema { id, local_sha256 }))
                }))
                .await?
                .into_iter()
                .collect();
            Ok(metadata
                .stage_portable_rejoin(progress, cutoff, writer, device, expected, &schemas)
                .await?)
        })
        .await
    }

    async fn prepare_portable_replay(
        &self,
        candidates: &[ReplayCandidate<'_>],
    ) -> Result<Prepared, SchemaError> {
        if candidates.len() > MAX_CANDIDATES
            || candidates
                .windows(2)
                .any(|pair| pair[0].entry.stamp <= pair[1].entry.stamp)
        {
            return Err(invalid(
                "replay candidates must be bounded and strictly newest first",
            ));
        }
        let names: BTreeSet<_> = candidates.iter().map(|input| input.local_schema).collect();
        let schemas: BTreeMap<_, _> =
            futures::future::try_join_all(names.into_iter().map(|name| async {
                Ok::<_, SchemaError>((name.to_owned(), self.load_schema_for_delta(name).await?))
            }))
            .await?
            .into_iter()
            .collect();
        let records = self.db_ops.records();
        let mut addresses = Vec::with_capacity(candidates.len());
        let mut seen = HashSet::with_capacity(candidates.len());
        for input in candidates {
            let schema = &schemas[input.local_schema];
            PortableReplaySchema::bind_version(input.definition, input.definition.id(), schema)?
                .validate_entry(input.entry)?;
            let key = input
                .entry
                .operation
                .primary_key()
                .ok_or_else(|| invalid("a schema definition is not a replay candidate"))?;
            let address = records.record_address(schema, &key.clone().into())?;
            if !seen.insert(address.as_str().to_owned()) {
                return Err(invalid("replay candidates repeat a primary address"));
            }
            addresses.push(address);
        }
        let refs: Vec<_> = addresses.iter().collect();
        let (before, deletes) = futures::try_join!(
            records.read_records(&addresses),
            records.read_applied_deletes(&refs),
        )?;
        Ok(Prepared {
            schemas,
            addresses,
            before,
            deletes,
        })
    }
}

fn replay_inline_write(
    entry: &MutationEntry,
    before: Option<&StoredRecord>,
) -> Option<InlineWrite> {
    let MutationOperation::ReplaceRecord { data, .. } = &entry.operation else {
        return None;
    };
    let before = before.and_then(|record| record.inline_file.as_ref());
    match data.inline_file.as_deref() {
        Some(file) if before == Some(&file.marker) => None,
        Some(file) => Some(InlineWrite::Put(file.clone())),
        None if before.is_some() => Some(InlineWrite::Clear),
        None => None,
    }
}

fn replay_record(entry: &MutationEntry) -> Result<StoredRecord, SchemaError> {
    let MutationOperation::ReplaceRecord { primary_key, data } = &entry.operation else {
        return Ok(StoredRecord::erased(entry.stamp.clone()));
    };
    let meta = data
        .metadata
        .as_ref()
        .map(|metadata| {
            let object: Map<String, Value> = metadata.clone().into_iter().collect();
            serde_json::from_value(Value::Object(object))
                .map_err(|_| invalid("portable record metadata differs from the record API"))
        })
        .transpose()?;
    let definition_id = match &entry.schema {
        SchemaIdentity::Shared(id) => Some(
            id.parse::<PortableSchemaId>()
                .map_err(|_| invalid("a shared record names an invalid schema definition"))?,
        ),
        SchemaIdentity::Local(_) => None,
    };
    Ok(StoredRecord {
        v: RECORD_VALUE_VERSION,
        key: primary_key.clone().into(),
        alternates: data
            .alternates
            .iter()
            .map(|(name, key)| (name.clone(), key.clone().map(Into::into)))
            .collect(),
        fields: data.fields.clone(),
        stamp: entry.stamp.clone(),
        meta,
        file: data.file.as_ref().map(|file| FileMarker {
            name: file.name.clone(),
            media_type: file.media_type.clone(),
            size_bytes: file.size_bytes,
            sha256: file.sha256.clone(),
        }),
        file_version: data.file.as_ref().map(|file| file.version.clone()),
        inline_file: data.inline_file.as_ref().map(|file| file.marker.clone()),
        definition_id,
        deleted: false,
    })
}

fn invalid(detail: &str) -> SchemaError {
    SchemaError::InvalidData(detail.into())
}
