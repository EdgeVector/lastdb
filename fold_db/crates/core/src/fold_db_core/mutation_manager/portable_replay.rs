//! Apply checked portable winners through the native record writer.

use super::{portable_schema::PortableReplaySchema, MutationManager};
use crate::db_operations::record_store::RecordChange;
use crate::schema::{Schema, SchemaError};
use crate::storage::disk_pack::transaction;
use crate::storage::format::records::RecordAddress;
use lastdb_model::portable_mutation::{MutationEntry, Position, Stamp, StreamId, StreamProgress};
use lastdb_model::record_value::StoredRecord;
use schema_types::PortableSchemaDefinition;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

mod values;

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
        self.db_ops
            .metadata()
            .validate_portable_restore_checkpoint(progress, cutoff)
            .await
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
        mut files: BTreeMap<Position, Vec<u8>>,
    ) -> Result<(), SchemaError> {
        transaction::request_scope(self.db_ops.native_runtime(), async {
            let prepared = self.prepare_portable_replay(candidates).await?;
            let mut groups = BTreeMap::<&str, Vec<RecordChange>>::new();
            let records = self.db_ops.records();
            for index in prepared.winners(candidates) {
                let input = &candidates[index];
                let before = prepared.before[index].clone();
                let after = values::record(input.entry)?;
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
                groups
                    .entry(input.local_schema)
                    .or_default()
                    .push(RecordChange {
                        key: prepared.addresses[index].clone(),
                        before,
                        after,
                        file_write,
                    });
            }
            for (schema, changes) in groups {
                records
                    .stage_records(&prepared.schemas[schema], changes, Vec::new())
                    .await?;
            }
            Ok(())
        })
        .await
    }

    /// Seal the private staged restore before its owning coordinator activates it.
    pub async fn finish_portable_restore(
        &self,
        progress: &BTreeMap<StreamId, StreamProgress>,
        cutoff: Option<u64>,
        fresh_stream: &StreamId,
    ) -> Result<(), SchemaError> {
        transaction::request_scope(
            self.db_ops.native_runtime(),
            self.db_ops
                .metadata()
                .stage_portable_restore(progress, cutoff, fresh_stream),
        )
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
            PortableReplaySchema::bind(input.definition, input.definition.id(), schema)?
                .validate_entry(input.entry)?;
            let address = records
                .record_address(schema, &input.entry.operation.primary_key().clone().into())?;
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

fn invalid(detail: &str) -> SchemaError {
    SchemaError::InvalidData(detail.into())
}
