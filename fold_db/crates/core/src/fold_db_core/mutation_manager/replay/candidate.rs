//! Fold an accepted entry into the confirmed winner, then settle the visible record.

use super::attachments::{AttachmentContext, Pins};
use super::records::as_mutation;
use super::state::{decode, invalid, validate_record, Baseline};
use super::MutationManager;
use crate::schema::types::{cas::CasExpectation, KeyValue, Mutation, MutationType, Schema};
use crate::schema::SchemaError;
use crate::storage::format::metadata::{
    file_ref_key, index_key, inline_key, record_key, ReplayStamp,
};
use lastdb_model::portable_mutation::{Position, Stamp, StreamId};
use lastdb_model::record_value::StoredRecord;

impl MutationManager {
    /// Save the confirmed winner, or install it directly when the local queue settles.
    pub async fn apply_replay_candidate(
        &self,
        mut mutation: Mutation,
        stamp: &ReplayStamp,
        settle: bool,
    ) -> Result<bool, SchemaError> {
        mutation.writer = Some(stamp.writer_key.clone());
        self.db_ops.metadata().require_replay_scope()?;
        let schema = self.load_schema_for_delta(&mutation.schema_name).await?;
        let address = self.replay_record_address(&schema.name, &mutation.key_value)?;
        let capture = self.replay_capture(&address).await?;
        let baseline = capture.baseline.as_deref().map(decode).transpose()?;
        let live = self.replay_preimage(address.clone()).await?;
        let live = live.as_deref().filter(|r| r.is_live());
        let deleted = matches!(
            mutation.mutation_type,
            MutationType::Delete | MutationType::Purge
        );
        let confirmed = baseline.as_ref().filter(|b| b.stamp == capture.winner);
        let before = match confirmed {
            Some(saved) => saved.record.as_ref(),
            None if deleted => None,
            // Older text-only pending writes did not alter either attachment slot.
            None if !capture.pending_attachment => live,
            None if mutation.file_write.is_some() && mutation.inline_write.is_some() => None,
            None => return Ok(false),
        };
        let authorship = authorship_preimage(confirmed.is_none(), before, live, deleted);
        prepare_candidate(self, &schema, &mut mutation, deleted)?;
        let context = AttachmentContext {
            address: &address,
            before,
            live,
            capture: &capture,
        };
        let (record, pins) = if deleted {
            (
                None,
                Pins {
                    inline: None,
                    file_ref: None,
                },
            )
        } else {
            let stamp = Stamp {
                authored_at_ms: stamp.ordering_ms,
                position: Position {
                    source_stream: StreamId::try_from(uuid::Uuid::new_v4().to_string())
                        .map_err(invalid)?,
                    sequence: 1,
                },
            };
            let mut record =
                super::super::replace::replace_record(&schema, &mutation, stamp, before)?;
            super::super::replace::stamp_authors(&mut record, &mutation, authorship);
            if schema.file_required && record.file.is_none() {
                return Err(invalid("this record requires a file"));
            }
            let pins = self
                .candidate_attachments(&context, &mutation, &mut record, !settle)
                .await?;
            validate_record(&record)?;
            (Some(record), pins)
        };
        if settle {
            self.settle_candidate(&context, &mutation, record.as_ref(), &pins)
                .await?;
            return Ok(true);
        }
        let next = Baseline {
            stamp: Some(stamp.clone()),
            record,
        };
        self.retire_replay_file(
            &address,
            baseline.as_ref().and_then(|b| b.record.as_ref()),
            live,
            next.record.as_ref(),
        )?;
        self.db_ops
            .metadata()
            .stage_replay_rows(vec![
                (
                    record_key(&address, "baseline"),
                    Some(serde_json::to_vec(&next).map_err(invalid)?),
                ),
                (inline_key(&address), pins.inline),
                (file_ref_key(&address), pins.file_ref),
                (
                    index_key(&schema.name, &address),
                    Some(address.as_bytes().to_vec()),
                ),
                (record_key(&address, "deferred"), None),
            ])
            .await?;
        Ok(true)
    }

    /// Install the complete winner when the latest pending local write is observed.
    pub async fn apply_replay_baseline(
        &self,
        schema: &str,
        key: &KeyValue,
        winner: &ReplayStamp,
    ) -> Result<bool, SchemaError> {
        let address = self.replay_record_address(schema, key)?;
        let capture = self.replay_capture(&address).await?;
        let Some(bytes) = capture.baseline.as_deref() else {
            return Ok(false);
        };
        let baseline = decode(bytes)?;
        if baseline.stamp.as_ref() != Some(winner) {
            return Ok(false);
        }
        let live = self.replay_preimage(address.clone()).await?;
        let live = live.as_deref().filter(|r| r.is_live());
        if let Some(record) = &baseline.record {
            if !self.replay_record_key_matches(schema, key, &record.key)? {
                return Err(invalid("stored record key differs"));
            }
        }
        let unchanged = same_content(live, baseline.record.as_ref());
        let mut mutation = as_mutation(schema, key.clone(), baseline.record.clone());
        if let Some(record) = &baseline.record {
            let context = AttachmentContext {
                address: &address,
                before: Some(record),
                live,
                capture: &capture,
            };
            self.materialize_attachments(&context, record, &mut mutation, unchanged)
                .await?;
        }
        if !unchanged {
            self.apply_replay_record(mutation, &winner.writer_key)
                .await?;
        }
        Ok(true)
    }
}

/// A complete file payload has no content base. Authorship still follows the live record.
fn authorship_preimage<'a>(
    confirmed_absent: bool,
    before: Option<&'a StoredRecord>,
    live: Option<&'a StoredRecord>,
    deleted: bool,
) -> Option<&'a StoredRecord> {
    if confirmed_absent && before.is_none() && !deleted {
        live
    } else {
        before
    }
}

fn prepare_candidate(
    manager: &MutationManager,
    schema: &Schema,
    mutation: &mut Mutation,
    deleted: bool,
) -> Result<(), SchemaError> {
    mutation.expected = if deleted {
        CasExpectation::Revision {
            revision: "accepted-replay".into(),
        }
    } else {
        CasExpectation::Absent
    };
    if !deleted {
        mutation.mutation_type = MutationType::Create;
    }
    mutation.validate_record_intent()?;
    mutation.key_value = MutationManager::canonical_primary_key(schema, &mutation.key_value)?;
    if !deleted {
        manager.prepare_mutation_batch(std::slice::from_ref(mutation))?;
        let keys = schema.require_record_keys()?;
        for (name, key) in &mut mutation.alternates {
            if let Some(range) = key.as_mut().and_then(|key| key.range.as_mut()) {
                let profile = keys.alternates[name].range_profile().map_err(invalid)?;
                *range = crate::schema::types::key_comparison::canonical_range(profile, range)?;
            }
        }
    }
    Ok(())
}

pub(super) fn same_content(left: Option<&StoredRecord>, right: Option<&StoredRecord>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(a), Some(b)) => {
            a.key == b.key
                && a.alternates == b.alternates
                && a.fields == b.fields
                && a.meta == b.meta
                && a.created_by == b.created_by
                && a.updated_by == b.updated_by
                && a.file == b.file
                && a.inline_file == b.inline_file
        }
        _ => false,
    }
}
