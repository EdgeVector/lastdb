//! Guards for an erase that the TTL sweep sends: spare a key whose tip is newer
//! than the sweep cutoff, and find the keys of a batch that are already gone.

use super::*;

type ResidentSlot = (String, String, String);

/// What one try to queue a purge envelope did.
pub(super) enum PurgeEnqueue {
    /// Queued. The caller waits for this envelope's durable completion.
    Queued(tokio::sync::oneshot::Receiver<super::super::molecules::PurgeCompletion>),
    /// Nothing queued: these keys of the batch have a tip at or after the
    /// cutoff of their erase. The erasures come back unchanged.
    Spared(HashSet<usize>, Vec<Mutation>),
}

impl MutationManager {
    /// The keys of `keys` that no field of `schema_name` has a record for.
    ///
    /// A purge refuses a batch that names a key already gone. The sweep calls
    /// this after such a refusal, drops the absent keys, and sends the rest.
    pub async fn absent_erase_targets(
        &self,
        schema_name: &str,
        keys: &[crate::schema::types::KeyValue],
    ) -> Result<Vec<crate::schema::types::KeyValue>, SchemaError> {
        super::super::super::purge::absent_purge_targets(
            super::super::super::purge::PurgeScope {
                db_ops: &self.db_ops,
                schema_manager: &self.schema_manager,
                schema_name,
            },
            keys,
        )
        .await
    }

    /// Queue the purge envelope of one schema. A key that the write-time guard
    /// spares leaves the batch, and the batch is planned again without it.
    /// `None` means every target was spared; every queued job has a receiver.
    pub(super) async fn enqueue_schema_purge_checked(
        &self,
        schema_name: String,
        mut erasures: Vec<Mutation>,
        storage_prefix: Option<&str>,
        (missing, verb): (
            super::super::super::purge::PurgeMissingPolicy,
            super::super::super::purge::HardEraseVerb,
        ),
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<
        Option<tokio::sync::oneshot::Receiver<super::super::molecules::PurgeCompletion>>,
        SchemaError,
    > {
        loop {
            let queued = self
                .enqueue_schema_purge_envelope(
                    schema_name.clone(),
                    erasures,
                    storage_prefix,
                    (missing, verb),
                    author_clock_barrier.clone(),
                )
                .await?;
            let (spared, all) = match queued {
                PurgeEnqueue::Queued(receiver) => return Ok(Some(receiver)),
                PurgeEnqueue::Spared(spared, all) => (spared, all),
            };
            // A key with a tip at or after the cutoff was rewritten after the
            // sweep chose it. Plan again without it. This repeats only when a
            // writer raced the plan, so the cost is rare.
            tracing::info!(
                schema = %schema_name,
                spared = spared.len(),
                "purge spared keys rewritten after the sweep cutoff"
            );
            erasures = all
                .into_iter()
                .enumerate()
                .filter(|(index, _)| !spared.contains(index))
                .map(|(_, mutation)| mutation)
                .collect();
            if erasures.is_empty() {
                return Ok(None);
            }
        }
    }

    /// The indexes that [`Self::erasures_rewritten_since_cutoff`] finds, for a
    /// purge. `None` when the verb is not a purge or no key is spared.
    pub(super) async fn spared_erasures(
        &self,
        schema: &crate::schema::Schema,
        erasures: &[Mutation],
        (storage_prefix, verb): (Option<&str>, super::super::super::purge::HardEraseVerb),
    ) -> Result<Option<HashSet<usize>>, SchemaError> {
        if verb != super::super::super::purge::HardEraseVerb::Purge {
            return Ok(None);
        }
        let spared = self
            .erasures_rewritten_since_cutoff(schema, erasures, storage_prefix)
            .await?;
        Ok((!spared.is_empty()).then_some(spared))
    }

    /// Indexes into `erasures` of the keys that have a tip written at or after
    /// the cutoff of their own mutation (`erase_tips_before_nanos`).
    ///
    /// The caller holds every target molecule's write lock while it compares
    /// current tip clocks with the mutation cutoffs. A tip at or after its
    /// cutoff spares that mutation before the complete native deletion enters
    /// the lane. No resident tombstone overlay participates in this check.
    pub(super) async fn erasures_rewritten_since_cutoff(
        &self,
        schema: &crate::schema::Schema,
        erasures: &[Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<HashSet<usize>, SchemaError> {
        let mut slots: Vec<(usize, u64, String, ResidentSlot)> = Vec::new();
        for (index, mutation) in erasures.iter().enumerate() {
            let Some(cutoff) = mutation.erase_tips_before_nanos else {
                continue;
            };
            for field in schema.runtime_fields.values() {
                let Some(uuid) = field.common().molecule_uuid() else {
                    continue;
                };
                let Some((hash, range)) = field.slot_coordinates(&mutation.key_value) else {
                    continue;
                };
                for candidate in super::super::super::purge::delete_candidates_for_key(
                    &self.db_ops,
                    field,
                    &mutation.key_value,
                )? {
                    let slot = (uuid.clone(), hash.clone(), range.clone());
                    slots.push((index, cutoff, candidate.mk_key, slot));
                }
            }
        }
        let mut spared = HashSet::new();
        if slots.is_empty() {
            return Ok(spared);
        }
        let mk_keys: Vec<String> = slots.iter().map(|slot| slot.2.clone()).collect();
        let disk_tips = self
            .db_ops
            .atoms()
            .delete_target_tips_with_generations(&mk_keys, storage_prefix)
            .await?;
        for ((index, cutoff, ..), tip) in slots.iter().zip(disk_tips) {
            if tip.is_some_and(|tip| tip.entry.written_at >= *cutoff) {
                spared.insert(*index);
            }
        }
        Ok(spared)
    }
}
